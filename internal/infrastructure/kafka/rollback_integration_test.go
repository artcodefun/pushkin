//go:build integration

package kafka_test

import (
	"context"
	"errors"
	"testing"
	"time"
	"uuid"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	usercontract "github.com/superman/pushkin/api/kafka/v1"
	"github.com/superman/pushkin/internal/application/ports"
	contracts "github.com/superman/pushkin/internal/contracts/kafka"
	adapter "github.com/superman/pushkin/internal/infrastructure/kafka"
)

func TestConsumersRollbackPolledBatch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	brokers := startKafka(t, ctx)
	for _, transactional := range []bool{false, true} {
		name := "plain"
		if transactional {
			name = "transactional"
		}
		t.Run(name, func(t *testing.T) {
			topic := contracts.Topic("pushkin.test.rollback." + uuid.NewV7().String())
			createTopics(t, ctx, brokers, topic)
			producer := newProducer(t, brokers)
			defer producer.Close()
			first, second := newDeliveryWork(), newDeliveryWork()
			if err := producer.Produce(ctx, []ports.OutboundKafkaMessage{{Topic: topic, Key: []byte("same"), Value: first}, {Topic: topic, Key: []byte("same"), Value: second}}); err != nil {
				t.Fatal(err)
			}
			var poll func(context.Context, int) ([]ports.KafkaRecord, error)
			var rollback func(context.Context) error
			var complete func(context.Context, []ports.KafkaRecord) error
			group := "rollback-" + uuid.NewV7().String()
			if transactional {
				c := newDeliveryTransactionalConsumer(t, brokers, group, topic)
				defer c.Close()
				poll, rollback = c.PollMany, c.Rollback
				complete = func(ctx context.Context, _ []ports.KafkaRecord) error { return c.Complete(ctx, nil) }
			} else {
				c := newDeliveryConsumer(t, brokers, group, topic)
				defer c.Close()
				poll, rollback, complete = c.PollMany, c.Rollback, c.Commit
			}
			records, err := poll(ctx, 1)
			if err != nil || len(records) != 1 {
				t.Fatalf("first poll: %v %v", records, err)
			}
			if err := rollback(ctx); err != nil {
				t.Fatal(err)
			}
			if err := rollback(ctx); err != nil {
				t.Fatal(err)
			}
			replay, err := poll(ctx, 1)
			if err != nil || len(replay) != 1 || replay[0].Offset != records[0].Offset {
				t.Fatalf("record skipped after rollback: %v %v", replay, err)
			}
			if transactional {
				// An invalid output fails after Begin and exercises the native abort path.
				c := newDeliveryTransactionalConsumer(t, brokers, "abort-"+uuid.NewV7().String(), topic)
				first, err := c.PollMany(ctx, 1)
				if err != nil || len(first) != 1 {
					c.Close()
					t.Fatalf("abort poll: %v", err)
				}
				if err := c.Complete(ctx, []ports.OutboundKafkaMessage{{}}); err == nil {
					c.Close()
					t.Fatal("invalid output accepted")
				}
				if err := c.Rollback(ctx); err != nil {
					c.Close()
					t.Fatal(err)
				}
				again, err := c.PollMany(ctx, 1)
				c.Close()
				if err != nil || len(again) != 1 || again[0].Offset != first[0].Offset {
					t.Fatalf("abort skipped record: %v %v", again, err)
				}
			}
			if err := complete(ctx, replay); err != nil {
				t.Fatal(err)
			}
			next, err := poll(ctx, 1)
			if err != nil || len(next) != 1 || next[0].Value.(contracts.DeliveryWorkV1).DeliveryID != second.DeliveryID {
				t.Fatalf("next poll: %v %v", next, err)
			}
			if err := complete(ctx, next); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestTransactionalConsumerRecreatesAfterFencing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	brokers := startKafka(t, ctx)
	inputTopic := contracts.Topic("pushkin.test.transaction-recreate." + uuid.NewV7().String())
	outputTopic := contracts.Topic("pushkin.test.transaction-recreate.output." + uuid.NewV7().String())
	createTopics(t, ctx, brokers, inputTopic, outputTopic)

	producer := newProducer(t, brokers)
	defer producer.Close()
	firstInput, replayedInput := newDeliveryWork(), newDeliveryWork()
	if err := producer.Produce(ctx, []ports.OutboundKafkaMessage{
		{Topic: inputTopic, Key: []byte(firstInput.DeliveryID.String()), Value: firstInput},
		{Topic: inputTopic, Key: []byte(replayedInput.DeliveryID.String()), Value: replayedInput},
	}); err != nil {
		t.Fatal(err)
	}

	transactionalID := "pushkin-test-fence-" + uuid.NewV7().String()
	consumer, err := adapter.NewTransactionalConsumer(adapter.TransactionalConsumerParams{
		Brokers: brokers, ConsumerGroup: "transaction-recreate-" + uuid.NewV7().String(), TransactionalID: transactionalID, InputTopic: inputTopic,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()
	_, found, err := consumer.Poll(ctx)
	if err != nil || !found {
		t.Fatalf("first poll: found=%v err=%v", found, err)
	}
	if err := consumer.Complete(ctx, []ports.OutboundKafkaMessage{{Topic: outputTopic, Value: newDeliveryWork()}}); err != nil {
		t.Fatalf("initialize first transactional session: %v", err)
	}

	first, found, err := consumer.Poll(ctx)
	if err != nil || !found {
		t.Fatalf("second poll: found=%v err=%v", found, err)
	}

	fencingConsumer, err := adapter.NewTransactionalConsumer(adapter.TransactionalConsumerParams{
		Brokers: brokers, ConsumerGroup: "transaction-fencer-" + uuid.NewV7().String(), TransactionalID: transactionalID, InputTopic: inputTopic,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := fencingConsumer.Complete(ctx, []ports.OutboundKafkaMessage{{Topic: outputTopic, Value: newDeliveryWork()}}); err != nil {
		fencingConsumer.Close()
		t.Fatalf("start fencing transaction: %v", err)
	}
	fencingConsumer.Close()
	completeErr := consumer.Complete(ctx, []ports.OutboundKafkaMessage{{Topic: outputTopic, Value: newDeliveryWork()}})
	if completeErr == nil {
		t.Fatal("fenced transaction committed")
	}

	replay, found, err := consumer.Poll(ctx)
	if err != nil || !found || replay.Offset != first.Offset {
		t.Fatalf("recreated consumer did not replay input: record=%+v found=%v err=%v", replay, found, err)
	}
	if err := consumer.Complete(ctx, []ports.OutboundKafkaMessage{{Topic: outputTopic, Value: newDeliveryWork()}}); err != nil {
		t.Fatalf("complete replay with recreated consumer: %v", err)
	}
}

func TestExternalConsumerRollsBackUserEvent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	brokers := startKafka(t, ctx)
	topic := contracts.Topic("pushkin.test.user-events.rollback." + uuid.NewV7().String())
	createTopics(t, ctx, brokers, topic)

	producer, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	event := usercontract.UserEventV1{
		Type:     usercontract.UserEventTypeCreated,
		TenantID: uuid.NewV7().String(),
		UserID:   "user-1",
	}
	value, err := usercontract.EncodeMessage(event)
	if err != nil {
		t.Fatal(err)
	}
	if err := producer.ProduceSync(ctx, &kgo.Record{Topic: string(topic), Key: []byte("user-1"), Value: value}).FirstErr(); err != nil {
		t.Fatal(err)
	}

	consumer, err := adapter.NewExternalConsumer(adapter.ExternalConsumerParams{
		Brokers:       brokers,
		ConsumerGroup: "user-events-rollback-" + uuid.NewV7().String(),
		InputTopic:    usercontract.Topic(topic),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()
	_, found, err := consumer.Poll(ctx)
	if err != nil || !found {
		t.Fatalf("first poll: found=%v err=%v", found, err)
	}
	if err := consumer.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	replay, found, err := consumer.Poll(ctx)
	if err != nil || !found {
		t.Fatalf("user event skipped after rollback: found=%v err=%v", found, err)
	}
	replayed, ok := replay.Value.(usercontract.UserEventV1)
	if !ok || replayed.Type != event.Type || replayed.TenantID != event.TenantID || replayed.UserID != event.UserID {
		t.Fatalf("replayed user event = %+v, want %+v", replay.Value, event)
	}
}

func TestConsumersDoNotSkipMalformedRecords(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	brokers := startKafka(t, ctx)
	producer, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	for _, kind := range []string{"plain", "transactional", "external"} {
		t.Run(kind, func(t *testing.T) {
			topic := contracts.Topic("pushkin.test.decode." + uuid.NewV7().String())
			createTopics(t, ctx, brokers, topic)
			if err := producer.ProduceSync(ctx, &kgo.Record{Topic: string(topic), Value: []byte("invalid envelope")}, &kgo.Record{Topic: string(topic), Value: []byte("another invalid envelope")}).FirstErr(); err != nil {
				t.Fatal(err)
			}
			group := "decode-" + uuid.NewV7().String()
			var poll func(context.Context) error
			switch kind {
			case "plain":
				c := newDeliveryConsumer(t, brokers, group, topic)
				defer c.Close()
				poll = func(ctx context.Context) error { _, _, err := c.Poll(ctx); return err }
			case "transactional":
				c := newDeliveryTransactionalConsumer(t, brokers, group, topic)
				defer c.Close()
				poll = func(ctx context.Context) error { _, _, err := c.Poll(ctx); return err }
			case "external":
				c, err := adapter.NewExternalConsumer(adapter.ExternalConsumerParams{Brokers: brokers, ConsumerGroup: group, InputTopic: usercontract.Topic(topic)})
				if err != nil {
					t.Fatal(err)
				}
				defer c.Close()
				poll = func(ctx context.Context) error { _, _, err := c.Poll(ctx); return err }
			}
			first := poll(ctx)
			if first == nil {
				t.Fatal("malformed input accepted")
			}
			for range 2 {
				err := poll(ctx)
				// Error identity includes the raw offset. Repeated errors must refer to
				// the same malformed record, rather than progressing or timing out.
				if err == nil || errors.Is(err, context.DeadlineExceeded) || err.Error() != first.Error() {
					t.Fatalf("record skipped: first=%v next=%v", first, err)
				}
			}
		})
	}
}

func TestRollbackReturnsEveryPartitionToBatchStart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	brokers := startKafka(t, ctx)
	producer, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	for _, transactional := range []bool{false, true} {
		name := "plain"
		if transactional {
			name = "transactional"
		}
		t.Run(name, func(t *testing.T) {
			topic := contracts.Topic("pushkin.test.partitions." + uuid.NewV7().String())
			created, err := kadm.NewClient(producer).CreateTopics(ctx, 2, 1, nil, string(topic))
			if err != nil {
				t.Fatal(err)
			}
			for _, r := range created {
				if r.Err != nil {
					t.Fatal(r.Err)
				}
			}
			value, err := adapter.EncodeMessage(newDeliveryWork())
			if err != nil {
				t.Fatal(err)
			}
			if err := producer.ProduceSync(ctx, &kgo.Record{Topic: string(topic), Partition: 0, Value: value}, &kgo.Record{Topic: string(topic), Partition: 1, Value: value}).FirstErr(); err != nil {
				t.Fatal(err)
			}
			var poll func(context.Context, int) ([]ports.KafkaRecord, error)
			var rollback func(context.Context) error
			var commit func(context.Context, []ports.KafkaRecord) error
			group := "partitions-" + uuid.NewV7().String()
			if transactional {
				c := newDeliveryTransactionalConsumer(t, brokers, group, topic)
				defer c.Close()
				poll, rollback = c.PollMany, c.Rollback
				commit = func(ctx context.Context, _ []ports.KafkaRecord) error { return c.Complete(ctx, nil) }
			} else {
				c := newDeliveryConsumer(t, brokers, group, topic)
				defer c.Close()
				poll, rollback, commit = c.PollMany, c.Rollback, c.Commit
			}
			seen := map[int32]bool{}
			for len(seen) < 2 {
				batch, err := poll(ctx, 2)
				if err != nil || len(batch) == 0 {
					t.Fatalf("poll: %v", err)
				}
				if err := rollback(ctx); err != nil {
					t.Fatal(err)
				}
				expected := map[ports.KafkaOffset]bool{}
				for _, r := range batch {
					expected[r.Offset] = true
				}
				for len(expected) > 0 {
					replay, err := poll(ctx, 2)
					if err != nil || len(replay) == 0 {
						t.Fatalf("replay: %v", err)
					}
					for _, r := range replay {
						if r.Offset.Offset != 0 {
							t.Fatalf("skipped partition start: %v", r.Offset)
						}
						delete(expected, r.Offset)
						seen[r.Offset.Partition] = true
					}
					if err := commit(ctx, replay); err != nil {
						t.Fatal(err)
					}
				}
			}
		})
	}
}
