//go:build integration

package kafka_test

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"
	"uuid"

	"github.com/testcontainers/testcontainers-go"
	testkafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	usercontract "github.com/superman/pushkin/api/kafka/v1"
	"github.com/superman/pushkin/internal/application/ports"
	contracts "github.com/superman/pushkin/internal/contracts/kafka"
	adapter "github.com/superman/pushkin/internal/infrastructure/kafka"
)

func TestTransactionalConsumerPublishesAndCommitsInput(t *testing.T) {
	ctx := context.Background()
	brokers := startKafka(t, ctx)
	inputTopic := contracts.Topic("pushkin.test.input." + uuid.NewV7().String())
	outputTopic := contracts.Topic("pushkin.test.output." + uuid.NewV7().String())
	groupID := "pushkin-test-group-" + uuid.NewV7().String()
	createTopics(t, ctx, brokers, inputTopic, outputTopic)

	producer := newProducer(t, brokers)
	t.Cleanup(producer.Close)
	input := newDeliveryWork()
	if err := producer.Produce(ctx, []ports.OutboundKafkaMessage{{
		Topic: inputTopic,
		Key:   []byte(input.DeliveryID.String()),
		Value: input,
	}}); err != nil {
		t.Fatalf("produce input transaction: %v", err)
	}

	consumer := newDeliveryTransactionalConsumer(t, brokers, groupID, inputTopic)
	record, found, err := pollDelivery(t, consumer)
	if err != nil {
		t.Fatalf("poll input record: %v", err)
	}
	work, ok := record.Value.(contracts.DeliveryWorkV1)
	if !found || !ok || work.DeliveryID != input.DeliveryID {
		t.Fatalf("unexpected input record: found=%v record=%+v", found, record)
	}
	if err := consumer.Complete(ctx, []ports.OutboundKafkaMessage{{
		Topic: outputTopic,
		Key:   []byte(work.DeliveryID.String()),
		Value: work,
	}}); err != nil {
		t.Fatalf("process transaction: %v", err)
	}
	consumer.Close()

	output := readOutput(t, brokers, outputTopic)
	if output.DeliveryID != input.DeliveryID {
		t.Fatalf("unexpected output: %+v", output)
	}

	resumed := newDeliveryTransactionalConsumer(t, brokers, groupID, inputTopic)
	t.Cleanup(resumed.Close)
	deadline, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	_, found, err = resumed.Poll(deadline)
	if found || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("committed input record must not be replayed: found=%v err=%v", found, err)
	}
}

func TestTransactionalConsumerWaitForAssignmentRewindsAndCommitsInitialRecord(t *testing.T) {
	ctx := context.Background()
	brokers := startKafka(t, ctx)
	inputTopic := contracts.Topic("pushkin.test.assignment." + uuid.NewV7().String())
	outputTopic := contracts.Topic("pushkin.test.assignment.output." + uuid.NewV7().String())
	groupID := "pushkin-test-assignment-" + uuid.NewV7().String()
	createTopics(t, ctx, brokers, inputTopic, outputTopic)

	producer := newProducer(t, brokers)
	t.Cleanup(producer.Close)
	input := newDeliveryWork()
	if err := producer.Produce(ctx, []ports.OutboundKafkaMessage{{
		Topic: inputTopic,
		Key:   []byte(input.DeliveryID.String()),
		Value: input,
	}}); err != nil {
		t.Fatalf("produce assignment input: %v", err)
	}

	pipeline := newDeliveryTransactionalConsumer(t, brokers, groupID, inputTopic)
	waitCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	assignment, err := pipeline.WaitForAssignment(waitCtx)
	if err != nil {
		t.Fatalf("wait for assignment: %v", err)
	}
	if len(assignment) != 1 || assignment[0].Topic != inputTopic || assignment[0].Partition != 0 {
		t.Fatalf("unexpected assignment: %+v", assignment)
	}

	record, found, err := pipeline.Poll(waitCtx)
	if err != nil || !found {
		t.Fatalf("poll after assignment: found=%v err=%v", found, err)
	}
	work, ok := record.Value.(contracts.DeliveryWorkV1)
	if !ok || work.DeliveryID != input.DeliveryID {
		t.Fatalf("unexpected record after assignment: %+v", record)
	}
	if err := pipeline.Complete(ctx, []ports.OutboundKafkaMessage{{
		Topic: outputTopic,
		Key:   []byte(work.DeliveryID.String()),
		Value: work,
	}}); err != nil {
		t.Fatalf("complete rewound record transaction: %v", err)
	}
	pipeline.Close()

	resumed := newDeliveryTransactionalConsumer(t, brokers, groupID, inputTopic)
	t.Cleanup(resumed.Close)
	deadline, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	_, found, err = resumed.Poll(deadline)
	if found || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("committed rewound input must not be replayed: found=%v err=%v", found, err)
	}
}

func TestTransactionalConsumerWaitForAssignmentBeforeInputProcessesLaterRecord(t *testing.T) {
	ctx := context.Background()
	brokers := startKafka(t, ctx)
	inputTopic := contracts.Topic("pushkin.test.assignment.empty." + uuid.NewV7().String())
	outputTopic := contracts.Topic("pushkin.test.assignment.empty.output." + uuid.NewV7().String())
	groupID := "pushkin-test-assignment-empty-" + uuid.NewV7().String()
	createTopics(t, ctx, brokers, inputTopic, outputTopic)

	pipeline := newDeliveryTransactionalConsumer(t, brokers, groupID, inputTopic)
	t.Cleanup(pipeline.Close)
	assignmentCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	assignment, err := pipeline.WaitForAssignment(assignmentCtx)
	if err != nil {
		t.Fatalf("wait for empty-topic assignment: %v", err)
	}
	if len(assignment) != 1 || assignment[0].Topic != inputTopic || assignment[0].Partition != 0 {
		t.Fatalf("unexpected assignment: %+v", assignment)
	}

	producer := newProducer(t, brokers)
	t.Cleanup(producer.Close)
	input := newDeliveryWork()
	if err := producer.Produce(ctx, []ports.OutboundKafkaMessage{{
		Topic: inputTopic,
		Key:   []byte(input.DeliveryID.String()),
		Value: input,
	}}); err != nil {
		t.Fatalf("produce later input: %v", err)
	}

	record, found, err := pipeline.Poll(assignmentCtx)
	if err != nil || !found {
		t.Fatalf("poll later input: found=%v err=%v", found, err)
	}
	work, ok := record.Value.(contracts.DeliveryWorkV1)
	if !ok || work.DeliveryID != input.DeliveryID {
		t.Fatalf("unexpected later record: %+v", record)
	}
	if err := pipeline.Complete(ctx, []ports.OutboundKafkaMessage{{
		Topic: outputTopic,
		Key:   []byte(input.DeliveryID.String()),
		Value: input,
	}}); err != nil {
		t.Fatalf("complete later input transaction: %v", err)
	}
}

func TestCompactedTopicLoaderLoadsSelectedPartition(t *testing.T) {
	ctx := context.Background()
	brokers := startKafka(t, ctx)
	statsTopic := contracts.Topic("pushkin.test.stats." + uuid.NewV7().String())
	createTopics(t, ctx, brokers, statsTopic)

	producer := newProducer(t, brokers)
	t.Cleanup(producer.Close)
	campaignID := uuid.NewV7()
	snapshot := contracts.CampaignStatsSnapshotV1{
		MessageHeaderV1:    contracts.NewMessageHeaderV1(),
		CampaignID:         campaignID,
		SourceBatchesTotal: 1,
	}
	if err := producer.Produce(ctx, []ports.OutboundKafkaMessage{{
		Topic: statsTopic,
		Key:   []byte(campaignID.String()),
		Value: snapshot,
	}}); err != nil {
		t.Fatalf("produce stats snapshot: %v", err)
	}

	loader, err := adapter.NewCompactedTopicLoader(adapter.CompactedTopicLoaderParams{Brokers: brokers})
	if err != nil {
		t.Fatalf("new compacted topic loader: %v", err)
	}
	loadCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	records, err := loader.Load(loadCtx, statsTopic, []int32{0})
	if err != nil {
		t.Fatalf("load compacted topic: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("loaded record count = %d, want 1", len(records))
	}
	loaded, ok := records[0].Value.(contracts.CampaignStatsSnapshotV1)
	if !ok || loaded.CampaignID != campaignID {
		t.Fatalf("unexpected compacted record: %+v", records[0])
	}
}

func TestCompactedTopicLoaderLoadsEmptySelectedPartition(t *testing.T) {
	ctx := context.Background()
	brokers := startKafka(t, ctx)
	statsTopic := contracts.Topic("pushkin.test.stats.empty." + uuid.NewV7().String())
	createTopics(t, ctx, brokers, statsTopic)

	loader, err := adapter.NewCompactedTopicLoader(adapter.CompactedTopicLoaderParams{Brokers: brokers})
	if err != nil {
		t.Fatalf("new compacted topic loader: %v", err)
	}
	loadCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	records, err := loader.Load(loadCtx, statsTopic, []int32{0})
	if err != nil {
		t.Fatalf("load empty compacted topic: %v", err)
	}
	if len(records) != 0 {
		t.Fatalf("loaded record count = %d, want 0", len(records))
	}
}

func TestConsumerPollsAndCommitsRecord(t *testing.T) {
	ctx := context.Background()
	brokers := startKafka(t, ctx)
	inputTopic := contracts.Topic("pushkin.test.consumer." + uuid.NewV7().String())
	groupID := "pushkin-test-consumer-" + uuid.NewV7().String()
	createTopics(t, ctx, brokers, inputTopic)

	producer := newProducer(t, brokers)
	t.Cleanup(producer.Close)
	input := newDeliveryWork()
	if err := producer.Produce(ctx, []ports.OutboundKafkaMessage{{
		Topic: inputTopic,
		Key:   []byte(input.DeliveryID.String()),
		Value: input,
	}}); err != nil {
		t.Fatalf("produce consumer input: %v", err)
	}

	consumer := newDeliveryConsumer(t, brokers, groupID, inputTopic)
	record, found, err := pollDelivery(t, consumer)
	if err != nil || !found {
		t.Fatalf("poll consumer input: found=%v err=%v", found, err)
	}
	work, ok := record.Value.(contracts.DeliveryWorkV1)
	if !ok || work.DeliveryID != input.DeliveryID {
		t.Fatalf("unexpected consumer input: %+v", record)
	}
	if err := consumer.Commit(ctx, []ports.KafkaRecord{record}); err != nil {
		t.Fatalf("commit consumer input: %v", err)
	}
	consumer.Close()

	resumed := newDeliveryConsumer(t, brokers, groupID, inputTopic)
	t.Cleanup(resumed.Close)
	deadline, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	_, found, err = resumed.Poll(deadline)
	if found || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("committed input record must not be replayed: found=%v err=%v", found, err)
	}
}

func TestUserEventsConsumerPollsAndCommitsRecord(t *testing.T) {
	ctx := context.Background()
	brokers := startKafka(t, ctx)
	userEventsTopic := contracts.Topic(usercontract.UserEventsTopic)
	groupID := "pushkin-test-user-events-" + uuid.NewV7().String()
	createTopics(t, ctx, brokers, userEventsTopic)

	producer, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatalf("new user events producer: %v", err)
	}
	t.Cleanup(producer.Close)
	tenantID := uuid.NewV7()
	event := usercontract.UserEventV1{Type: usercontract.UserEventTypeCreated, TenantID: tenantID.String(), UserID: "user-1"}
	value, err := usercontract.EncodeMessage(event)
	if err != nil {
		t.Fatalf("marshal user event: %v", err)
	}
	if err := producer.ProduceSync(ctx, &kgo.Record{Topic: string(usercontract.UserEventsTopic), Key: usercontract.PartitionKey(event.TenantID, event.UserID), Value: value}).FirstErr(); err != nil {
		t.Fatalf("produce user event: %v", err)
	}

	consumer, err := adapter.NewExternalConsumer(adapter.ExternalConsumerParams{Brokers: brokers, ConsumerGroup: groupID, InputTopic: usercontract.UserEventsTopic})
	if err != nil {
		t.Fatalf("new user events consumer: %v", err)
	}
	pollCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	record, found, err := consumer.Poll(pollCtx)
	cancel()
	if err != nil || !found {
		t.Fatalf("poll user event: found=%v err=%v", found, err)
	}
	decoded, ok := record.Value.(usercontract.UserEventV1)
	if !ok || decoded.Type != event.Type || decoded.TenantID != event.TenantID || decoded.UserID != event.UserID {
		t.Fatalf("user event = %+v, want %+v", record.Value, event)
	}
	if err := consumer.Commit(ctx, record); err != nil {
		t.Fatalf("commit user event: %v", err)
	}
	consumer.Close()

	resumed, err := adapter.NewExternalConsumer(adapter.ExternalConsumerParams{Brokers: brokers, ConsumerGroup: groupID, InputTopic: usercontract.UserEventsTopic})
	if err != nil {
		t.Fatalf("resume user events consumer: %v", err)
	}
	t.Cleanup(resumed.Close)
	deadline, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	_, found, err = resumed.Poll(deadline)
	if found || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("committed user event must not be replayed: found=%v err=%v", found, err)
	}
}

func TestTopicProvisionerEnsuresSystemAndChannelTopics(t *testing.T) {
	ctx := context.Background()
	brokers := startKafka(t, ctx)
	provisioner, err := adapter.NewTopicProvisioner(adapter.TopicProvisionerParams{
		Brokers:           brokers,
		WorkRetention:     24 * time.Hour,
		ProgressRetention: 48 * time.Hour,
	})
	if err != nil {
		t.Fatalf("new topic provisioner: %v", err)
	}
	t.Cleanup(provisioner.Close)

	channelID := uuid.NewV7()
	if err := provisioner.EnsureSystemTopics(ctx); err != nil {
		t.Fatalf("ensure system topics: %v", err)
	}
	if err := provisioner.EnsureChannelTopics(ctx, channelID); err != nil {
		t.Fatalf("ensure channel topics: %v", err)
	}
	if err := provisioner.EnsureSystemTopics(ctx); err != nil {
		t.Fatalf("ensure existing system topics: %v", err)
	}
	if err := provisioner.EnsureChannelTopics(ctx, channelID); err != nil {
		t.Fatalf("ensure existing channel topics: %v", err)
	}

	// A newly constructed producer must be able to publish immediately after
	// provisioning completes. Kafka can acknowledge CreateTopics before the
	// topic metadata is visible to another client.
	producer := newProducer(t, brokers)
	t.Cleanup(producer.Close)
	run := contracts.CampaignRunRequestedV1{
		MessageHeaderV1: contracts.NewMessageHeaderV1(),
		CampaignID:      uuid.NewV7(),
		RunID:           uuid.NewV7(),
	}
	if err := producer.Produce(ctx, []ports.OutboundKafkaMessage{{
		Topic: contracts.TopicCampaignInlineRun,
		Key:   []byte(run.CampaignID.String()),
		Value: run,
	}}); err != nil {
		t.Fatalf("produce immediately after topic provisioning: %v", err)
	}

	want := []contracts.Topic{
		contracts.TopicCampaignBatchedRun,
		contracts.TopicCampaignBatchedSourceBatchFanout,
		contracts.TopicCampaignInlineRun,
		contracts.TopicCampaignInlineFanout,
		contracts.TopicCampaignProgress,
		contracts.TopicCampaignStats,
	}
	for _, priority := range [...]contracts.PriorityV1{
		contracts.PriorityCriticalV1,
		contracts.PriorityHighV1,
		contracts.PriorityNormalV1,
	} {
		topic, err := contracts.DeliveryTopic(priority, channelID)
		if err != nil {
			t.Fatalf("delivery topic: %v", err)
		}
		want = append(want, topic)
	}
	for _, bucket := range [...]contracts.RetryBucketV1{
		contracts.RetryBucketOneMinuteV1,
		contracts.RetryBucketFiveMinutesV1,
		contracts.RetryBucketThirtyMinutesV1,
	} {
		topic, err := contracts.RetryTopic(bucket, channelID)
		if err != nil {
			t.Fatalf("retry topic: %v", err)
		}
		want = append(want, topic)
	}
	assertTopicsExist(t, ctx, brokers, want...)
}

func startKafka(t *testing.T, ctx context.Context) []string {
	t.Helper()
	container, err := testkafka.Run(
		ctx,
		"confluentinc/confluent-local:7.5.0",
		testkafka.WithClusterID("pushkin-test-cluster"),
	)
	if err != nil {
		t.Fatalf("start Kafka test container: %v", err)
	}
	testcontainers.CleanupContainer(t, container)
	brokers, err := container.Brokers(ctx)
	if err != nil {
		t.Fatalf("get Kafka test brokers: %v", err)
	}
	return brokers
}

func createTopics(t *testing.T, ctx context.Context, brokers []string, topics ...contracts.Topic) {
	t.Helper()
	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatalf("new Kafka admin client: %v", err)
	}
	t.Cleanup(client.Close)
	names := make([]string, len(topics))
	for index, topic := range topics {
		names[index] = string(topic)
	}
	responses, err := kadm.NewClient(client).CreateTopics(ctx, 1, 1, nil, names...)
	if err != nil {
		t.Fatalf("create Kafka topics: %v", err)
	}
	for _, topic := range names {
		response, found := responses[topic]
		if !found || response.Err != nil {
			t.Fatalf("create Kafka topic %q: %+v", topic, response)
		}
	}
}

func assertTopicsExist(t *testing.T, ctx context.Context, brokers []string, topics ...contracts.Topic) {
	t.Helper()
	const expectedPushkinTopicPartitions = 4
	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatalf("new Kafka admin client: %v", err)
	}
	t.Cleanup(client.Close)
	names := make([]string, len(topics))
	for index, topic := range topics {
		names[index] = string(topic)
	}
	details, err := kadm.NewClient(client).ListTopics(ctx, names...)
	if err != nil {
		t.Fatalf("list Kafka topics: %v", err)
	}
	for _, name := range names {
		detail, found := details[name]
		if !found {
			t.Fatalf("Kafka topic %q was not created", name)
		}
		if got := len(detail.Partitions); got != expectedPushkinTopicPartitions {
			t.Fatalf("Kafka topic %q partitions = %d, want %d", name, got, expectedPushkinTopicPartitions)
		}
		for partitionID, partition := range detail.Partitions {
			if partition.Err != nil {
				t.Fatalf("Kafka topic %q partition %d error: %v", name, partitionID, partition.Err)
			}
			if partition.Leader < 0 || !slices.Contains(partition.ISR, partition.Leader) {
				t.Fatalf(
					"Kafka topic %q partition %d has no in-sync leader: leader=%d isr=%v",
					name,
					partitionID,
					partition.Leader,
					partition.ISR,
				)
			}
		}
	}
}

func newProducer(t *testing.T, brokers []string) *adapter.Producer {
	t.Helper()
	producer, err := adapter.NewProducer(adapter.ProducerParams{Brokers: brokers})
	if err != nil {
		t.Fatalf("new producer: %v", err)
	}
	return producer
}

func newDeliveryTransactionalConsumer(
	t *testing.T,
	brokers []string,
	groupID string,
	inputTopic contracts.Topic,
) *adapter.TransactionalConsumer {
	t.Helper()
	consumer, err := adapter.NewTransactionalConsumer(adapter.TransactionalConsumerParams{
		Brokers:         brokers,
		ConsumerGroup:   groupID,
		TransactionalID: "pushkin-test-delivery-" + uuid.NewV7().String(),
		InputTopic:      inputTopic,
	})
	if err != nil {
		t.Fatalf("new delivery pipeline: %v", err)
	}
	return consumer
}

type deliveryPoller interface {
	Poll(context.Context) (ports.KafkaRecord, bool, error)
}

func pollDelivery(t *testing.T, consumer deliveryPoller) (ports.KafkaRecord, bool, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	return consumer.Poll(ctx)
}

func newDeliveryConsumer(
	t *testing.T,
	brokers []string,
	groupID string,
	inputTopic contracts.Topic,
) *adapter.Consumer {
	t.Helper()
	consumer, err := adapter.NewConsumer(adapter.ConsumerParams{
		Brokers:       brokers,
		ConsumerGroup: groupID,
		InputTopic:    inputTopic,
	})
	if err != nil {
		t.Fatalf("new delivery consumer: %v", err)
	}
	return consumer
}

func readOutput(t *testing.T, brokers []string, topic contracts.Topic) contracts.DeliveryWorkV1 {
	t.Helper()
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(string(topic)),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
	)
	if err != nil {
		t.Fatalf("new output reader: %v", err)
	}
	t.Cleanup(client.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	fetches := client.PollFetches(ctx)
	if err := fetches.Err(); err != nil {
		t.Fatalf("poll output record: %v", err)
	}
	records := fetches.Records()
	if len(records) != 1 {
		t.Fatalf("unexpected output record count %d", len(records))
	}
	message, err := adapter.DecodeMessage(records[0].Value)
	if err != nil {
		t.Fatalf("decode output record: %v", err)
	}
	work, ok := message.(contracts.DeliveryWorkV1)
	if !ok {
		t.Fatalf("unexpected output type %T", message)
	}
	return work
}

func newDeliveryWork() contracts.DeliveryWorkV1 {
	return contracts.DeliveryWorkV1{
		MessageHeaderV1:    contracts.NewMessageHeaderV1(),
		DeliveryID:         uuid.NewV7(),
		CampaignID:         uuid.NewV7(),
		TenantID:           uuid.NewV7(),
		ChannelID:          uuid.NewV7(),
		PushInstallationID: uuid.NewV7(),
		Priority:           string(contracts.PriorityHighV1),
	}
}
