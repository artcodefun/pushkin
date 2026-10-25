package kafka

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"

	contract "github.com/superman/pushkin/api/kafka/v1"
)

// ExternalRecord is one decoded record from Pushkin's public Kafka API.
type ExternalRecord struct {
	Key       []byte
	Value     contract.MessageV1
	partition int32
	offset    int64
}

type ExternalConsumerParams struct {
	Brokers       []string
	ConsumerGroup string
	InputTopic    contract.Topic
}

// ExternalConsumer owns a Franz consumer for one public Pushkin Kafka topic.
// It is deliberately non-transactional: external event handlers are
// idempotent and commit an offset only after their application command
// succeeds.
type ExternalConsumer struct {
	batch      pollBatch
	client     *kgo.Client
	inputTopic contract.Topic
}

func NewExternalConsumer(params ExternalConsumerParams) (*ExternalConsumer, error) {
	if len(params.Brokers) == 0 || strings.TrimSpace(params.ConsumerGroup) == "" || params.InputTopic == "" {
		return nil, fmt.Errorf("external Kafka consumer brokers, group, and input topic must not be empty")
	}
	client, err := kgo.NewClient(
		kgo.SeedBrokers(params.Brokers...),
		kgo.ConsumerGroup(params.ConsumerGroup),
		kgo.ConsumeTopics(string(params.InputTopic)),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
	)
	if err != nil {
		return nil, fmt.Errorf("create external Kafka consumer: %w", err)
	}
	return &ExternalConsumer{client: client, inputTopic: params.InputTopic}, nil
}

func (c *ExternalConsumer) Close() {
	c.client.CloseAllowingRebalance()
}

func (c *ExternalConsumer) Poll(ctx context.Context) (result ExternalRecord, found bool, err error) {
	defer func() {
		if err != nil {
			err = errors.Join(err, c.Rollback(ctx))
		}
	}()
	if len(c.batch.offsets) != 0 {
		return ExternalRecord{}, false, fmt.Errorf("Kafka batch is still pending")
	}
	fetches := c.client.PollRecords(ctx, 1)
	c.batch.capture(fetches.Records())
	if err := fetches.Err(); err != nil {
		return ExternalRecord{}, false, fmt.Errorf("poll external Kafka record: %w", err)
	}
	records := fetches.Records()
	if len(records) == 0 {
		c.batch.finish(c.client)
		return ExternalRecord{}, false, nil
	}
	record := records[0]
	if contract.Topic(record.Topic) != c.inputTopic {
		return ExternalRecord{}, false, fmt.Errorf("external Kafka record topic %q does not match configured input topic %q", record.Topic, c.inputTopic)
	}
	value, err := contract.DecodeMessage(record.Value)
	if err != nil {
		return ExternalRecord{}, false, fmt.Errorf("decode external Kafka record at %s[%d] offset %d: %w", record.Topic, record.Partition, record.Offset, err)
	}
	return ExternalRecord{Key: record.Key, Value: value, partition: record.Partition, offset: record.Offset}, true, nil
}

func (c *ExternalConsumer) Commit(ctx context.Context, record ExternalRecord) error {
	if err := c.client.CommitRecords(ctx, &kgo.Record{Topic: string(c.inputTopic), Partition: record.partition, Offset: record.offset}); err != nil {
		return fmt.Errorf("commit external Kafka record: %w", err)
	}
	c.batch.finish(c.client)
	return nil
}

func (c *ExternalConsumer) Rollback(_ context.Context) error {
	c.batch.rollback(c.client)
	return nil
}
