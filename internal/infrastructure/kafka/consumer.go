package kafka

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/superman/pushkin/internal/application/ports"
	contracts "github.com/superman/pushkin/internal/contracts/kafka"
)

type Consumer struct {
	client     *kgo.Client
	inputTopic contracts.Topic
}

type ConsumerParams struct {
	Brokers       []string
	ConsumerGroup string
	InputTopic    contracts.Topic
}

func NewConsumer(params ConsumerParams) (*Consumer, error) {
	if len(params.Brokers) == 0 || strings.TrimSpace(params.ConsumerGroup) == "" || params.InputTopic == "" {
		return nil, fmt.Errorf("Kafka consumer brokers, group, and input topic must not be empty")
	}
	client, err := kgo.NewClient(
		kgo.SeedBrokers(params.Brokers...),
		kgo.ConsumerGroup(params.ConsumerGroup),
		kgo.ConsumeTopics(string(params.InputTopic)),
		kgo.DisableAutoCommit(),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
	)
	if err != nil {
		return nil, fmt.Errorf("create Kafka consumer: %w", err)
	}
	return &Consumer{client: client, inputTopic: params.InputTopic}, nil
}

func (c *Consumer) Close() { c.client.Close() }

func (c *Consumer) Poll(ctx context.Context) (ports.KafkaRecord, bool, error) {
	records, err := c.PollMany(ctx, 1)
	if err != nil || len(records) == 0 {
		return ports.KafkaRecord{}, false, err
	}
	return records[0], true, nil
}

func (c *Consumer) PollMany(ctx context.Context, limit int) ([]ports.KafkaRecord, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("Kafka poll limit must be positive")
	}
	fetches := c.client.PollRecords(ctx, limit)
	if err := fetches.Err(); err != nil {
		if c.inputTopic == contracts.TopicCampaignStats && isKafkaDataLoss(err) {
			// Campaign stats are absolute snapshots. Franz-go has already reset
			// its cursor to the earliest retained offset, so a later poll can
			// safely resume materializing the current state.
			return nil, nil
		}
		return nil, fmt.Errorf("poll Kafka records: %w", err)
	}
	decoded := make([]ports.KafkaRecord, 0, len(fetches.Records()))
	for _, record := range fetches.Records() {
		if contracts.Topic(record.Topic) != c.inputTopic {
			return nil, fmt.Errorf("Kafka record topic %q does not match configured input topic %q", record.Topic, c.inputTopic)
		}
		value, err := DecodeMessage(record.Value)
		if err != nil {
			return nil, fmt.Errorf("decode Kafka record at %s[%d] offset %d: %w", record.Topic, record.Partition, record.Offset, err)
		}
		decoded = append(decoded, ports.KafkaRecord{Value: value, Offset: ports.KafkaOffset{Topic: c.inputTopic, Partition: record.Partition, Offset: record.Offset}})
	}
	return decoded, nil
}

func isKafkaDataLoss(err error) bool {
	var dataLoss *kgo.ErrDataLoss
	return errors.As(err, &dataLoss)
}

func (c *Consumer) Commit(ctx context.Context, records []ports.KafkaRecord) error {
	if len(records) == 0 {
		return nil
	}
	kgoRecords := make([]*kgo.Record, 0, len(records))
	for _, record := range records {
		if record.Offset.Topic != c.inputTopic {
			return fmt.Errorf("Kafka commit topic %q does not match configured input topic %q", record.Offset.Topic, c.inputTopic)
		}
		kgoRecords = append(kgoRecords, &kgo.Record{Topic: string(record.Offset.Topic), Partition: record.Offset.Partition, Offset: record.Offset.Offset})
	}
	if err := c.client.CommitRecords(ctx, kgoRecords...); err != nil {
		return fmt.Errorf("commit Kafka records: %w", err)
	}
	return nil
}

func (c *Consumer) Pause(_ context.Context, partitions []ports.KafkaPartition) error {
	c.client.PauseFetchPartitions(kgoPartitions(partitions))
	return nil
}

func (c *Consumer) Resume(_ context.Context, partitions []ports.KafkaPartition) error {
	c.client.ResumeFetchPartitions(kgoPartitions(partitions))
	return nil
}

func (c *Consumer) Seek(_ context.Context, offsets ports.KafkaPartitionOffsets) error {
	c.client.SetOffsets(kgoOffsets(offsets))
	return nil
}

func kgoOffsets(offsets ports.KafkaPartitionOffsets) map[string]map[int32]kgo.EpochOffset {
	positions := make(map[string]map[int32]kgo.EpochOffset, len(offsets))
	for partition, offset := range offsets {
		partitions := positions[string(partition.Topic)]
		if partitions == nil {
			partitions = make(map[int32]kgo.EpochOffset)
			positions[string(partition.Topic)] = partitions
		}
		partitions[partition.Partition] = kgo.EpochOffset{Offset: offset}
	}
	return positions
}
