package kafka

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/superman/pushkin/internal/application/ports"
	contracts "github.com/superman/pushkin/internal/contracts/kafka"
)

const compactedTopicFetchMaxWait = 250 * time.Millisecond

// CompactedTopicLoader reads selected compacted-topic partitions without
// participating in a consumer group or committing any offsets.
type CompactedTopicLoader struct {
	brokers []string
}

type CompactedTopicLoaderParams struct {
	Brokers []string
}

func NewCompactedTopicLoader(params CompactedTopicLoaderParams) (*CompactedTopicLoader, error) {
	if len(params.Brokers) == 0 {
		return nil, fmt.Errorf("Kafka brokers must not be empty")
	}
	return &CompactedTopicLoader{brokers: append([]string(nil), params.Brokers...)}, nil
}

func (l *CompactedTopicLoader) Load(
	ctx context.Context,
	topic contracts.Topic,
	partitions []int32,
) ([]ports.KafkaRecord, error) {
	if topic == "" {
		return nil, fmt.Errorf("compacted Kafka topic must not be empty")
	}
	requested, err := requestedCompactedPartitions(topic, partitions)
	if err != nil {
		return nil, err
	}
	if len(requested) == 0 {
		return nil, nil
	}

	client, err := kgo.NewClient(
		kgo.SeedBrokers(l.brokers...),
		kgo.ConsumePartitions(requested),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		kgo.FetchMaxWait(compactedTopicFetchMaxWait),
	)
	if err != nil {
		return nil, fmt.Errorf("create compacted Kafka topic reader: %w", err)
	}
	defer client.Close()
	if empty, err := compactedTopicIsEmpty(ctx, kadm.NewClient(client), topic, partitions); err != nil {
		return nil, err
	} else if empty {
		return nil, nil
	}

	remaining := make(map[int32]struct{}, len(partitions))
	for _, partition := range partitions {
		remaining[partition] = struct{}{}
	}
	targetStableOffsets := make(map[int32]int64, len(partitions))

	records := make([]ports.KafkaRecord, 0)
	for len(remaining) > 0 {
		pollCtx, cancel := context.WithTimeout(ctx, 2*compactedTopicFetchMaxWait)
		fetches := client.PollFetches(pollCtx)
		pollErr := fetches.Err()
		cancel()
		if errors.Is(pollErr, context.DeadlineExceeded) && ctx.Err() == nil {
			if allStableOffsetsCaptured(partitions, targetStableOffsets) {
				// franz-go reports a context deadline rather than an empty fetch
				// after its cursor reaches a quiet tail. The target offsets were
				// captured before this poll loop, so this confirms that every
				// remaining partition has been traversed through its LSO.
				return records, nil
			}
			continue
		}
		if pollErr != nil {
			return nil, fmt.Errorf("poll compacted Kafka topic: %w", pollErr)
		}
		if err := captureCompactedStableOffsets(fetches, topic, targetStableOffsets); err != nil {
			return nil, err
		}
		for _, record := range fetches.Records() {
			if contracts.Topic(record.Topic) != topic {
				return nil, fmt.Errorf("compacted Kafka record topic %q does not match requested topic %q", record.Topic, topic)
			}
			if _, found := remaining[record.Partition]; !found {
				continue
			}
			targetStableOffset, found := targetStableOffsets[record.Partition]
			if !found {
				return nil, fmt.Errorf("compacted Kafka partition %d did not report a last stable offset", record.Partition)
			}
			if record.Offset >= targetStableOffset {
				delete(remaining, record.Partition)
				continue
			}
			value, err := DecodeMessage(record.Value)
			if err != nil {
				return nil, fmt.Errorf("decode compacted Kafka record at %s[%d] offset %d: %w", record.Topic, record.Partition, record.Offset, err)
			}
			records = append(records, ports.KafkaRecord{
				Value:  value,
				Offset: ports.KafkaOffset{Topic: topic, Partition: record.Partition, Offset: record.Offset},
			})
		}
		markCompactedPartitionsAtStableEnd(fetches, topic, targetStableOffsets, remaining)
	}
	return records, nil
}

func compactedTopicIsEmpty(
	ctx context.Context,
	admin *kadm.Client,
	topic contracts.Topic,
	partitions []int32,
) (bool, error) {
	startOffsets, err := admin.ListStartOffsets(ctx, string(topic))
	if err != nil {
		return false, fmt.Errorf("list compacted Kafka topic start offsets: %w", err)
	}
	if err := startOffsets.Error(); err != nil {
		return false, fmt.Errorf("list compacted Kafka topic start offsets: %w", err)
	}
	endOffsets, err := admin.ListEndOffsets(ctx, string(topic))
	if err != nil {
		return false, fmt.Errorf("list compacted Kafka topic end offsets: %w", err)
	}
	if err := endOffsets.Error(); err != nil {
		return false, fmt.Errorf("list compacted Kafka topic end offsets: %w", err)
	}
	startByPartition := make(map[int32]int64, len(partitions))
	endByPartition := make(map[int32]int64, len(partitions))
	for _, partition := range partitions {
		endOffset, found := endOffsets.Lookup(string(topic), partition)
		if !found {
			return false, fmt.Errorf("compacted Kafka topic %q partition %d end offset is unavailable", topic, partition)
		}
		startOffset, found := startOffsets.Lookup(string(topic), partition)
		if !found {
			return false, fmt.Errorf("compacted Kafka topic %q partition %d start offset is unavailable", topic, partition)
		}
		startByPartition[partition] = startOffset.Offset
		endByPartition[partition] = endOffset.Offset
	}
	return compactedPartitionsAreEmpty(partitions, startByPartition, endByPartition)
}

func compactedPartitionsAreEmpty(
	partitions []int32,
	startOffsets map[int32]int64,
	endOffsets map[int32]int64,
) (bool, error) {
	for _, partition := range partitions {
		startOffset, found := startOffsets[partition]
		if !found {
			return false, fmt.Errorf("compacted Kafka partition %d start offset is unavailable", partition)
		}
		endOffset, found := endOffsets[partition]
		if !found {
			return false, fmt.Errorf("compacted Kafka partition %d end offset is unavailable", partition)
		}
		if startOffset != endOffset {
			return false, nil
		}
	}
	return true, nil
}

func allStableOffsetsCaptured(partitions []int32, targetStableOffsets map[int32]int64) bool {
	for _, partition := range partitions {
		if _, captured := targetStableOffsets[partition]; !captured {
			return false
		}
	}
	return true
}

// captureCompactedStableOffsets captures the last stable offset from the first
// response for every partition. It is the read-committed restore boundary;
// later fetches can have a higher value after new records are written.
func captureCompactedStableOffsets(
	fetches kgo.Fetches,
	topic contracts.Topic,
	targetStableOffsets map[int32]int64,
) error {
	var result error
	fetches.EachPartition(func(partition kgo.FetchTopicPartition) {
		if result != nil {
			return
		}
		if contracts.Topic(partition.Topic) != topic {
			result = fmt.Errorf("compacted Kafka fetch topic %q does not match requested topic %q", partition.Topic, topic)
			return
		}
		if _, captured := targetStableOffsets[partition.Partition]; !captured {
			targetStableOffsets[partition.Partition] = partition.LastStableOffset
		}
	})
	return result
}

// markCompactedPartitionsAtStableEnd handles invisible aborted records and
// transactional markers at the tail of a read-committed topic. An empty fetch
// at or beyond the captured stable offset proves that its consumer cursor has
// traversed that tail.
func markCompactedPartitionsAtStableEnd(
	fetches kgo.Fetches,
	topic contracts.Topic,
	targetStableOffsets map[int32]int64,
	remaining map[int32]struct{},
) {
	fetches.EachPartition(func(partition kgo.FetchTopicPartition) {
		if partition.Topic != string(topic) || len(partition.Records) != 0 {
			return
		}
		targetStableOffset, captured := targetStableOffsets[partition.Partition]
		if _, found := remaining[partition.Partition]; found && captured && partition.LastStableOffset >= targetStableOffset {
			delete(remaining, partition.Partition)
		}
	})
}

func requestedCompactedPartitions(topic contracts.Topic, partitions []int32) (map[string]map[int32]kgo.Offset, error) {
	requested := make(map[string]map[int32]kgo.Offset, 1)
	for _, partition := range partitions {
		if partition < 0 {
			return nil, fmt.Errorf("compacted Kafka partition must not be negative")
		}
		if requested[string(topic)] == nil {
			requested[string(topic)] = make(map[int32]kgo.Offset, len(partitions))
		}
		if _, exists := requested[string(topic)][partition]; exists {
			return nil, fmt.Errorf("compacted Kafka partitions must be unique")
		}
		// This one-shot reader has no committed group position. AtStart is its
		// initial seek to the earliest offset still retained by Kafka.
		requested[string(topic)][partition] = kgo.NewOffset().AtStart()
	}
	return requested, nil
}
