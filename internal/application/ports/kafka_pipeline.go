package ports

import (
	"context"

	contracts "github.com/superman/pushkin/internal/contracts/kafka"
	"github.com/superman/pushkin/internal/domain"
)

// KafkaRecord identifies a consumed internal Kafka message whose offset may be
// committed only by its owning consumer group transaction.
type KafkaRecord struct {
	Value  contracts.MessageV1
	Offset KafkaOffset
}

type KafkaOffset struct {
	Topic     contracts.Topic
	Partition int32
	Offset    int64
}

// KafkaPartition identifies one partition assigned to the current consumer.
type KafkaPartition struct {
	Topic     contracts.Topic
	Partition int32
}

// KafkaPartitionOffsets contains one consumer position per partition. For a
// commit its value is the next offset; for seek it is the record offset to
// read next.
type KafkaPartitionOffsets map[KafkaPartition]int64

// KafkaOffsetMap coalesces processed records into the next consumer position
// for every affected partition.
func KafkaOffsetMap(records ...KafkaRecord) KafkaPartitionOffsets {
	positions := make(KafkaPartitionOffsets)
	for _, record := range records {
		offset := record.Offset
		partition := KafkaPartition{Topic: offset.Topic, Partition: offset.Partition}
		nextOffset := offset.Offset + 1
		if current, found := positions[partition]; !found || nextOffset > current {
			positions[partition] = nextOffset
		}
	}
	return positions
}

type OutboundKafkaMessage struct {
	Topic contracts.Topic
	Key   []byte
	Value contracts.MessageV1
}

// KafkaConsumer consumes a fixed subscription configured by infrastructure.
// Every input record is decoded through the versioned internal Kafka envelope;
// services validate the message type they expect explicitly.
type KafkaConsumer interface {
	Poll(ctx context.Context) (record KafkaRecord, found bool, err error)
	PollMany(ctx context.Context, limit int) ([]KafkaRecord, error)
	Commit(ctx context.Context, records []KafkaRecord) error
	Pause(ctx context.Context, partitions []KafkaPartition) error
	Resume(ctx context.Context, partitions []KafkaPartition) error
	Seek(ctx context.Context, offsets KafkaPartitionOffsets) error
}

// KafkaTransactionalConsumer is a Kafka-aware consume-produce-offset pipeline
// backed by one consumer group and transactional producer. Complete publishes
// output and commits every record consumed since the previous successful call.
type KafkaTransactionalConsumer interface {
	WaitForAssignment(ctx context.Context) ([]KafkaPartition, error)
	Poll(ctx context.Context) (record KafkaRecord, found bool, err error)
	PollMany(ctx context.Context, limit int) ([]KafkaRecord, error)
	Complete(ctx context.Context, messages []OutboundKafkaMessage) error
	PauseAll(ctx context.Context) error
	ResumeAll(ctx context.Context) error
	Pause(ctx context.Context, partitions []KafkaPartition) error
	Resume(ctx context.Context, partitions []KafkaPartition) error
	Seek(ctx context.Context, offsets KafkaPartitionOffsets) error
}

type KafkaProducer interface {
	Produce(ctx context.Context, messages []OutboundKafkaMessage) error
}

// ChannelTopicProvisioner creates the dynamic delivery and retry topics owned
// by one Channel. Its operation is idempotent.
type ChannelTopicProvisioner interface {
	EnsureChannelTopics(ctx context.Context, channelID domain.ChannelID) error
}

// CompactedTopicLoader reads the specified partitions of a compacted topic
// from their earliest available offsets through their current end offsets. It
// does not join a consumer group or commit offsets.
type CompactedTopicLoader interface {
	Load(ctx context.Context, topic contracts.Topic, partitions []int32) ([]KafkaRecord, error)
}
