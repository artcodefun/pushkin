package kafka

import "github.com/twmb/franz-go/pkg/kgo"

// pollBatch retains the first raw record in each partition, including records
// whose envelopes cannot be decoded. Its owner blocks rebalances until settled.
type pollBatch struct {
	offsets map[string]map[int32]kgo.EpochOffset
}

func (b *pollBatch) capture(records []*kgo.Record) {
	b.offsets = make(map[string]map[int32]kgo.EpochOffset)
	for _, record := range records {
		partitions := b.offsets[record.Topic]
		if partitions == nil {
			partitions = make(map[int32]kgo.EpochOffset)
			b.offsets[record.Topic] = partitions
		}
		if old, ok := partitions[record.Partition]; !ok || record.Offset < old.Offset {
			partitions[record.Partition] = kgo.EpochOffset{Epoch: record.LeaderEpoch, Offset: record.Offset}
		}
	}
}

func (b *pollBatch) rollback(client *kgo.Client) {
	client.SetOffsets(b.offsets)
	b.finish(client)
}

func (b *pollBatch) finish(client *kgo.Client) {
	b.offsets = nil
	client.AllowRebalance()
}

func (b *pollBatch) seek(client *kgo.Client, offsets map[string]map[int32]kgo.EpochOffset) {
	client.SetOffsets(offsets)
	for topic, partitions := range offsets {
		for partition := range partitions {
			delete(b.offsets[topic], partition)
		}
		if len(b.offsets[topic]) == 0 {
			delete(b.offsets, topic)
		}
	}
	if len(b.offsets) == 0 {
		b.finish(client)
	}
}
