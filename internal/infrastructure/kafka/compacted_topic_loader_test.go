package kafka

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"

	contracts "github.com/superman/pushkin/internal/contracts/kafka"
)

func TestCompactedPartitionsAreEmpty(t *testing.T) {
	tests := []struct {
		name    string
		start   map[int32]int64
		end     map[int32]int64
		want    bool
		wantErr bool
	}{
		{name: "empty retained log at nonzero offset", start: map[int32]int64{0: 284413}, end: map[int32]int64{0: 284413}, want: true},
		{name: "retained records", start: map[int32]int64{0: 42}, end: map[int32]int64{0: 43}, want: false},
		{name: "missing start offset", start: map[int32]int64{}, end: map[int32]int64{0: 1}, wantErr: true},
		{name: "missing end offset", start: map[int32]int64{0: 1}, end: map[int32]int64{}, wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			empty, err := compactedPartitionsAreEmpty([]int32{0}, test.start, test.end)
			if (err != nil) != test.wantErr {
				t.Fatalf("error = %v, want error = %t", err, test.wantErr)
			}
			if empty != test.want {
				t.Fatalf("empty = %t, want %t", empty, test.want)
			}
		})
	}
}

func TestCaptureCompactedStableOffsetsKeepsInitialBoundary(t *testing.T) {
	targets := map[int32]int64{0: 10}
	fetches := kgo.Fetches{{Topics: []kgo.FetchTopic{{
		Topic: string(contracts.TopicCampaignStats),
		Partitions: []kgo.FetchPartition{{
			Partition:        0,
			LastStableOffset: 12,
		}},
	}}}}

	if err := captureCompactedStableOffsets(fetches, contracts.TopicCampaignStats, targets); err != nil {
		t.Fatalf("capture stable offsets: %v", err)
	}

	if targets[0] != 10 {
		t.Fatalf("target offset = %d, want 10", targets[0])
	}
}

func TestAllStableOffsetsCaptured(t *testing.T) {
	if allStableOffsetsCaptured([]int32{0, 1}, map[int32]int64{0: 10}) {
		t.Fatal("incomplete stable offsets reported as captured")
	}
	if !allStableOffsetsCaptured([]int32{0, 1}, map[int32]int64{0: 10, 1: 12}) {
		t.Fatal("complete stable offsets reported as incomplete")
	}
}

func TestMarkCompactedPartitionsAtStableEndHandlesInvisibleTail(t *testing.T) {
	targets := map[int32]int64{0: 12}
	remaining := map[int32]struct{}{0: {}}
	fetches := kgo.Fetches{{Topics: []kgo.FetchTopic{{
		Topic: string(contracts.TopicCampaignStats),
		Partitions: []kgo.FetchPartition{{
			Partition:        0,
			LastStableOffset: 12,
		}},
	}}}}

	markCompactedPartitionsAtStableEnd(fetches, contracts.TopicCampaignStats, targets, remaining)

	if len(remaining) != 0 {
		t.Fatalf("remaining = %v, want empty", remaining)
	}
}

func TestMarkCompactedPartitionsAtStableEndKeepsPartitionWithRecords(t *testing.T) {
	targets := map[int32]int64{0: 12}
	remaining := map[int32]struct{}{0: {}}
	fetches := kgo.Fetches{{Topics: []kgo.FetchTopic{{
		Topic: string(contracts.TopicCampaignStats),
		Partitions: []kgo.FetchPartition{{
			Partition:        0,
			LastStableOffset: 12,
			Records:          []*kgo.Record{{Offset: 10}},
		}},
	}}}}

	markCompactedPartitionsAtStableEnd(fetches, contracts.TopicCampaignStats, targets, remaining)

	if _, found := remaining[0]; !found {
		t.Fatal("partition with records was incorrectly marked complete")
	}
}
