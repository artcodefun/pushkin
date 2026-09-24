package kafka

import (
	"context"
	"testing"
	"time"

	"github.com/superman/pushkin/internal/application/ports"
	contracts "github.com/superman/pushkin/internal/contracts/kafka"
)

func TestNewTransactionalConsumerValidatesParameters(t *testing.T) {
	t.Parallel()
	_, err := NewTransactionalConsumer(TransactionalConsumerParams{
		ConsumerGroup:   "group",
		TransactionalID: "transaction",
		InputTopic:      contracts.Topic("pushkin.delivery.test"),
	})
	if err == nil {
		t.Fatal("missing brokers must be rejected")
	}
}

func TestTransactionalConsumerAssignmentChangesExcludesInitialAssignment(t *testing.T) {
	t.Parallel()
	consumer := TransactionalConsumer{
		inputTopic:        contracts.TopicCampaignProgress,
		assignmentChanges: make(chan struct{}, 1),
	}

	consumer.onPartitionsAssigned(context.Background(), nil, map[string][]int32{
		string(contracts.TopicCampaignProgress): {0},
	})
	select {
	case <-consumer.AssignmentChanges():
		t.Fatal("initial assignment must not trigger a state reset")
	default:
	}

	consumer.onPartitionsAssigned(context.Background(), nil, map[string][]int32{
		string(contracts.TopicCampaignProgress): {1},
	})
	select {
	case <-consumer.AssignmentChanges():
	case <-time.After(time.Second):
		t.Fatal("subsequent assignment must trigger a state reset")
	}

	assignment, err := consumer.WaitForAssignment(context.Background())
	if err != nil {
		t.Fatalf("wait for assignment: %v", err)
	}
	want := []ports.KafkaPartition{{Topic: contracts.TopicCampaignProgress, Partition: 1}}
	if len(assignment) != len(want) || assignment[0] != want[0] {
		t.Fatalf("assignment = %+v, want %+v", assignment, want)
	}
}

func TestTransactionalConsumerAssignmentChangesCoalescesRebalances(t *testing.T) {
	t.Parallel()
	consumer := TransactionalConsumer{
		inputTopic:        contracts.TopicCampaignProgress,
		assignmentReady:   true,
		assignmentChanges: make(chan struct{}, 1),
	}

	for range 3 {
		consumer.onPartitionsAssigned(context.Background(), nil, map[string][]int32{
			string(contracts.TopicCampaignProgress): {0},
		})
	}
	select {
	case <-consumer.AssignmentChanges():
	default:
		t.Fatal("assignment change must be reported")
	}
	select {
	case <-consumer.AssignmentChanges():
		t.Fatal("assignment changes must be coalesced")
	default:
	}
}
