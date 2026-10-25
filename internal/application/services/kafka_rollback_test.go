package services

import (
	"context"
	"errors"
	"testing"

	"github.com/superman/pushkin/internal/application"
	"github.com/superman/pushkin/internal/application/ports"
)

func TestKafkaServicesRollbackFailedBatch(t *testing.T) {
	rollbackFailure := errors.New("rollback failed")
	for _, rollbackErr := range []error{nil, rollbackFailure} {
		for _, name := range []string{"batched run", "batched fanout", "inline run", "inline fanout", "delivery", "retry", "progress", "stats", "notifications"} {
			t.Run(name+"/"+map[bool]string{false: "rollback succeeds", true: "rollback fails"}[rollbackErr != nil], func(t *testing.T) {
				consumer := &rollbackConsumerFake{rollbackErr: rollbackErr}
				var process func(context.Context) error
				switch name {
				case "batched run":
					process = (&BatchedCampaignRunCoordinatorService{kafkaConsumer: consumer}).Process
				case "batched fanout":
					process = (&BatchedSourceBatchFanoutService{kafkaConsumer: consumer}).Process
				case "inline run":
					process = (&InlineCampaignRunCoordinatorService{kafkaConsumer: consumer, batchSize: 1}).Process
				case "inline fanout":
					process = (&InlineCampaignFanoutService{kafkaConsumer: consumer, batchSize: 1}).Process
				case "delivery":
					process = (&DeliveryService{kafkaConsumer: consumer, batchSize: 1}).Process
				case "retry":
					process = (&RetryDeliveryService{kafkaConsumer: consumer}).Process
				case "progress":
					process = (&CampaignProgressAggregatorService{kafkaConsumer: consumer, batchSize: 1, restored: true}).Process
				case "stats":
					process = (&CampaignStatsProjectionService{kafkaConsumer: consumer, batchSize: 1}).Process
				case "notifications":
					process = (&NotificationProjectionService{consumer: consumer, batchSize: 1}).Process
				}
				err := process(context.Background())
				if !errors.Is(err, application.ErrValidation) {
					t.Fatalf("lost processing error: %v", err)
				}
				if consumer.rollbacks != 1 {
					t.Fatalf("rollbacks = %d", consumer.rollbacks)
				}
				if errors.Is(err, rollbackFailure) != (rollbackErr != nil) {
					t.Fatalf("lost rollback outcome: %v", err)
				}
				if consumer.commits != 0 {
					t.Fatal("committed failed batch")
				}
			})
		}
	}
}

type rollbackConsumerFake struct {
	kafkaPipelineControlFake
	rollbackErr error
	rollbacks   int
	commits     int
}

func (c *rollbackConsumerFake) Poll(context.Context) (ports.KafkaRecord, bool, error) {
	return ports.KafkaRecord{}, true, nil
}
func (c *rollbackConsumerFake) PollMany(context.Context, int) ([]ports.KafkaRecord, error) {
	return []ports.KafkaRecord{{}}, nil
}
func (c *rollbackConsumerFake) Rollback(context.Context) error { c.rollbacks++; return c.rollbackErr }
func (c *rollbackConsumerFake) Complete(context.Context, []ports.OutboundKafkaMessage) error {
	c.commits++
	return nil
}
func (c *rollbackConsumerFake) Commit(context.Context, []ports.KafkaRecord) error {
	c.commits++
	return nil
}
