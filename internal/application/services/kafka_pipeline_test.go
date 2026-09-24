package services

import (
	"context"

	"github.com/superman/pushkin/internal/application/ports"
)

// kafkaPipelineControlFake supplies the control operations that a test does
// not exercise. Stateful pipeline fakes stay next to their owning service.
type kafkaPipelineControlFake struct{}

func (kafkaPipelineControlFake) WaitForAssignment(context.Context) ([]ports.KafkaPartition, error) {
	return nil, nil
}

func (kafkaPipelineControlFake) PauseAll(context.Context) error { return nil }

func (kafkaPipelineControlFake) ResumeAll(context.Context) error { return nil }

func (kafkaPipelineControlFake) Pause(context.Context, []ports.KafkaPartition) error { return nil }

func (kafkaPipelineControlFake) Resume(context.Context, []ports.KafkaPartition) error { return nil }

func (kafkaPipelineControlFake) Seek(context.Context, ports.KafkaPartitionOffsets) error { return nil }
