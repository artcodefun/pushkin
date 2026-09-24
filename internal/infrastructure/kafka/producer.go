package kafka

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/superman/pushkin/internal/application/ports"
)

type Producer struct {
	client *kgo.Client
}

type ProducerParams struct {
	Brokers []string
}

func NewProducer(params ProducerParams) (*Producer, error) {
	if len(params.Brokers) == 0 {
		return nil, fmt.Errorf("Kafka brokers must not be empty")
	}
	client, err := kgo.NewClient(
		kgo.SeedBrokers(params.Brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	if err != nil {
		return nil, fmt.Errorf("create Kafka producer: %w", err)
	}
	return &Producer{client: client}, nil
}

func (p *Producer) Close() {
	p.client.Close()
}

func (p *Producer) Produce(ctx context.Context, messages []ports.OutboundKafkaMessage) error {
	return produce(ctx, p.client.ProduceSync, messages)
}
