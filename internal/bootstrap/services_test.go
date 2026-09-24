package bootstrap

import (
	"testing"
	"uuid"

	contracts "github.com/superman/pushkin/internal/contracts/kafka"
	"github.com/superman/pushkin/internal/domain"
)

func TestServicesCreateDeliveryProcessors(t *testing.T) {
	t.Parallel()
	servicesBundle := NewServices(&Adapters{}, Config{DeliveryProcessingBatchSize: 100})
	channelID := uuid.NewV7()

	delivery, err := servicesBundle.NewDeliveryService(DeliveryServiceFactoryParams{
		ChannelID: channelID,
		Priority:  domain.PriorityHigh,
	})
	if err != nil || delivery == nil {
		t.Fatalf("create delivery service: service=%v error=%v", delivery, err)
	}
	retry, err := servicesBundle.NewRetryDeliveryService(RetryDeliveryServiceFactoryParams{
		ChannelID: channelID,
		Bucket:    contracts.RetryBucketOneMinuteV1,
	})
	if err != nil || retry == nil {
		t.Fatalf("create retry delivery service: service=%v error=%v", retry, err)
	}
}

func TestDeliveryProcessorFactoriesRejectInvalidTopics(t *testing.T) {
	t.Parallel()
	servicesBundle := NewServices(&Adapters{}, Config{DeliveryProcessingBatchSize: 100})
	channelID := uuid.NewV7()

	if _, err := servicesBundle.NewDeliveryService(DeliveryServiceFactoryParams{
		ChannelID: channelID,
		Priority:  domain.Priority("invalid"),
	}); err == nil {
		t.Fatal("delivery factory must reject an invalid priority")
	}
	if _, err := servicesBundle.NewRetryDeliveryService(RetryDeliveryServiceFactoryParams{
		ChannelID: channelID,
		Bucket:    contracts.RetryBucketV1("invalid"),
	}); err == nil {
		t.Fatal("retry factory must reject an invalid bucket")
	}
}
