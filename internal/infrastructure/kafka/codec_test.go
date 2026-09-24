package kafka

import (
	"testing"
	"uuid"

	contracts "github.com/superman/pushkin/internal/contracts/kafka"
)

func TestDecodeMessage(t *testing.T) {
	t.Parallel()
	campaignID := uuid.NewV7()
	encoded, err := EncodeMessage(contracts.CampaignProgressDeltaV1{
		MessageHeaderV1:       contracts.NewMessageHeaderV1(),
		Type:                  contracts.CampaignProgressEventTypeDeliveryDelta,
		CampaignID:            campaignID,
		DeliveryAcceptedDelta: 2,
	})
	if err != nil {
		t.Fatalf("encode message: %v", err)
	}

	message, err := DecodeMessage(encoded)
	if err != nil {
		t.Fatalf("decode message: %v", err)
	}
	delta, ok := message.(contracts.CampaignProgressDeltaV1)
	if !ok {
		t.Fatalf("unexpected message type %T", message)
	}
	if delta.CampaignID != campaignID || delta.DeliveryAcceptedDelta != 2 {
		t.Fatalf("unexpected progress delta: %+v", delta)
	}
}

func TestDecodeMessageRejectsUnknownType(t *testing.T) {
	t.Parallel()
	_, err := DecodeMessage([]byte(`{"schema_version":1,"type":"unknown","payload":{}}`))
	if err == nil {
		t.Fatal("unknown progress type must be rejected")
	}
}

func TestDecodeMessageRejectsUnknownSchema(t *testing.T) {
	t.Parallel()
	_, err := DecodeMessage([]byte(`{"schema_version":2,"type":"delivery_work","payload":{}}`))
	if err == nil {
		t.Fatal("unknown schema version must be rejected")
	}
}
