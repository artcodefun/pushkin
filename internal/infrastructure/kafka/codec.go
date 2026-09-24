package kafka

import (
	"encoding/json"
	"fmt"

	contracts "github.com/superman/pushkin/internal/contracts/kafka"
)

func EncodeMessage(message contracts.MessageV1) ([]byte, error) {
	if message == nil {
		return nil, fmt.Errorf("Kafka message must not be nil")
	}
	payload, err := json.Marshal(message)
	if err != nil {
		return nil, fmt.Errorf("encode Kafka message payload: %w", err)
	}
	return json.Marshal(contracts.EnvelopeV1{
		Type:          message.MessageType(),
		SchemaVersion: contracts.SchemaVersionV1,
		Payload:       payload,
	})
}

func DecodeMessage(data []byte) (contracts.MessageV1, error) {
	var envelope contracts.EnvelopeV1
	if err := json.Unmarshal(data, &envelope); err != nil {
		return nil, fmt.Errorf("decode Kafka message envelope: %w", err)
	}
	if envelope.SchemaVersion != contracts.SchemaVersionV1 {
		return nil, fmt.Errorf("unsupported Kafka schema version %d", envelope.SchemaVersion)
	}

	switch envelope.Type {
	case contracts.MessageTypeCampaignRunRequestedV1:
		return decodePayload[contracts.CampaignRunRequestedV1](envelope.Payload)
	case contracts.MessageTypeBatchedSourceBatchFanoutV1:
		return decodePayload[contracts.BatchedSourceBatchFanoutV1](envelope.Payload)
	case contracts.MessageTypeInlineCampaignFanoutV1:
		return decodePayload[contracts.InlineCampaignFanoutV1](envelope.Payload)
	case contracts.MessageTypeDeliveryWorkV1:
		return decodePayload[contracts.DeliveryWorkV1](envelope.Payload)
	case contracts.MessageTypeRetryWorkV1:
		return decodePayload[contracts.RetryWorkV1](envelope.Payload)
	case contracts.MessageTypeCampaignRunStartedV1:
		return decodePayload[contracts.CampaignRunStartedV1](envelope.Payload)
	case contracts.MessageTypeBatchedSourceBatchFanoutCompletedV1:
		return decodePayload[contracts.BatchedSourceBatchFanoutCompletedV1](envelope.Payload)
	case contracts.MessageTypeInlineCampaignFanoutCompletedV1:
		return decodePayload[contracts.InlineCampaignFanoutCompletedV1](envelope.Payload)
	case contracts.MessageTypeCampaignProgressDeltaV1:
		return decodePayload[contracts.CampaignProgressDeltaV1](envelope.Payload)
	case contracts.MessageTypeCampaignStatsSnapshotV1:
		return decodePayload[contracts.CampaignStatsSnapshotV1](envelope.Payload)
	default:
		return nil, fmt.Errorf("unsupported Kafka message type %q", envelope.Type)
	}
}

func decodePayload[T contracts.MessageV1](data json.RawMessage) (T, error) {
	var message T
	if err := json.Unmarshal(data, &message); err != nil {
		return message, fmt.Errorf("decode Kafka message payload: %w", err)
	}
	return message, nil
}
