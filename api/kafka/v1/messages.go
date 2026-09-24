package v1

import (
	"encoding/json"
	"fmt"
)

const SchemaVersionV1 uint16 = 1

type Topic string

type MessageTypeV1 string

const MessageTypeUserEventV1 MessageTypeV1 = "user_event"

// EnvelopeV1 is the versioned wire shape of every public Pushkin Kafka
// record. Payload contains the JSON representation of the MessageV1 selected
// by Type.
type EnvelopeV1 struct {
	Type          MessageTypeV1   `json:"type"`
	SchemaVersion uint16          `json:"schema_version"`
	Payload       json.RawMessage `json:"payload"`
}

// MessageV1 is a public record accepted by Pushkin's external Kafka API.
// MessageType is encoded in the transport envelope.
type MessageV1 interface {
	isKafkaMessageV1()
	MessageType() MessageTypeV1
}

func EncodeMessage(message MessageV1) ([]byte, error) {
	if message == nil {
		return nil, fmt.Errorf("Kafka message must not be nil")
	}
	payload, err := json.Marshal(message)
	if err != nil {
		return nil, fmt.Errorf("encode Kafka message payload: %w", err)
	}
	encoded, err := json.Marshal(EnvelopeV1{
		Type:          message.MessageType(),
		SchemaVersion: SchemaVersionV1,
		Payload:       payload,
	})
	if err != nil {
		return nil, fmt.Errorf("encode Kafka message envelope: %w", err)
	}
	return encoded, nil
}

func DecodeMessage(data []byte) (MessageV1, error) {
	var envelope EnvelopeV1
	if err := json.Unmarshal(data, &envelope); err != nil {
		return nil, fmt.Errorf("decode Kafka message envelope: %w", err)
	}
	if envelope.SchemaVersion != SchemaVersionV1 {
		return nil, fmt.Errorf("unsupported Kafka schema version %d", envelope.SchemaVersion)
	}

	switch envelope.Type {
	case MessageTypeUserEventV1:
		return decodePayload[UserEventV1](envelope.Payload)
	default:
		return nil, fmt.Errorf("unsupported Kafka message type %q", envelope.Type)
	}
}

func decodePayload[T MessageV1](data json.RawMessage) (T, error) {
	var message T
	if err := json.Unmarshal(data, &message); err != nil {
		return message, fmt.Errorf("decode Kafka message payload: %w", err)
	}
	return message, nil
}
