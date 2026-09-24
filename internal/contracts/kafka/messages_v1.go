package kafka

import (
	"encoding/json"
	"fmt"
	"time"
	"uuid"

	"github.com/superman/pushkin/internal/domain"
)

const SchemaVersionV1 uint16 = 1

type Topic string

const (
	TopicCampaignBatchedRun               Topic = "pushkin.campaign.batched.run"
	TopicCampaignBatchedSourceBatchFanout Topic = "pushkin.campaign.batched.source-batch.fanout"
	TopicCampaignInlineRun                Topic = "pushkin.campaign.inline.run"
	TopicCampaignInlineFanout             Topic = "pushkin.campaign.inline.fanout"
	TopicCampaignProgress                 Topic = "pushkin.campaign.progress"
	TopicCampaignStats                    Topic = "pushkin.campaign.stats"
)

type PriorityV1 string

const (
	PriorityCriticalV1 PriorityV1 = "critical"
	PriorityHighV1     PriorityV1 = "high"
	PriorityNormalV1   PriorityV1 = "normal"
)

func (p PriorityV1) IsValid() bool {
	return p == PriorityCriticalV1 || p == PriorityHighV1 || p == PriorityNormalV1
}

type RetryBucketV1 string

const (
	RetryBucketOneMinuteV1     RetryBucketV1 = "1m"
	RetryBucketFiveMinutesV1   RetryBucketV1 = "5m"
	RetryBucketThirtyMinutesV1 RetryBucketV1 = "30m"
)

func (b RetryBucketV1) IsValid() bool {
	return b == RetryBucketOneMinuteV1 ||
		b == RetryBucketFiveMinutesV1 ||
		b == RetryBucketThirtyMinutesV1
}

type CampaignRecipientModeV1 string

const (
	CampaignRecipientModeBatchedV1 CampaignRecipientModeV1 = "batched"
	CampaignRecipientModeInlineV1  CampaignRecipientModeV1 = "inline"
)

func (m CampaignRecipientModeV1) IsValid() bool {
	return m == CampaignRecipientModeBatchedV1 || m == CampaignRecipientModeInlineV1
}

func DeliveryTopic(priority PriorityV1, channelID uuid.UUID) (Topic, error) {
	if !priority.IsValid() {
		return "", fmt.Errorf("invalid delivery topic priority %q", priority)
	}
	if channelID == uuid.Nil() {
		return "", fmt.Errorf("delivery topic channel_id must not be nil")
	}
	return Topic("pushkin.delivery." + string(priority) + "." + channelID.String()), nil
}

func RetryTopic(bucket RetryBucketV1, channelID uuid.UUID) (Topic, error) {
	if !bucket.IsValid() {
		return "", fmt.Errorf("invalid retry topic bucket %q", bucket)
	}
	if channelID == uuid.Nil() {
		return "", fmt.Errorf("retry topic channel_id must not be nil")
	}
	return Topic("pushkin.retry." + string(bucket) + "." + channelID.String()), nil
}

// CampaignRunTopic returns the fixed input topic for one campaign recipient mode.
func CampaignRunTopic(recipientMode CampaignRecipientModeV1) (Topic, error) {
	switch recipientMode {
	case CampaignRecipientModeBatchedV1:
		return TopicCampaignBatchedRun, nil
	case CampaignRecipientModeInlineV1:
		return TopicCampaignInlineRun, nil
	default:
		return "", fmt.Errorf("invalid campaign recipient mode %q", recipientMode)
	}
}

type MessageTypeV1 string

const (
	MessageTypeCampaignRunRequestedV1              MessageTypeV1 = "campaign_run_requested"
	MessageTypeBatchedSourceBatchFanoutV1          MessageTypeV1 = "batched_source_batch_fanout"
	MessageTypeInlineCampaignFanoutV1              MessageTypeV1 = "inline_campaign_fanout"
	MessageTypeDeliveryWorkV1                      MessageTypeV1 = "delivery_work"
	MessageTypeRetryWorkV1                         MessageTypeV1 = "retry_work"
	MessageTypeCampaignRunStartedV1                MessageTypeV1 = "campaign_run_started"
	MessageTypeBatchedSourceBatchFanoutCompletedV1 MessageTypeV1 = "batched_source_batch_fanout_completed"
	MessageTypeInlineCampaignFanoutCompletedV1     MessageTypeV1 = "inline_campaign_fanout_completed"
	MessageTypeCampaignProgressDeltaV1             MessageTypeV1 = "campaign_progress_delta"
	MessageTypeCampaignStatsSnapshotV1             MessageTypeV1 = "campaign_stats_snapshot"
)

// EnvelopeV1 is the versioned wire shape of every internal Kafka record.
// Payload contains the JSON representation of the MessageV1 selected by Type.
type EnvelopeV1 struct {
	Type          MessageTypeV1   `json:"type"`
	SchemaVersion uint16          `json:"schema_version"`
	Payload       json.RawMessage `json:"payload"`
}

// MessageV1 is a sealed record that may be published to the internal Kafka
// pipeline. MessageType is encoded in the transport envelope.
type MessageV1 interface {
	isKafkaMessageV1()
	MessageType() MessageTypeV1
}

// MessageHeaderV1 contains transport metadata common to all internal records.
type MessageHeaderV1 struct {
	SchemaVersion uint16 `json:"schema_version"`
}

func NewMessageHeaderV1() MessageHeaderV1 {
	return MessageHeaderV1{
		SchemaVersion: SchemaVersionV1,
	}
}

func (MessageHeaderV1) isKafkaMessageV1() {}

// CampaignRunRequestedV1 requests processing of one fenced campaign run.
// Consumers must reject a record whose RunID is no longer current.
type CampaignRunRequestedV1 struct {
	MessageHeaderV1
	CampaignID uuid.UUID `json:"campaign_id"`
	RunID      uuid.UUID `json:"run_id"`
}

func (CampaignRunRequestedV1) MessageType() MessageTypeV1 {
	return MessageTypeCampaignRunRequestedV1
}

// BatchedSourceBatchFanoutV1 requests resolution of one durable source batch into
// individual delivery work records.
type BatchedSourceBatchFanoutV1 struct {
	MessageHeaderV1
	SourceBatchID uuid.UUID `json:"source_batch_id"`
}

func (BatchedSourceBatchFanoutV1) MessageType() MessageTypeV1 {
	return MessageTypeBatchedSourceBatchFanoutV1
}

// InlineCampaignFanoutV1 resolves a small immutable campaign recipient set
// into delivery work records.
type InlineCampaignFanoutV1 struct {
	MessageHeaderV1
	CampaignID uuid.UUID `json:"campaign_id"`
	Recipients []string  `json:"recipients"`
}

func (InlineCampaignFanoutV1) MessageType() MessageTypeV1 { return MessageTypeInlineCampaignFanoutV1 }

// DeliveryWorkV1 is one logical provider delivery to a push installation. It
// is used both in a delivery topic and as the immutable portion of RetryWorkV1.
type DeliveryWorkV1 struct {
	MessageHeaderV1
	DeliveryID         uuid.UUID `json:"delivery_id"`
	CampaignID         uuid.UUID `json:"campaign_id"`
	TenantID           uuid.UUID `json:"tenant_id"`
	ChannelID          uuid.UUID `json:"channel_id"`
	PushInstallationID uuid.UUID `json:"push_installation_id"`
	Priority           string    `json:"priority"`
	RetryAttempt       uint      `json:"retry_attempt"`
}

// NewDeliveryWorkV1 translates one domain delivery work item into its
// immutable internal Kafka record.
func NewDeliveryWorkV1(work domain.DeliveryWork) DeliveryWorkV1 {
	return DeliveryWorkV1{
		MessageHeaderV1:    NewMessageHeaderV1(),
		DeliveryID:         work.ID(),
		CampaignID:         work.CampaignID(),
		TenantID:           work.TenantID(),
		ChannelID:          work.ChannelID(),
		PushInstallationID: work.PushInstallationID(),
		Priority:           string(work.Priority()),
		RetryAttempt:       work.RetryAttempt(),
	}
}

func (DeliveryWorkV1) MessageType() MessageTypeV1 { return MessageTypeDeliveryWorkV1 }

// RetryWorkV1 delays a DeliveryWorkV1 until DueAt. It retains the same
// DeliveryID and routing fields across retry buckets.
type RetryWorkV1 struct {
	DeliveryWorkV1
	DueAt time.Time `json:"due_at"`
}

func (RetryWorkV1) MessageType() MessageTypeV1 { return MessageTypeRetryWorkV1 }

type CampaignProgressEventTypeV1 string

const (
	CampaignProgressEventTypeRunStarted           CampaignProgressEventTypeV1 = "campaign_run_started"
	CampaignProgressEventTypeSourceBatchFannedOut CampaignProgressEventTypeV1 = "source_batch_fanout_completed"
	CampaignProgressEventTypeDeliveryDelta        CampaignProgressEventTypeV1 = "delivery_delta"
)

// CampaignProgressMessageV1 is the closed set of records accepted by the
// progress aggregator from pushkin.campaign.progress.
type CampaignProgressMessageV1 interface {
	MessageV1
	isCampaignProgressMessageV1()
}

// CampaignRunStartedV1 initializes aggregation for a fenced campaign run.
type CampaignRunStartedV1 struct {
	MessageHeaderV1
	Type               CampaignProgressEventTypeV1 `json:"type"`
	CampaignID         uuid.UUID                   `json:"campaign_id"`
	SourceBatchesTotal uint64                      `json:"source_batches_total"`
}

func (CampaignRunStartedV1) isCampaignProgressMessageV1() {}
func (CampaignRunStartedV1) MessageType() MessageTypeV1   { return MessageTypeCampaignRunStartedV1 }

// BatchedSourceBatchFanoutCompletedV1 reports every delivery work record created for
// one source batch.
type BatchedSourceBatchFanoutCompletedV1 struct {
	MessageHeaderV1
	Type          CampaignProgressEventTypeV1 `json:"type"`
	CampaignID    uuid.UUID                   `json:"campaign_id"`
	SourceBatchID uuid.UUID                   `json:"source_batch_id"`
	DeliveryCount uint64                      `json:"delivery_count"`
}

func (BatchedSourceBatchFanoutCompletedV1) isCampaignProgressMessageV1() {}
func (BatchedSourceBatchFanoutCompletedV1) MessageType() MessageTypeV1 {
	return MessageTypeBatchedSourceBatchFanoutCompletedV1
}

// InlineCampaignFanoutCompletedV1 reports delivery work created for the one
// logical recipient input of an inline campaign.
type InlineCampaignFanoutCompletedV1 struct {
	MessageHeaderV1
	Type          CampaignProgressEventTypeV1 `json:"type"`
	CampaignID    uuid.UUID                   `json:"campaign_id"`
	DeliveryCount uint64                      `json:"delivery_count"`
}

func (InlineCampaignFanoutCompletedV1) isCampaignProgressMessageV1() {}
func (InlineCampaignFanoutCompletedV1) MessageType() MessageTypeV1 {
	return MessageTypeInlineCampaignFanoutCompletedV1
}

// CampaignProgressDeltaV1 reports terminal provider outcomes for a processing
// transaction. Retried items are deliberately absent until terminal.
type CampaignProgressDeltaV1 struct {
	MessageHeaderV1
	Type                  CampaignProgressEventTypeV1 `json:"type"`
	CampaignID            uuid.UUID                   `json:"campaign_id"`
	DeliveryAcceptedDelta uint64                      `json:"delivery_accepted_delta"`
	DeliveryFailedDelta   uint64                      `json:"delivery_failed_delta"`
}

func (CampaignProgressDeltaV1) isCampaignProgressMessageV1() {}
func (CampaignProgressDeltaV1) MessageType() MessageTypeV1   { return MessageTypeCampaignProgressDeltaV1 }

// CampaignStatsSnapshotV1 is the compacted absolute aggregate state for one
// campaign.
type CampaignStatsSnapshotV1 struct {
	MessageHeaderV1
	CampaignID             uuid.UUID `json:"campaign_id"`
	SourceBatchesTotal     uint64    `json:"source_batches_total"`
	SourceBatchesFannedOut uint64    `json:"source_batches_fanned_out"`
	DeliveryTotal          uint64    `json:"delivery_total"`
	DeliveryProcessed      uint64    `json:"delivery_processed"`
	DeliveryAcceptedCount  uint64    `json:"delivery_accepted_count"`
	DeliveryFailedCount    uint64    `json:"delivery_failed_count"`
}

func (CampaignStatsSnapshotV1) MessageType() MessageTypeV1 { return MessageTypeCampaignStatsSnapshotV1 }
