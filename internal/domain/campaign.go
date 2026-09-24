package domain

import (
	"fmt"
	"strings"
	"time"
	"uuid"
)

type CampaignStatus string

const (
	CampaignStatusDraft     CampaignStatus = "draft"
	CampaignStatusScheduled CampaignStatus = "scheduled"
	CampaignStatusStarting  CampaignStatus = "starting"
	CampaignStatusStarted   CampaignStatus = "started"
	CampaignStatusCompleted CampaignStatus = "completed"
	CampaignStatusFailed    CampaignStatus = "failed"
)

type CampaignRecipientMode string

const (
	CampaignRecipientModeBatched CampaignRecipientMode = "batched"
	CampaignRecipientModeInline  CampaignRecipientMode = "inline"
)

const MaxInlineCampaignRecipients = 100

type NewBatchedCampaignParams struct {
	TenantID    TenantID
	ChannelID   ChannelID
	PushPayload PushPayload
	Priority    Priority
}

type NewInlineCampaignParams struct {
	TenantID    TenantID
	ChannelID   ChannelID
	PushPayload PushPayload
	Priority    Priority
	Recipients  []UserID
	ScheduledAt *time.Time
	Now         time.Time
}

type Campaign struct {
	id               CampaignID
	tenantID         TenantID
	channelID        ChannelID
	pushPayload      PushPayload
	priority         Priority
	recipientMode    CampaignRecipientMode
	inlineRecipients []UserID
	status           CampaignStatus
	scheduledAt      *time.Time
	runID            *RunID
	runAttemptedAt   *time.Time
	startedAt        *time.Time
	completedAt      *time.Time
	failedAt         *time.Time
	failureReason    string
	currentProgress  *CampaignProgress
}

func NewBatchedCampaign(params NewBatchedCampaignParams) (*Campaign, error) {
	return newCampaign(
		params.TenantID,
		params.ChannelID,
		params.PushPayload,
		params.Priority,
		CampaignRecipientModeBatched,
	)
}

// NewInlineCampaign creates a small immutable campaign ready to start.
// It enters SCHEDULED only when ScheduledAt is in the future; an absent or due
// timestamp starts it immediately.
func NewInlineCampaign(params NewInlineCampaignParams) (*Campaign, error) {
	if len(params.Recipients) == 0 || len(params.Recipients) > MaxInlineCampaignRecipients {
		return nil, fmt.Errorf("%w: inline campaign recipient count must be between 1 and %d", ErrInvalidArgument, MaxInlineCampaignRecipients)
	}
	for _, recipient := range params.Recipients {
		if strings.TrimSpace(string(recipient)) == "" {
			return nil, fmt.Errorf("%w: inline campaign user_id must not be empty", ErrInvalidArgument)
		}
	}
	campaign, err := newCampaign(
		params.TenantID,
		params.ChannelID,
		params.PushPayload,
		params.Priority,
		CampaignRecipientModeInline,
	)
	if err != nil {
		return nil, err
	}
	campaign.inlineRecipients = cloneUserIDs(params.Recipients)
	if params.ScheduledAt == nil || !params.ScheduledAt.After(params.Now) {
		if err := campaign.RequestStart(); err != nil {
			return nil, err
		}
		return campaign, nil
	}
	if err := campaign.Schedule(*params.ScheduledAt, params.Now); err != nil {
		return nil, err
	}
	return campaign, nil
}

func newCampaign(
	tenantID TenantID,
	channelID ChannelID,
	pushPayload PushPayload,
	priority Priority,
	recipientMode CampaignRecipientMode,
) (*Campaign, error) {
	if err := requireUUID("tenant_id", tenantID); err != nil {
		return nil, err
	}
	if err := requireUUID("channel_id", channelID); err != nil {
		return nil, err
	}
	if !priority.IsValid() {
		return nil, fmt.Errorf("%w: invalid campaign priority", ErrInvalidArgument)
	}
	if !recipientMode.isValid() {
		return nil, fmt.Errorf("%w: invalid campaign recipient mode", ErrInvalidArgument)
	}
	if err := pushPayload.validate(); err != nil {
		return nil, err
	}
	return &Campaign{
		id:            uuid.NewV7(),
		tenantID:      tenantID,
		channelID:     channelID,
		pushPayload:   pushPayload,
		priority:      priority,
		recipientMode: recipientMode,
		status:        CampaignStatusDraft,
	}, nil
}

// Schedule closes recipient import and waits until scheduled_at becomes due.
func (c *Campaign) Schedule(scheduledAt, now time.Time) error {
	if c.status != CampaignStatusDraft {
		return c.transitionError(CampaignStatusScheduled)
	}
	if !scheduledAt.After(now) {
		return fmt.Errorf("%w: scheduled_at must be in the future", ErrInvalidArgument)
	}

	c.status = CampaignStatusScheduled
	c.scheduledAt = cloneTime(&scheduledAt)
	return nil
}

// RequestStart closes recipient import for an immediate campaign. A scheduler
// subsequently creates the fencing run and publishes it.
func (c *Campaign) RequestStart() error {
	if c.status != CampaignStatusDraft {
		return c.transitionError(CampaignStatusStarting)
	}

	c.status = CampaignStatusStarting
	return nil
}

// CanBeginRun reports whether a due campaign may receive a new fencing run.
func (c *Campaign) CanBeginRun(now time.Time, retryAfter time.Duration) bool {
	if now.IsZero() || retryAfter <= 0 {
		return false
	}

	switch c.status {
	case CampaignStatusScheduled:
		return c.scheduledAt != nil && !c.scheduledAt.After(now)
	case CampaignStatusStarting:
		return c.runAttemptedAt == nil || !c.runAttemptedAt.After(now.Add(-retryAfter))
	default:
		return false
	}
}

// BeginRun starts a due campaign or fences a stale STARTING run. Every created
// run has a new fencing token, invalidating all previously published requests.
func (c *Campaign) BeginRun(now time.Time, retryAfter time.Duration) (RunID, error) {
	if now.IsZero() {
		return uuid.Nil(), fmt.Errorf("%w: start time must not be zero", ErrInvalidArgument)
	}
	if retryAfter <= 0 {
		return uuid.Nil(), fmt.Errorf("%w: retry delay must be positive", ErrInvalidArgument)
	}
	if !c.CanBeginRun(now, retryAfter) {
		return uuid.Nil(), c.transitionError(CampaignStatusStarting)
	}

	runID := uuid.NewV7()
	c.status = CampaignStatusStarting
	c.runID = cloneRunID(&runID)
	c.runAttemptedAt = cloneTime(&now)
	return runID, nil
}

// MarkStarted is idempotent for replay of the currently fenced run. A replay
// must still continue with Kafka fan-out; the return value reports whether the
// PostgreSQL state changed during this call.
func (c *Campaign) MarkStarted(runID RunID, now time.Time) (bool, error) {
	if err := requireTime("started_at", now); err != nil {
		return false, err
	}
	if c.runID == nil || *c.runID != runID {
		return false, fmt.Errorf("%w: expected current run_id", ErrRunMismatch)
	}

	switch c.status {
	case CampaignStatusStarting:
		c.status = CampaignStatusStarted
		c.startedAt = cloneTime(&now)
		return true, nil
	case CampaignStatusStarted:
		return false, nil
	default:
		return false, c.transitionError(CampaignStatusStarted)
	}
}

func (c *Campaign) Complete(now time.Time) error {
	if err := requireTime("completed_at", now); err != nil {
		return err
	}
	if c.status == CampaignStatusCompleted {
		return nil
	}
	if c.status != CampaignStatusStarted {
		return c.transitionError(CampaignStatusCompleted)
	}

	c.status = CampaignStatusCompleted
	c.completedAt = cloneTime(&now)
	return nil
}

// ApplyCurrentProgress replaces Campaign's current delivery state with a
// snapshot from the progress pipeline. Progress is monotonic; a stale snapshot
// must not overwrite a newer state.
func (c *Campaign) ApplyCurrentProgress(progress *CampaignProgress, now time.Time) error {
	if progress == nil {
		return fmt.Errorf("%w: campaign progress must not be nil", ErrInvalidArgument)
	}
	if err := requireTime("progress_updated_at", now); err != nil {
		return err
	}
	if c.status == CampaignStatusCompleted {
		if !progress.Equals(c.currentProgress) {
			return fmt.Errorf("%w: completed campaign accepts only its current progress", ErrInvalidTransition)
		}
		return nil
	}
	if c.status != CampaignStatusStarted {
		return c.transitionError(CampaignStatusStarted)
	}
	if c.currentProgress != nil && !progress.IsMonotonicFrom(c.currentProgress) {
		return fmt.Errorf("%w: campaign progress must be monotonic", ErrInvalidArgument)
	}
	copy := progress.Copy()
	c.currentProgress = copy
	if copy.IsComplete() {
		return c.Complete(now)
	}
	return nil
}

func (c *Campaign) Fail(reason string, now time.Time) error {
	if strings.TrimSpace(reason) == "" {
		return fmt.Errorf("%w: failure reason must not be empty", ErrInvalidArgument)
	}
	if err := requireTime("failed_at", now); err != nil {
		return err
	}
	if c.status == CampaignStatusCompleted {
		return c.transitionError(CampaignStatusFailed)
	}
	if c.status == CampaignStatusFailed {
		return nil
	}

	c.status = CampaignStatusFailed
	c.failureReason = reason
	c.failedAt = cloneTime(&now)
	return nil
}

func (c *Campaign) CanAddRecipients() bool {
	return c.recipientMode == CampaignRecipientModeBatched && c.status == CampaignStatusDraft
}

func (c *Campaign) ID() CampaignID                       { return c.id }
func (c *Campaign) TenantID() TenantID                   { return c.tenantID }
func (c *Campaign) ChannelID() ChannelID                 { return c.channelID }
func (c *Campaign) PushPayload() PushPayload             { return c.pushPayload }
func (c *Campaign) Priority() Priority                   { return c.priority }
func (c *Campaign) RecipientMode() CampaignRecipientMode { return c.recipientMode }
func (c *Campaign) InlineRecipients() []UserID           { return cloneUserIDs(c.inlineRecipients) }
func (c *Campaign) Status() CampaignStatus               { return c.status }
func (c *Campaign) ScheduledAt() *time.Time              { return cloneTime(c.scheduledAt) }
func (c *Campaign) RunID() *RunID                        { return cloneRunID(c.runID) }
func (c *Campaign) RunAttemptedAt() *time.Time           { return cloneTime(c.runAttemptedAt) }
func (c *Campaign) StartedAt() *time.Time                { return cloneTime(c.startedAt) }
func (c *Campaign) CompletedAt() *time.Time              { return cloneTime(c.completedAt) }
func (c *Campaign) FailedAt() *time.Time                 { return cloneTime(c.failedAt) }
func (c *Campaign) FailureReason() string                { return c.failureReason }
func (c *Campaign) CurrentProgress() *CampaignProgress   { return c.currentProgress.Copy() }

func (c *Campaign) transitionError(target CampaignStatus) error {
	return fmt.Errorf("%w: campaign cannot transition from %s to %s", ErrInvalidTransition, c.status, target)
}

func (s CampaignStatus) isValid() bool {
	switch s {
	case CampaignStatusDraft, CampaignStatusScheduled, CampaignStatusStarting,
		CampaignStatusStarted, CampaignStatusCompleted, CampaignStatusFailed:
		return true
	default:
		return false
	}
}

func (s CampaignStatus) IsTerminal() bool {
	return s == CampaignStatusCompleted || s == CampaignStatusFailed
}

func (m CampaignRecipientMode) isValid() bool {
	return m == CampaignRecipientModeBatched || m == CampaignRecipientModeInline
}

func cloneTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func cloneRunID(value *RunID) *RunID {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func cloneUserIDs(value []UserID) []UserID {
	if value == nil {
		return nil
	}
	return append([]UserID(nil), value...)
}

type HydrateCampaignParams struct {
	ID               CampaignID
	TenantID         TenantID
	ChannelID        ChannelID
	PushPayload      PushPayload
	Priority         Priority
	RecipientMode    CampaignRecipientMode
	InlineRecipients []UserID
	Status           CampaignStatus
	ScheduledAt      *time.Time
	RunID            *RunID
	RunAttemptedAt   *time.Time
	StartedAt        *time.Time
	CompletedAt      *time.Time
	FailedAt         *time.Time
	FailureReason    string
	CurrentProgress  *CampaignProgress
}

func HydrateCampaign(params HydrateCampaignParams) (*Campaign, error) {
	if err := requireUUID("campaign_id", params.ID); err != nil {
		return nil, err
	}
	if err := requireUUID("tenant_id", params.TenantID); err != nil {
		return nil, err
	}
	if err := requireUUID("channel_id", params.ChannelID); err != nil {
		return nil, err
	}
	if err := params.PushPayload.validate(); err != nil {
		return nil, err
	}
	if !params.Priority.IsValid() {
		return nil, fmt.Errorf("%w: invalid campaign priority", ErrInvalidArgument)
	}
	if !params.RecipientMode.isValid() {
		return nil, fmt.Errorf("%w: invalid campaign recipient mode", ErrInvalidArgument)
	}
	if params.RecipientMode == CampaignRecipientModeInline {
		if len(params.InlineRecipients) == 0 || len(params.InlineRecipients) > MaxInlineCampaignRecipients {
			return nil, fmt.Errorf("%w: invalid inline campaign recipient count", ErrInvalidArgument)
		}
	}
	if params.RecipientMode == CampaignRecipientModeBatched && len(params.InlineRecipients) != 0 {
		return nil, fmt.Errorf("%w: batched campaign must not have inline recipients", ErrInvalidArgument)
	}
	if !params.Status.isValid() {
		return nil, fmt.Errorf("%w: invalid campaign status", ErrInvalidArgument)
	}
	if params.RunID != nil {
		if err := requireUUID("run_id", *params.RunID); err != nil {
			return nil, err
		}
	}
	if params.Status == CampaignStatusScheduled && params.ScheduledAt == nil {
		return nil, fmt.Errorf("%w: scheduled campaign must have scheduled_at", ErrInvalidArgument)
	}
	if (params.Status == CampaignStatusStarted || params.Status == CampaignStatusCompleted) &&
		(params.RunID == nil || params.StartedAt == nil) {
		return nil, fmt.Errorf("%w: started campaign must have run_id and started_at", ErrInvalidArgument)
	}
	if params.Status == CampaignStatusCompleted && params.CompletedAt == nil {
		return nil, fmt.Errorf("%w: completed campaign must have completed_at", ErrInvalidArgument)
	}
	if params.Status == CampaignStatusFailed &&
		(params.FailedAt == nil || strings.TrimSpace(params.FailureReason) == "") {
		return nil, fmt.Errorf("%w: failed campaign must have failure details", ErrInvalidArgument)
	}

	return &Campaign{
		id:               params.ID,
		tenantID:         params.TenantID,
		channelID:        params.ChannelID,
		pushPayload:      params.PushPayload,
		priority:         params.Priority,
		recipientMode:    params.RecipientMode,
		inlineRecipients: cloneUserIDs(params.InlineRecipients),
		status:           params.Status,
		scheduledAt:      cloneTime(params.ScheduledAt),
		runID:            cloneRunID(params.RunID),
		runAttemptedAt:   cloneTime(params.RunAttemptedAt),
		startedAt:        cloneTime(params.StartedAt),
		completedAt:      cloneTime(params.CompletedAt),
		failedAt:         cloneTime(params.FailedAt),
		failureReason:    params.FailureReason,
		currentProgress:  params.CurrentProgress.Copy(),
	}, nil
}
