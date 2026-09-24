package domain

import (
	"errors"
	"testing"
	"time"
	"uuid"
)

func TestCampaignImmediateRunLifecycle(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.September, 5, 10, 0, 0, 0, time.UTC)
	campaign := newTestCampaign(t, nil, now)

	if !campaign.CanAddRecipients() {
		t.Fatal("new campaign must accept recipients")
	}
	if err := campaign.RequestStart(); err != nil {
		t.Fatalf("request start: %v", err)
	}
	runID, err := campaign.BeginRun(now.Add(time.Minute), time.Minute)
	if err != nil {
		t.Fatalf("begin run: %v", err)
	}
	if campaign.CanAddRecipients() {
		t.Fatal("starting campaign must reject recipients")
	}

	changed, err := campaign.MarkStarted(runID, now.Add(2*time.Minute))
	if err != nil || !changed {
		t.Fatalf("mark started: changed=%v err=%v", changed, err)
	}

	changed, err = campaign.MarkStarted(runID, now.Add(3*time.Minute))
	if err != nil || changed {
		t.Fatalf("replayed mark started: changed=%v err=%v", changed, err)
	}

	if err := campaign.Complete(now.Add(4 * time.Minute)); err != nil {
		t.Fatalf("complete: %v", err)
	}
	if campaign.Status() != CampaignStatusCompleted {
		t.Fatalf("unexpected status: %s", campaign.Status())
	}
}

func TestInlineCampaignKeepsBoundedImmutableRecipients(t *testing.T) {
	t.Parallel()

	payload, err := NewPushPayload("Title", "Body", "", nil)
	if err != nil {
		t.Fatalf("new payload: %v", err)
	}
	recipients := []UserID{"user-1", "user-2"}
	campaign, err := NewInlineCampaign(NewInlineCampaignParams{
		TenantID: uuid.NewV7(), ChannelID: uuid.NewV7(), PushPayload: payload, Priority: PriorityNormal,
		Recipients: recipients,
	})
	if err != nil {
		t.Fatalf("new inline campaign: %v", err)
	}
	recipients[0] = "changed"
	if campaign.RecipientMode() != CampaignRecipientModeInline || campaign.CanAddRecipients() {
		t.Fatalf("unexpected inline campaign state: mode=%q accepts=%v", campaign.RecipientMode(), campaign.CanAddRecipients())
	}
	if campaign.Status() != CampaignStatusStarting {
		t.Fatalf("immediate inline campaign status = %q, want starting", campaign.Status())
	}
	if got := campaign.InlineRecipients(); len(got) != 2 || got[0] != "user-1" {
		t.Fatalf("unexpected recipients: %v", got)
	}
	if _, err := NewInlineCampaign(NewInlineCampaignParams{
		TenantID: uuid.NewV7(), ChannelID: uuid.NewV7(), PushPayload: payload, Priority: PriorityNormal,
		Recipients: make([]UserID, MaxInlineCampaignRecipients+1),
	}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("expected invalid recipient count, got %v", err)
	}
}

func TestCampaignScheduledRunCannotBeginEarly(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.September, 5, 10, 0, 0, 0, time.UTC)
	scheduledAt := now.Add(time.Hour)
	campaign := newTestCampaign(t, nil, now)

	if err := campaign.Schedule(scheduledAt, now); err != nil {
		t.Fatalf("schedule: %v", err)
	}
	if campaign.CanBeginRun(now.Add(30*time.Minute), time.Minute) {
		t.Fatal("early campaign must not be eligible")
	}
	if !campaign.CanBeginRun(scheduledAt, time.Minute) {
		t.Fatal("due campaign must be eligible")
	}
	if _, err := campaign.BeginRun(scheduledAt, time.Minute); err != nil {
		t.Fatalf("begin due run: %v", err)
	}
}

func TestCampaignRecoveryFencesPreviousRun(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.September, 5, 10, 0, 0, 0, time.UTC)
	campaign := newTestCampaign(t, nil, now)
	if err := campaign.RequestStart(); err != nil {
		t.Fatalf("request start: %v", err)
	}
	firstRunID, err := campaign.BeginRun(now.Add(time.Minute), time.Minute)
	if err != nil {
		t.Fatalf("begin run: %v", err)
	}
	secondRunID, err := campaign.BeginRun(now.Add(2*time.Minute), time.Minute)
	if err != nil {
		t.Fatalf("recover run: %v", err)
	}
	if firstRunID == secondRunID {
		t.Fatal("recovery must create a new run_id")
	}

	if _, err := campaign.MarkStarted(firstRunID, now.Add(3*time.Minute)); !errors.Is(err, ErrRunMismatch) {
		t.Fatalf("expected run mismatch, got %v", err)
	}
	changed, err := campaign.MarkStarted(secondRunID, now.Add(3*time.Minute))
	if err != nil || !changed {
		t.Fatalf("mark current run started: changed=%v err=%v", changed, err)
	}
}

func TestCampaignRejectsInvalidTransitions(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.September, 5, 10, 0, 0, 0, time.UTC)
	campaign := newTestCampaign(t, nil, now)

	if err := campaign.Complete(now); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("expected invalid transition, got %v", err)
	}
	if err := campaign.RequestStart(); err != nil {
		t.Fatalf("request start: %v", err)
	}
	runID, err := campaign.BeginRun(now, time.Minute)
	if err != nil {
		t.Fatalf("begin run: %v", err)
	}
	if _, err := campaign.MarkStarted(runID, now.Add(time.Minute)); err != nil {
		t.Fatalf("mark started: %v", err)
	}
	if campaign.CanBeginRun(now.Add(2*time.Minute), time.Minute) {
		t.Fatal("started campaign must not be eligible")
	}
}

func TestCampaignAppliesCurrentProgressAndCompletes(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.September, 5, 10, 0, 0, 0, time.UTC)
	campaign := newTestCampaign(t, nil, now)
	if err := campaign.RequestStart(); err != nil {
		t.Fatalf("request start: %v", err)
	}
	runID, err := campaign.BeginRun(now, time.Minute)
	if err != nil {
		t.Fatalf("begin run: %v", err)
	}
	if _, err := campaign.MarkStarted(runID, now.Add(time.Minute)); err != nil {
		t.Fatalf("mark started: %v", err)
	}

	progress := NewEmptyCampaignProgress(1)
	if err := progress.RecordSourceBatchFanout(2); err != nil {
		t.Fatalf("record fanout: %v", err)
	}
	if err := progress.RecordDeliveryResults(1, 1); err != nil {
		t.Fatalf("record delivery results: %v", err)
	}
	if err := campaign.ApplyCurrentProgress(progress, now.Add(2*time.Minute)); err != nil {
		t.Fatalf("apply current progress: %v", err)
	}
	if campaign.Status() != CampaignStatusCompleted {
		t.Fatalf("unexpected campaign status: %s", campaign.Status())
	}
	current := campaign.CurrentProgress()
	if current == nil || current.DeliveryAcceptedCount() != 1 || !current.IsComplete() {
		t.Fatalf("unexpected current progress: %+v", current)
	}
	if err := campaign.ApplyCurrentProgress(progress, now.Add(3*time.Minute)); err != nil {
		t.Fatalf("replay completed progress: %v", err)
	}
	if err := campaign.ApplyCurrentProgress(NewEmptyCampaignProgress(0), now.Add(3*time.Minute)); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("apply different completed progress error = %v, want invalid transition", err)
	}
}

func TestNewBatchedCampaignRejectsZeroPayload(t *testing.T) {
	t.Parallel()

	_, err := NewBatchedCampaign(NewBatchedCampaignParams{
		TenantID:  uuid.NewV7(),
		ChannelID: uuid.NewV7(),
		Priority:  PriorityNormal,
	})
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("expected invalid argument, got %v", err)
	}
}

func TestHydrateCampaignRejectsScheduledCampaignWithoutStartTime(t *testing.T) {
	t.Parallel()

	payload, err := NewPushPayload("title", "", "", nil)
	if err != nil {
		t.Fatalf("new push payload: %v", err)
	}
	_, err = HydrateCampaign(HydrateCampaignParams{
		ID:          uuid.NewV7(),
		TenantID:    uuid.NewV7(),
		ChannelID:   uuid.NewV7(),
		PushPayload: payload,
		Priority:    PriorityNormal,
		Status:      CampaignStatusScheduled,
	})
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("expected invalid argument, got %v", err)
	}
}

func TestCampaignStatusIsTerminal(t *testing.T) {
	t.Parallel()

	tests := []struct {
		status CampaignStatus
		want   bool
	}{
		{status: CampaignStatusDraft},
		{status: CampaignStatusScheduled},
		{status: CampaignStatusStarting},
		{status: CampaignStatusStarted},
		{status: CampaignStatusCompleted, want: true},
		{status: CampaignStatusFailed, want: true},
	}
	for _, test := range tests {
		if got := test.status.IsTerminal(); got != test.want {
			t.Fatalf("status %q terminal = %t, want %t", test.status, got, test.want)
		}
	}
}

func newTestCampaign(t *testing.T, scheduledAt *time.Time, now time.Time) *Campaign {
	t.Helper()
	payload, err := NewPushPayload("title", "body", "", map[string]string{"kind": "test"})
	if err != nil {
		t.Fatalf("new payload: %v", err)
	}
	campaign, err := NewBatchedCampaign(NewBatchedCampaignParams{
		TenantID:    uuid.NewV7(),
		ChannelID:   uuid.NewV7(),
		PushPayload: payload,
		Priority:    PriorityNormal,
	})
	if err != nil {
		t.Fatalf("new campaign: %v", err)
	}
	if campaign.ID() == uuid.Nil() {
		t.Fatal("new campaign must generate an ID")
	}
	return campaign
}
