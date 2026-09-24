package services

import (
	"context"
	"testing"
	"time"
	"uuid"

	"github.com/superman/pushkin/internal/application/ports"
	contracts "github.com/superman/pushkin/internal/contracts/kafka"
	"github.com/superman/pushkin/internal/domain"
)

func TestCampaignSchedulerServiceStartsDueCampaign(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	tenantID := uuid.NewV7()
	campaign := newScheduledBatchedCampaign(t, tenantID, now.Add(-time.Minute))
	repository := &schedulerCampaignRepository{
		candidateIDs: []domain.CampaignID{campaign.ID()},
		campaigns:    map[domain.CampaignID]*domain.Campaign{campaign.ID(): campaign},
	}
	pipeline := &schedulerKafkaPipeline{}
	service := NewCampaignSchedulerService(CampaignSchedulerServiceParams{
		CampaignRepository: repository,
		TransactionManager: schedulerTransactionManager{},
		KafkaProducer:      pipeline,
		StartingTimeout:    time.Minute,
	})

	result, err := service.ProcessDue(context.Background(), 10)
	if err != nil {
		t.Fatalf("process due campaigns: %v", err)
	}
	if result.PublishedRuns != 1 {
		t.Fatalf("expected one published run, got %d", result.PublishedRuns)
	}
	if result.BatchLimitReached {
		t.Fatal("batch limit must not be reached")
	}
	if campaign.Status() != domain.CampaignStatusStarting {
		t.Fatalf("expected campaign to start, got %q", campaign.Status())
	}
	publishedRun := schedulerPublishedRun(t, pipeline, contracts.TopicCampaignBatchedRun)
	if publishedRun.CampaignID != campaign.ID() || publishedRun.RunID == uuid.Nil() {
		t.Fatal("due campaign run was not published")
	}
}

func TestCampaignSchedulerServiceRoutesInlineCampaignToInlineRunTopic(t *testing.T) {
	t.Parallel()

	tenantID := uuid.NewV7()
	payload, err := domain.NewPushPayload("Title", "Body", "", nil)
	if err != nil {
		t.Fatalf("new push payload: %v", err)
	}
	campaign, err := domain.NewInlineCampaign(domain.NewInlineCampaignParams{
		TenantID:    tenantID,
		ChannelID:   uuid.NewV7(),
		PushPayload: payload,
		Priority:    domain.PriorityNormal,
		Recipients:  []domain.UserID{"user-1"},
	})
	if err != nil {
		t.Fatalf("new inline campaign: %v", err)
	}
	repository := &schedulerCampaignRepository{
		candidateIDs: []domain.CampaignID{campaign.ID()},
		campaigns:    map[domain.CampaignID]*domain.Campaign{campaign.ID(): campaign},
	}
	pipeline := &schedulerKafkaPipeline{}
	service := NewCampaignSchedulerService(CampaignSchedulerServiceParams{
		CampaignRepository: repository,
		TransactionManager: schedulerTransactionManager{},
		KafkaProducer:      pipeline,
		StartingTimeout:    time.Minute,
	})

	result, err := service.ProcessDue(context.Background(), 10)
	if err != nil {
		t.Fatalf("process inline campaign: %v", err)
	}
	if result.PublishedRuns != 1 {
		t.Fatalf("published runs = %d, want 1", result.PublishedRuns)
	}
	if publishedRun := schedulerPublishedRun(t, pipeline, contracts.TopicCampaignInlineRun); publishedRun.CampaignID != campaign.ID() {
		t.Fatalf("published campaign = %s, want %s", publishedRun.CampaignID, campaign.ID())
	}
}

func TestCampaignSchedulerServiceStartsDueCampaignsAsBatch(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	tenantID := uuid.NewV7()
	first := newScheduledBatchedCampaign(t, tenantID, now.Add(-time.Minute))
	second := newScheduledBatchedCampaign(t, tenantID, now.Add(-time.Minute))
	repository := &schedulerCampaignRepository{
		candidateIDs: []domain.CampaignID{first.ID(), second.ID()},
		campaigns: map[domain.CampaignID]*domain.Campaign{
			first.ID():  first,
			second.ID(): second,
		},
	}
	pipeline := &schedulerKafkaPipeline{}
	service := NewCampaignSchedulerService(CampaignSchedulerServiceParams{
		CampaignRepository: repository,
		TransactionManager: schedulerTransactionManager{},
		KafkaProducer:      pipeline,
		StartingTimeout:    time.Minute,
	})

	result, err := service.ProcessDue(context.Background(), 2)
	if err != nil {
		t.Fatalf("process due campaigns: %v", err)
	}
	if result.PublishedRuns != 2 {
		t.Fatalf("expected two published runs, got %d", result.PublishedRuns)
	}
	if !result.BatchLimitReached {
		t.Fatal("batch limit must be reached")
	}
	if len(repository.updated) != 2 {
		t.Fatalf("expected two batch updates, got %d", len(repository.updated))
	}
	if len(pipeline.transaction.messages) != 2 {
		t.Fatalf("expected two published messages, got %d", len(pipeline.transaction.messages))
	}
}

func TestCampaignSchedulerServiceStartsImmediateCampaignWithoutRunAttempt(t *testing.T) {
	t.Parallel()

	tenantID := uuid.NewV7()
	campaign := newDraftBatchedCampaign(t, tenantID)
	if err := campaign.RequestStart(); err != nil {
		t.Fatalf("request start: %v", err)
	}
	repository := &schedulerCampaignRepository{
		candidateIDs: []domain.CampaignID{campaign.ID()},
		campaigns:    map[domain.CampaignID]*domain.Campaign{campaign.ID(): campaign},
	}
	pipeline := &schedulerKafkaPipeline{}
	service := NewCampaignSchedulerService(CampaignSchedulerServiceParams{
		CampaignRepository: repository,
		TransactionManager: schedulerTransactionManager{},
		KafkaProducer:      pipeline,
		StartingTimeout:    time.Minute,
	})

	result, err := service.ProcessDue(context.Background(), 10)
	if err != nil {
		t.Fatalf("process immediate campaign: %v", err)
	}
	publishedRun := schedulerPublishedRun(t, pipeline, contracts.TopicCampaignBatchedRun)
	if result.PublishedRuns != 1 || publishedRun.RunID == uuid.Nil() {
		t.Fatalf("expected one published run, got result=%+v run_id=%s", result, publishedRun.RunID)
	}
	if campaign.RunAttemptedAt() == nil {
		t.Fatal("scheduler must create the run attempt lease")
	}
}

func TestCampaignSchedulerServiceFencesStaleStartingCampaign(t *testing.T) {
	t.Parallel()

	tenantID := uuid.NewV7()
	campaign := newDraftBatchedCampaign(t, tenantID)
	if err := campaign.RequestStart(); err != nil {
		t.Fatalf("request start: %v", err)
	}
	previousRunID, err := campaign.BeginRun(time.Now().UTC().Add(-time.Hour), time.Minute)
	if err != nil {
		t.Fatalf("begin previous run: %v", err)
	}
	repository := &schedulerCampaignRepository{
		candidateIDs: []domain.CampaignID{campaign.ID()},
		campaigns:    map[domain.CampaignID]*domain.Campaign{campaign.ID(): campaign},
	}
	pipeline := &schedulerKafkaPipeline{}
	service := NewCampaignSchedulerService(CampaignSchedulerServiceParams{
		CampaignRepository: repository,
		TransactionManager: schedulerTransactionManager{},
		KafkaProducer:      pipeline,
		StartingTimeout:    time.Minute,
	})

	result, err := service.ProcessDue(context.Background(), 10)
	if err != nil {
		t.Fatalf("recover stale campaign: %v", err)
	}
	if result.PublishedRuns != 1 {
		t.Fatalf("expected one published run, got %d", result.PublishedRuns)
	}
	publishedRun := schedulerPublishedRun(t, pipeline, contracts.TopicCampaignBatchedRun)
	if publishedRun.RunID == previousRunID || campaign.RunID() == nil || *campaign.RunID() != publishedRun.RunID {
		t.Fatal("stale campaign must receive and publish a new fencing run ID")
	}
}

type schedulerCampaignRepository struct {
	candidateIDs []domain.CampaignID
	campaigns    map[domain.CampaignID]*domain.Campaign
	updated      []*domain.Campaign
}

func (r *schedulerCampaignRepository) Create(context.Context, *domain.Campaign) error {
	return nil
}

func (r *schedulerCampaignRepository) FindByID(
	_ context.Context,
	id domain.CampaignID,
) (*domain.Campaign, error) {
	return r.campaigns[id], nil
}

func (r *schedulerCampaignRepository) FindByIDs(
	_ context.Context,
	ids []domain.CampaignID,
) (map[domain.CampaignID]*domain.Campaign, error) {
	result := make(map[domain.CampaignID]*domain.Campaign, len(ids))
	for _, id := range ids {
		result[id] = r.campaigns[id]
	}
	return result, nil
}

func (r *schedulerCampaignRepository) FindByIDsForUpdate(
	ctx context.Context,
	ids []domain.CampaignID,
) (map[domain.CampaignID]*domain.Campaign, error) {
	return r.FindByIDs(ctx, ids)
}

func (r *schedulerCampaignRepository) FindStartCandidatesForUpdate(
	_ context.Context,
	_ time.Time,
	_ time.Time,
	_ int,
) ([]*domain.Campaign, error) {
	campaigns := make([]*domain.Campaign, 0, len(r.candidateIDs))
	for _, id := range r.candidateIDs {
		campaigns = append(campaigns, r.campaigns[id])
	}
	return campaigns, nil
}

func (r *schedulerCampaignRepository) FindByIDForUpdate(
	_ context.Context,
	id domain.CampaignID,
) (*domain.Campaign, error) {
	return r.campaigns[id], nil
}

func (r *schedulerCampaignRepository) Update(_ context.Context, campaign *domain.Campaign) error {
	r.updated = append(r.updated, campaign)
	return nil
}

func (r *schedulerCampaignRepository) UpdateBatch(_ context.Context, campaigns []*domain.Campaign) error {
	r.updated = append(r.updated, campaigns...)
	return nil
}

func (r *schedulerCampaignRepository) Tx(ports.Transaction) ports.CampaignRepository {
	return r
}

type schedulerTransactionManager struct{}

func (schedulerTransactionManager) WithTx(_ context.Context, fn func(ports.Transaction) error) error {
	return fn(struct{}{})
}

type schedulerKafkaPipeline struct {
	kafkaPipelineControlFake
	transaction schedulerKafkaTransaction
}

func (l *schedulerKafkaPipeline) Produce(_ context.Context, messages []ports.OutboundKafkaMessage) error {
	l.transaction.messages = append([]ports.OutboundKafkaMessage(nil), messages...)
	return nil
}

type schedulerKafkaTransaction struct {
	messages []ports.OutboundKafkaMessage
}

func schedulerPublishedRun(
	t *testing.T,
	pipeline *schedulerKafkaPipeline,
	wantTopic contracts.Topic,
) contracts.CampaignRunRequestedV1 {
	t.Helper()
	if len(pipeline.transaction.messages) != 1 {
		t.Fatalf("expected one published message, got %d", len(pipeline.transaction.messages))
	}
	message := pipeline.transaction.messages[0]
	if message.Topic != wantTopic {
		t.Fatalf("campaign run topic = %q, want %q", message.Topic, wantTopic)
	}
	publishedRun, ok := message.Value.(contracts.CampaignRunRequestedV1)
	if !ok {
		t.Fatalf("expected CampaignRunRequestedV1, got %T", message.Value)
	}
	return publishedRun
}

func newScheduledBatchedCampaign(t *testing.T, tenantID domain.TenantID, scheduledAt time.Time) *domain.Campaign {
	t.Helper()
	campaign := newCampaign(t, tenantID, &scheduledAt)
	if err := campaign.Schedule(scheduledAt, scheduledAt.Add(-time.Minute)); err != nil {
		t.Fatalf("schedule campaign: %v", err)
	}
	return campaign
}

func newDraftBatchedCampaign(t *testing.T, tenantID domain.TenantID) *domain.Campaign {
	t.Helper()
	return newCampaign(t, tenantID, nil)
}

func newCampaign(t *testing.T, tenantID domain.TenantID, scheduledAt *time.Time) *domain.Campaign {
	t.Helper()
	payload, err := domain.NewPushPayload("Title", "Body", "", nil)
	if err != nil {
		t.Fatalf("new push payload: %v", err)
	}
	campaign, err := domain.NewBatchedCampaign(domain.NewBatchedCampaignParams{
		TenantID:    tenantID,
		ChannelID:   uuid.NewV7(),
		PushPayload: payload,
		Priority:    domain.PriorityNormal,
	})
	if err != nil {
		t.Fatalf("new campaign: %v", err)
	}
	return campaign
}
