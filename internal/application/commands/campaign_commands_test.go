package commands

import (
	"context"
	"errors"
	"testing"
	"time"

	"uuid"

	"github.com/superman/pushkin/internal/application"
	"github.com/superman/pushkin/internal/application/ports"
	contracts "github.com/superman/pushkin/internal/contracts/kafka"
	"github.com/superman/pushkin/internal/domain"
)

func TestCampaignCommandsCreateBatchedCampaign(t *testing.T) {
	t.Parallel()

	tenantID := uuid.NewV7()
	channel := newTestChannel(t, tenantID)
	channels := &fakeChannelRepository{channel: channel}
	campaigns := &fakeCampaignRepository{}
	commands := NewCampaignCommands(CampaignCommandsParams{
		ChannelRepository:  channels,
		CampaignRepository: campaigns,
	})

	result, err := commands.CreateCampaign(context.Background(), application.CreateCampaignCommand{
		TenantID:   tenantID,
		ChannelKey: channel.Key(),
		Title:      "Balance",
		Body:       "Please top up",
		Data:       map[string]string{"kind": "balance_reminder"},
		Priority:   domain.PriorityHigh,
	})
	if err != nil {
		t.Fatalf("create campaign: %v", err)
	}
	if result.CampaignID == uuid.Nil() {
		t.Fatal("campaign ID must be generated")
	}
	if channels.tenantID != tenantID || channels.key != channel.Key() {
		t.Fatalf("unexpected channel lookup: tenant=%s key=%q", channels.tenantID, channels.key)
	}
	if campaigns.created == nil {
		t.Fatal("campaign was not persisted")
	}
	if campaigns.created.ID() != result.CampaignID {
		t.Fatal("result must contain persisted campaign ID")
	}
	if campaigns.created.ChannelID() != channel.ID() {
		t.Fatal("campaign must use resolved channel")
	}
	if got := campaigns.created.PushPayload().Data()["kind"]; got != "balance_reminder" {
		t.Fatalf("unexpected payload data: %q", got)
	}
}

func TestCampaignCommandsCreateBatchedCampaignRejectsDisabledChannel(t *testing.T) {
	t.Parallel()

	tenantID := uuid.NewV7()
	channel := newTestChannel(t, tenantID)
	if err := channel.Disable(); err != nil {
		t.Fatalf("disable channel: %v", err)
	}
	campaigns := &fakeCampaignRepository{}
	commands := NewCampaignCommands(CampaignCommandsParams{
		ChannelRepository:  &fakeChannelRepository{channel: channel},
		CampaignRepository: campaigns,
	})

	_, err := commands.CreateCampaign(context.Background(), application.CreateCampaignCommand{
		TenantID:   tenantID,
		ChannelKey: channel.Key(),
		Title:      "Title",
		Priority:   domain.PriorityNormal,
	})
	if !errors.Is(err, application.ErrConflict) {
		t.Fatalf("expected conflict, got %v", err)
	}
	if campaigns.created != nil {
		t.Fatal("disabled channel must not create a campaign")
	}
}

func TestCampaignCommandsCreateInlineCampaignSchedulesRecipients(t *testing.T) {
	t.Parallel()

	tenantID := uuid.NewV7()
	channel := newTestChannel(t, tenantID)
	campaigns := &fakeCampaignRepository{}
	transactions := &fakeTransactionManager{}
	commands := NewCampaignCommands(CampaignCommandsParams{
		ChannelRepository:  &fakeChannelRepository{channel: channel},
		CampaignRepository: campaigns,
		TransactionManager: transactions,
	})
	scheduledAt := time.Now().UTC().Add(time.Minute)
	result, err := commands.CreateInlineCampaign(context.Background(), application.CreateInlineCampaignCommand{
		TenantID: tenantID, ChannelKey: channel.Key(), Title: "Title", Body: "Body", Priority: domain.PriorityHigh,
		UserIDs: []domain.UserID{"user-1", "user-2"}, ScheduledAt: &scheduledAt,
	})
	if err != nil {
		t.Fatalf("create inline campaign: %v", err)
	}
	if campaigns.created == nil || result.Status != domain.CampaignStatusScheduled {
		t.Fatalf("unexpected result=%+v campaign=%v", result, campaigns.created)
	}
	if campaigns.created.RecipientMode() != domain.CampaignRecipientModeInline || len(campaigns.created.InlineRecipients()) != 2 {
		t.Fatalf("unexpected inline campaign: %+v", campaigns.created)
	}
	if transactions.called {
		t.Fatal("inline campaign creation must not open a transaction for one insert")
	}
}

func TestCampaignCommandsCreateInlineCampaignStartsImmediately(t *testing.T) {
	t.Parallel()

	tenantID := uuid.NewV7()
	channel := newTestChannel(t, tenantID)

	for _, scheduledAt := range []*time.Time{nil, timePointer(time.Now().UTC().Add(-time.Second))} {
		campaigns := &fakeCampaignRepository{}
		producer := &fakeKafkaProducer{}
		commands := NewCampaignCommands(CampaignCommandsParams{
			ChannelRepository:       &fakeChannelRepository{channel: channel},
			CampaignRepository:      campaigns,
			KafkaProducer:           producer,
			CampaignStartingTimeout: time.Minute,
		})

		result, err := commands.CreateInlineCampaign(context.Background(), application.CreateInlineCampaignCommand{
			TenantID: tenantID, ChannelKey: channel.Key(), Title: "Title", Priority: domain.PriorityNormal,
			UserIDs: []domain.UserID{"user-1"}, ScheduledAt: scheduledAt,
		})
		if err != nil {
			t.Fatalf("create immediate inline campaign: %v", err)
		}
		if result.Status != domain.CampaignStatusStarting || campaigns.created.RunID() == nil || campaigns.created.RunAttemptedAt() == nil {
			t.Fatalf("immediate campaign state: result=%+v campaign=%+v", result, campaigns.created)
		}
		if len(producer.messages) != 1 || producer.messages[0].Topic != contracts.TopicCampaignInlineRun {
			t.Fatalf("unexpected produced messages: %+v", producer.messages)
		}
		run, ok := producer.messages[0].Value.(contracts.CampaignRunRequestedV1)
		if !ok || run.CampaignID != campaigns.created.ID() || campaigns.created.RunID() == nil || run.RunID != *campaigns.created.RunID() {
			t.Fatalf("unexpected run message: %+v", producer.messages[0].Value)
		}
	}
}

func TestCampaignCommandsCreateInlineCampaignPersistsRecoveryStateWhenPublishFails(t *testing.T) {
	t.Parallel()

	tenantID := uuid.NewV7()
	channel := newTestChannel(t, tenantID)
	campaigns := &fakeCampaignRepository{}
	produceErr := errors.New("Kafka is unavailable")
	commands := NewCampaignCommands(CampaignCommandsParams{
		ChannelRepository:       &fakeChannelRepository{channel: channel},
		CampaignRepository:      campaigns,
		KafkaProducer:           &fakeKafkaProducer{err: produceErr},
		CampaignStartingTimeout: time.Minute,
	})

	_, err := commands.CreateInlineCampaign(context.Background(), application.CreateInlineCampaignCommand{
		TenantID: tenantID, ChannelKey: channel.Key(), Title: "Title", Priority: domain.PriorityNormal,
		UserIDs: []domain.UserID{"user-1"},
	})
	if !errors.Is(err, produceErr) {
		t.Fatalf("expected producer error, got %v", err)
	}
	if campaigns.created == nil || campaigns.created.Status() != domain.CampaignStatusStarting || campaigns.created.RunID() == nil || campaigns.created.RunAttemptedAt() == nil {
		t.Fatalf("campaign must retain scheduler recovery state: %+v", campaigns.created)
	}
}

func TestCampaignCommandsAddCampaignRecipients(t *testing.T) {
	t.Parallel()

	tenantID := uuid.NewV7()
	campaign := newTestCampaign(t, tenantID)
	campaigns := &fakeCampaignRepository{campaign: campaign}
	batches := &fakeSourceBatchRepository{}
	transactions := &fakeTransactionManager{}
	commands := NewCampaignCommands(CampaignCommandsParams{
		CampaignRepository:    campaigns,
		SourceBatchRepository: batches,
		TransactionManager:    transactions,
		SourceBatchMaxSize:    2,
	})

	result, err := commands.AddCampaignRecipients(context.Background(), application.AddCampaignRecipientsCommand{
		TenantID:   tenantID,
		CampaignID: campaign.ID(),
		UserIDs:    []domain.UserID{"user-1", "user-2"},
	})
	if err != nil {
		t.Fatalf("add campaign recipients: %v", err)
	}
	if result.SourceBatchID == uuid.Nil() {
		t.Fatal("source batch ID must be generated")
	}
	if !transactions.called {
		t.Fatal("recipient import must run in a transaction")
	}
	if campaigns.lockedCampaignID != campaign.ID() {
		t.Fatal("campaign must be loaded with a lock")
	}
	if batches.created == nil {
		t.Fatal("source batch was not persisted")
	}
	if batches.created.ID() != result.SourceBatchID {
		t.Fatal("result must contain persisted source batch ID")
	}
	if batches.created.CampaignID() != campaign.ID() || batches.created.Count() != 2 {
		t.Fatal("unexpected persisted source batch")
	}
}

func TestCampaignCommandsAddCampaignRecipientsRejectsBatchOverLimit(t *testing.T) {
	t.Parallel()

	tenantID := uuid.NewV7()
	transactions := &fakeTransactionManager{}
	commands := NewCampaignCommands(CampaignCommandsParams{
		CampaignRepository:    &fakeCampaignRepository{},
		SourceBatchRepository: &fakeSourceBatchRepository{},
		TransactionManager:    transactions,
		SourceBatchMaxSize:    2,
	})

	_, err := commands.AddCampaignRecipients(context.Background(), application.AddCampaignRecipientsCommand{
		TenantID:   tenantID,
		CampaignID: uuid.NewV7(),
		UserIDs:    []domain.UserID{"user-1", "user-2", "user-3"},
	})
	if !errors.Is(err, application.ErrValidation) {
		t.Fatalf("expected invalid argument, got %v", err)
	}
	if transactions.called {
		t.Fatal("oversized batch must be rejected before opening a transaction")
	}
}

func TestCampaignCommandsStartCampaignRequestsImmediateRun(t *testing.T) {
	t.Parallel()

	tenantID := uuid.NewV7()
	campaign := newTestCampaign(t, tenantID)
	campaigns := &fakeCampaignRepository{campaign: campaign}
	commands := NewCampaignCommands(CampaignCommandsParams{
		CampaignRepository: campaigns,
		TransactionManager: &fakeTransactionManager{},
	})

	result, err := commands.StartCampaign(context.Background(), application.StartCampaignCommand{
		TenantID:   tenantID,
		CampaignID: campaign.ID(),
	})
	if err != nil {
		t.Fatalf("start campaign: %v", err)
	}
	if result.CampaignID != campaign.ID() || result.Status != domain.CampaignStatusStarting {
		t.Fatalf("unexpected start result: %+v", result)
	}
	if campaigns.updated != campaign {
		t.Fatal("starting campaign must be persisted")
	}
	if campaign.RunID() != nil || campaign.RunAttemptedAt() != nil {
		t.Fatal("start command must leave run creation to scheduler")
	}
}

func TestCampaignCommandsStartCampaignSchedulesFutureCampaign(t *testing.T) {
	t.Parallel()

	tenantID := uuid.NewV7()
	scheduledAt := time.Now().UTC().Add(time.Hour)
	campaign := newTestCampaign(t, tenantID)
	campaigns := &fakeCampaignRepository{campaign: campaign}
	commands := NewCampaignCommands(CampaignCommandsParams{
		CampaignRepository: campaigns,
		TransactionManager: &fakeTransactionManager{},
	})

	result, err := commands.StartCampaign(context.Background(), application.StartCampaignCommand{
		TenantID:    tenantID,
		CampaignID:  campaign.ID(),
		ScheduledAt: &scheduledAt,
	})
	if err != nil {
		t.Fatalf("start scheduled campaign: %v", err)
	}
	if result.Status != domain.CampaignStatusScheduled {
		t.Fatalf("expected scheduled status, got %q", result.Status)
	}
	if campaigns.updated != campaign {
		t.Fatal("scheduled campaign must be persisted")
	}
	if got := campaign.ScheduledAt(); got == nil || !got.Equal(scheduledAt) {
		t.Fatalf("unexpected scheduled_at: %v", got)
	}
}

type fakeChannelRepository struct {
	channel  *domain.Channel
	err      error
	tenantID domain.TenantID
	key      string
}

func (r *fakeChannelRepository) FindByKey(
	_ context.Context,
	tenantID domain.TenantID,
	key string,
) (*domain.Channel, error) {
	r.tenantID = tenantID
	r.key = key
	if r.err != nil {
		return nil, r.err
	}
	return r.channel, nil
}

func (r *fakeChannelRepository) ListActiveIDs(context.Context) ([]domain.ChannelID, error) {
	if r.err != nil {
		return nil, r.err
	}
	if r.channel == nil {
		return nil, nil
	}
	return []domain.ChannelID{r.channel.ID()}, nil
}

func (r *fakeChannelRepository) FindProvisioningForUpdate(context.Context) (*domain.Channel, error) {
	if r.err != nil {
		return nil, r.err
	}
	if r.channel == nil {
		return nil, ports.ErrNotFound
	}
	return r.channel, nil
}

func (r *fakeChannelRepository) Create(_ context.Context, channel *domain.Channel) error {
	r.channel = channel
	return r.err
}

func (r *fakeChannelRepository) FindByID(_ context.Context, _ domain.ChannelID) (*domain.Channel, error) {
	if r.err != nil {
		return nil, r.err
	}
	return r.channel, nil
}

func (r *fakeChannelRepository) Update(_ context.Context, channel *domain.Channel) error {
	r.channel = channel
	return r.err
}

func (r *fakeChannelRepository) Tx(ports.Transaction) ports.ChannelRepository { return r }

func (r *fakeChannelRepository) LinkMobileApplication(
	_ context.Context,
	_ domain.ChannelID,
	_ domain.MobileApplicationID,
) error {
	return r.err
}

func (r *fakeChannelRepository) UnlinkMobileApplication(
	_ context.Context,
	_ domain.ChannelID,
	_ domain.MobileApplicationID,
) error {
	return r.err
}

type fakeCampaignRepository struct {
	created          *domain.Campaign
	campaign         *domain.Campaign
	updated          *domain.Campaign
	err              error
	candidateIDs     []domain.CampaignID
	lockedCampaignID domain.CampaignID
	tx               ports.Transaction
}

func (r *fakeCampaignRepository) Create(_ context.Context, campaign *domain.Campaign) error {
	if r.err != nil {
		return r.err
	}
	r.created = campaign
	return nil
}

func (r *fakeCampaignRepository) FindByID(
	_ context.Context,
	_ domain.CampaignID,
) (*domain.Campaign, error) {
	if r.err != nil {
		return nil, r.err
	}
	return r.campaign, nil
}

func (r *fakeCampaignRepository) FindByIDs(
	_ context.Context,
	_ []domain.CampaignID,
) (map[domain.CampaignID]*domain.Campaign, error) {
	if r.err != nil {
		return nil, r.err
	}
	return map[domain.CampaignID]*domain.Campaign{r.campaign.ID(): r.campaign}, nil
}

func (r *fakeCampaignRepository) FindByIDsForUpdate(
	ctx context.Context,
	ids []domain.CampaignID,
) (map[domain.CampaignID]*domain.Campaign, error) {
	return r.FindByIDs(ctx, ids)
}

func (r *fakeCampaignRepository) FindStartCandidatesForUpdate(
	_ context.Context,
	_ time.Time,
	_ time.Time,
	_ int,
) ([]*domain.Campaign, error) {
	if r.err != nil {
		return nil, r.err
	}
	if len(r.candidateIDs) == 0 {
		return nil, nil
	}
	return []*domain.Campaign{r.campaign}, nil
}

func (r *fakeCampaignRepository) FindByIDForUpdate(
	_ context.Context,
	id domain.CampaignID,
) (*domain.Campaign, error) {
	r.lockedCampaignID = id
	if r.err != nil {
		return nil, r.err
	}
	return r.campaign, nil
}

func (r *fakeCampaignRepository) Update(_ context.Context, campaign *domain.Campaign) error {
	if r.err != nil {
		return r.err
	}
	r.updated = campaign
	return nil
}

func (r *fakeCampaignRepository) UpdateBatch(ctx context.Context, campaigns []*domain.Campaign) error {
	for _, campaign := range campaigns {
		if err := r.Update(ctx, campaign); err != nil {
			return err
		}
	}
	return nil
}

func (r *fakeCampaignRepository) Tx(tx ports.Transaction) ports.CampaignRepository {
	r.tx = tx
	return r
}

type fakeSourceBatchRepository struct {
	created *domain.SourceBatch
	err     error
	tx      ports.Transaction
}

func (r *fakeSourceBatchRepository) Create(_ context.Context, batch *domain.SourceBatch) error {
	if r.err != nil {
		return r.err
	}
	r.created = batch
	return nil
}

func (r *fakeSourceBatchRepository) FindByID(
	_ context.Context,
	_ domain.SourceBatchID,
) (*domain.SourceBatch, error) {
	if r.err != nil {
		return nil, r.err
	}
	return r.created, nil
}

func (r *fakeSourceBatchRepository) ListIDsByCampaign(
	_ context.Context,
	_ domain.CampaignID,
) ([]domain.SourceBatchID, error) {
	return nil, r.err
}

func (r *fakeSourceBatchRepository) Tx(tx ports.Transaction) ports.SourceBatchRepository {
	r.tx = tx
	return r
}

func timePointer(value time.Time) *time.Time { return &value }

type fakeKafkaProducer struct {
	messages []ports.OutboundKafkaMessage
	err      error
}

func (p *fakeKafkaProducer) Produce(_ context.Context, messages []ports.OutboundKafkaMessage) error {
	if p.err != nil {
		return p.err
	}
	p.messages = append(p.messages, messages...)
	return nil
}

type fakeTransactionManager struct {
	called bool
	err    error
}

func (m *fakeTransactionManager) WithTx(_ context.Context, fn func(tx ports.Transaction) error) error {
	m.called = true
	if m.err != nil {
		return m.err
	}
	return fn(struct{}{})
}

func newTestChannel(t *testing.T, tenantID domain.TenantID) *domain.Channel {
	t.Helper()
	provider, err := domain.NewProvider(domain.NewProviderParams{
		TenantID:             tenantID,
		Type:                 domain.ProviderTypeFCM,
		EncryptedCredentials: "encrypted-credentials",
		RateLimitQPS:         100,
		RateLimitBurst:       100,
	})
	if err != nil {
		t.Fatalf("new provider: %v", err)
	}
	channel, err := domain.NewChannel(domain.NewChannelParams{
		TenantID: tenantID,
		Provider: provider,
		Type:     domain.ChannelTypeMobilePush,
		Key:      "customer-app",
	})
	if err != nil {
		t.Fatalf("new channel: %v", err)
	}
	if err := channel.Activate(); err != nil {
		t.Fatalf("activate channel: %v", err)
	}
	return channel
}

func newTestCampaign(t *testing.T, tenantID domain.TenantID) *domain.Campaign {
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
