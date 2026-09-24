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

func TestInlineCampaignRunCoordinatorProcessesBatch(t *testing.T) {
	t.Parallel()

	first, firstRun := newInlineCoordinatorCampaign(t, []domain.UserID{"user-1"})
	second, secondRun := newInlineCoordinatorCampaign(t, []domain.UserID{"user-2", "user-3"})
	pipeline := &inlineCampaignPipeline{records: []ports.KafkaRecord{
		{Value: contracts.CampaignRunRequestedV1{MessageHeaderV1: contracts.NewMessageHeaderV1(), CampaignID: first.ID(), RunID: firstRun}},
		{Value: contracts.CampaignRunRequestedV1{MessageHeaderV1: contracts.NewMessageHeaderV1(), CampaignID: second.ID(), RunID: secondRun}},
	}}
	repository := &inlineCampaignRepository{campaigns: map[domain.CampaignID]*domain.Campaign{first.ID(): first, second.ID(): second}}
	service := NewInlineCampaignRunCoordinatorService(InlineCampaignRunCoordinatorServiceParams{
		CampaignRepository: repository,
		TransactionManager: coordinatorTransactionManager{}, KafkaConsumer: pipeline,
		BatchSize: 100,
	})
	if err := service.Process(context.Background()); err != nil {
		t.Fatalf("process inline campaign runs: %v", err)
	}
	if pipeline.pollManyLimit != 100 {
		t.Fatalf("poll many limit = %d", pipeline.pollManyLimit)
	}
	if len(pipeline.messages) != 4 {
		t.Fatalf("outgoing message count = %d, want 4", len(pipeline.messages))
	}
	if len(repository.updatedBatch) != 2 {
		t.Fatalf("batch update campaign count = %d, want 2", len(repository.updatedBatch))
	}
	if repository.findByIDsForUpdateCalls != 1 {
		t.Fatalf("locked campaign batch lookup count = %d, want 1", repository.findByIDsForUpdateCalls)
	}
	if repository.findByIDForUpdateCalls != 0 {
		t.Fatalf("single locked campaign lookup count = %d, want 0", repository.findByIDForUpdateCalls)
	}
	for _, message := range pipeline.messages {
		if message.Topic != contracts.TopicCampaignProgress && message.Topic != contracts.TopicCampaignInlineFanout {
			t.Fatalf("unexpected topic: %q", message.Topic)
		}
	}
}

func TestInlineCampaignRunCoordinatorDeduplicatesCurrentRunWithinPoll(t *testing.T) {
	t.Parallel()

	campaign, runID := newInlineCoordinatorCampaign(t, []domain.UserID{"user-1"})
	pipeline := &inlineCampaignPipeline{records: []ports.KafkaRecord{
		{Value: contracts.CampaignRunRequestedV1{MessageHeaderV1: contracts.NewMessageHeaderV1(), CampaignID: campaign.ID(), RunID: runID}},
		{Value: contracts.CampaignRunRequestedV1{MessageHeaderV1: contracts.NewMessageHeaderV1(), CampaignID: campaign.ID(), RunID: runID}},
	}}
	service := NewInlineCampaignRunCoordinatorService(InlineCampaignRunCoordinatorServiceParams{
		CampaignRepository: &inlineCampaignRepository{campaigns: map[domain.CampaignID]*domain.Campaign{campaign.ID(): campaign}},
		TransactionManager: coordinatorTransactionManager{},
		KafkaConsumer:      pipeline,
		BatchSize:          100,
	})

	if err := service.Process(context.Background()); err != nil {
		t.Fatalf("process duplicate inline campaign runs: %v", err)
	}
	if len(pipeline.messages) != 2 {
		t.Fatalf("outgoing message count = %d, want 2", len(pipeline.messages))
	}
}

func TestInlineCampaignFanoutProcessesBatch(t *testing.T) {
	t.Parallel()

	first, _ := newInlineCoordinatorCampaign(t, []domain.UserID{"user-1"})
	second, _ := newInlineCoordinatorCampaign(t, []domain.UserID{"user-2"})
	installations := []domain.PushInstallationID{uuid.NewV7(), uuid.NewV7(), uuid.NewV7()}
	pipeline := &inlineCampaignPipeline{records: []ports.KafkaRecord{
		{Value: inlineFanoutMessage(first)},
		{Value: inlineFanoutMessage(second)},
	}}
	campaigns := &inlineCampaignRepository{campaigns: map[domain.CampaignID]*domain.Campaign{first.ID(): first, second.ID(): second}}
	applications := &mobileApplicationRepositoryFake{mobileApplicationIDs: []domain.MobileApplicationID{uuid.NewV7()}}
	pushInstallations := &pushInstallationRepositoryFake{installationIDs: installations}
	service := NewInlineCampaignFanoutService(InlineCampaignFanoutServiceParams{
		CampaignRepository:          campaigns,
		MobileApplicationRepository: applications,
		PushInstallationRepository:  pushInstallations,
		KafkaConsumer:               pipeline,
		BatchSize:                   100,
	})
	if err := service.Process(context.Background()); err != nil {
		t.Fatalf("process inline campaign fanout: %v", err)
	}
	if pipeline.pollManyLimit != 100 {
		t.Fatalf("poll many limit = %d", pipeline.pollManyLimit)
	}
	if len(pipeline.messages) != len(installations)*2+2 {
		t.Fatalf("outgoing message count = %d", len(pipeline.messages))
	}
	if campaigns.findByIDsCalls != 1 {
		t.Fatalf("campaign batch lookup count = %d, want 1", campaigns.findByIDsCalls)
	}
	if applications.listIDsByChannelsCalls != 1 {
		t.Fatalf("channel application batch lookup count = %d, want 1", applications.listIDsByChannelsCalls)
	}
	if pushInstallations.listActiveMatchesCalls != 2 {
		t.Fatalf("installation lookup count = %d, want one per channel", pushInstallations.listActiveMatchesCalls)
	}
}

func inlineFanoutMessage(campaign *domain.Campaign) contracts.InlineCampaignFanoutV1 {
	return contracts.InlineCampaignFanoutV1{MessageHeaderV1: contracts.NewMessageHeaderV1(), CampaignID: campaign.ID(), Recipients: userIDsToStrings(campaign.InlineRecipients())}
}

func newInlineCoordinatorCampaign(t *testing.T, recipients []domain.UserID) (*domain.Campaign, domain.RunID) {
	t.Helper()
	payload, err := domain.NewPushPayload("Title", "Body", "", nil)
	if err != nil {
		t.Fatalf("new payload: %v", err)
	}
	campaign, err := domain.NewInlineCampaign(domain.NewInlineCampaignParams{TenantID: uuid.NewV7(), ChannelID: uuid.NewV7(), PushPayload: payload, Priority: domain.PriorityNormal, Recipients: recipients})
	if err != nil {
		t.Fatalf("new inline campaign: %v", err)
	}
	runID, err := campaign.BeginRun(time.Now().UTC(), time.Minute)
	if err != nil {
		t.Fatalf("begin run: %v", err)
	}
	return campaign, runID
}

type inlineCampaignRepository struct {
	campaigns               map[domain.CampaignID]*domain.Campaign
	updatedBatch            []*domain.Campaign
	findByIDsCalls          int
	findByIDsForUpdateCalls int
	findByIDForUpdateCalls  int
}

func (r *inlineCampaignRepository) Create(context.Context, *domain.Campaign) error { return nil }
func (r *inlineCampaignRepository) FindByID(_ context.Context, id domain.CampaignID) (*domain.Campaign, error) {
	return r.campaigns[id], nil
}
func (r *inlineCampaignRepository) FindByIDs(context.Context, []domain.CampaignID) (map[domain.CampaignID]*domain.Campaign, error) {
	r.findByIDsCalls++
	return r.campaigns, nil
}
func (r *inlineCampaignRepository) FindByIDsForUpdate(context.Context, []domain.CampaignID) (map[domain.CampaignID]*domain.Campaign, error) {
	r.findByIDsForUpdateCalls++
	return r.campaigns, nil
}
func (r *inlineCampaignRepository) FindStartCandidatesForUpdate(context.Context, time.Time, time.Time, int) ([]*domain.Campaign, error) {
	return nil, nil
}
func (r *inlineCampaignRepository) FindByIDForUpdate(_ context.Context, id domain.CampaignID) (*domain.Campaign, error) {
	r.findByIDForUpdateCalls++
	return r.campaigns[id], nil
}
func (r *inlineCampaignRepository) Update(context.Context, *domain.Campaign) error { return nil }
func (r *inlineCampaignRepository) UpdateBatch(_ context.Context, campaigns []*domain.Campaign) error {
	r.updatedBatch = append(r.updatedBatch, campaigns...)
	return nil
}
func (r *inlineCampaignRepository) Tx(ports.Transaction) ports.CampaignRepository { return r }

type inlineCampaignPipeline struct {
	kafkaPipelineControlFake
	records       []ports.KafkaRecord
	pollManyLimit int
	messages      []ports.OutboundKafkaMessage
}

func (p *inlineCampaignPipeline) Poll(context.Context) (ports.KafkaRecord, bool, error) {
	return ports.KafkaRecord{}, false, nil
}
func (p *inlineCampaignPipeline) PollMany(_ context.Context, limit int) ([]ports.KafkaRecord, error) {
	p.pollManyLimit = limit
	return p.records, nil
}
func (p *inlineCampaignPipeline) Complete(_ context.Context, messages []ports.OutboundKafkaMessage) error {
	p.messages = append([]ports.OutboundKafkaMessage(nil), messages...)
	return nil
}
