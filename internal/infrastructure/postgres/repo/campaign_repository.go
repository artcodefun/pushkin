package repo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/superman/pushkin/internal/application/ports"
	"github.com/superman/pushkin/internal/domain"
	"github.com/superman/pushkin/internal/infrastructure/postgres/gen"
	"github.com/superman/pushkin/internal/infrastructure/postgres/repo/mappers"
)

var _ ports.CampaignRepository = (*CampaignRepository)(nil)

type CampaignRepository struct {
	queries *gen.Queries
}

func NewCampaignRepository(queries *gen.Queries) *CampaignRepository {
	return &CampaignRepository{queries: queries}
}

func (r *CampaignRepository) Create(ctx context.Context, campaign *domain.Campaign) error {
	payload := campaign.PushPayload()
	data, err := json.Marshal(payload.Data())
	if err != nil {
		return err
	}
	inlineRecipients := []byte("[]")
	if recipients := campaign.InlineRecipients(); recipients != nil {
		inlineRecipients, err = json.Marshal(recipients)
		if err != nil {
			return err
		}
	}
	return r.queries.CreateCampaign(ctx, gen.CreateCampaignParams{
		ID:               campaign.ID(),
		TenantID:         campaign.TenantID(),
		ChannelID:        campaign.ChannelID(),
		Title:            payload.Title(),
		Body:             payload.Body(),
		ImageUrl:         payload.ImageURL(),
		Data:             data,
		Priority:         string(campaign.Priority()),
		RecipientMode:    string(campaign.RecipientMode()),
		InlineRecipients: inlineRecipients,
		ScheduledAt:      campaign.ScheduledAt(),
		Status:           string(campaign.Status()),
		RunID:            campaign.RunID(),
		RunAttemptedAt:   campaign.RunAttemptedAt(),
	})
}

func (r *CampaignRepository) FindByID(ctx context.Context, id domain.CampaignID) (*domain.Campaign, error) {
	row, err := r.queries.GetCampaignByID(ctx, id)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ports.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return mappers.CampaignFromRow(row)
}

func (r *CampaignRepository) FindByIDs(
	ctx context.Context,
	ids []domain.CampaignID,
) (map[domain.CampaignID]*domain.Campaign, error) {
	rows, err := r.queries.GetCampaignByIDs(ctx, ids)
	return campaignsFromRows(rows, err)
}

func (r *CampaignRepository) FindByIDsForUpdate(
	ctx context.Context,
	ids []domain.CampaignID,
) (map[domain.CampaignID]*domain.Campaign, error) {
	rows, err := r.queries.GetCampaignByIDsForUpdate(ctx, ids)
	return campaignsFromRows(rows, err)
}

func campaignsFromRows(rows []gen.Campaign, err error) (map[domain.CampaignID]*domain.Campaign, error) {
	if err != nil {
		return nil, err
	}
	campaigns := make(map[domain.CampaignID]*domain.Campaign, len(rows))
	for _, row := range rows {
		campaign, err := mappers.CampaignFromRow(row)
		if err != nil {
			return nil, err
		}
		campaigns[campaign.ID()] = campaign
	}
	return campaigns, nil
}

func (r *CampaignRepository) FindStartCandidatesForUpdate(
	ctx context.Context,
	dueBefore time.Time,
	runAttemptedBefore time.Time,
	limit int,
) ([]*domain.Campaign, error) {
	if limit <= 0 || limit > math.MaxInt32 {
		return nil, fmt.Errorf("campaign start candidate limit must be between 1 and %d", math.MaxInt32)
	}
	rows, err := r.queries.ListStartCandidatesForUpdate(ctx, gen.ListStartCandidatesForUpdateParams{
		DueBefore:          &dueBefore,
		RunAttemptedBefore: &runAttemptedBefore,
		CandidateLimit:     int32(limit),
	})
	if err != nil {
		return nil, err
	}
	campaigns := make([]*domain.Campaign, 0, len(rows))
	for _, row := range rows {
		campaign, err := mappers.CampaignFromRow(row)
		if err != nil {
			return nil, err
		}
		campaigns = append(campaigns, campaign)
	}
	return campaigns, nil
}

func (r *CampaignRepository) FindByIDForUpdate(
	ctx context.Context,
	id domain.CampaignID,
) (*domain.Campaign, error) {
	row, err := r.queries.GetCampaignByIDForUpdate(ctx, id)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ports.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return mappers.CampaignFromRow(row)
}

func (r *CampaignRepository) Update(ctx context.Context, campaign *domain.Campaign) error {
	return r.queries.UpdateCampaign(ctx, updateCampaignParams(campaign))
}

func (r *CampaignRepository) UpdateBatch(ctx context.Context, campaigns []*domain.Campaign) error {
	if len(campaigns) == 0 {
		return nil
	}
	encoded, err := mappers.CampaignsToBatchUpdateJSON(campaigns)
	if err != nil {
		return err
	}
	return r.queries.UpdateCampaignBatch(ctx, encoded)
}

func updateCampaignParams(campaign *domain.Campaign) gen.UpdateCampaignParams {
	progress := campaign.CurrentProgress()
	params := gen.UpdateCampaignParams{
		ID:             campaign.ID(),
		Status:         string(campaign.Status()),
		ScheduledAt:    campaign.ScheduledAt(),
		RunID:          campaign.RunID(),
		RunAttemptedAt: campaign.RunAttemptedAt(),
		StartedAt:      campaign.StartedAt(),
		CompletedAt:    campaign.CompletedAt(),
		FailedAt:       campaign.FailedAt(),
		FailureReason:  campaign.FailureReason(),
	}
	if progress != nil {
		params.SourceBatchesTotal = int64(progress.SourceBatchesTotal())
		params.SourceBatchesFannedOut = int64(progress.SourceBatchesFannedOut())
		params.DeliveryTotal = int64(progress.DeliveryTotal())
		params.DeliveryProcessed = int64(progress.DeliveryProcessed())
		params.DeliveryAcceptedCount = int64(progress.DeliveryAcceptedCount())
		params.DeliveryFailedCount = int64(progress.DeliveryFailedCount())
	}
	return params
}
func (r *CampaignRepository) Tx(tx ports.Transaction) ports.CampaignRepository {
	return NewCampaignRepository(r.queries.WithTx(tx.(pgx.Tx)))
}
