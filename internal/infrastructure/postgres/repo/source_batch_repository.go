package repo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"

	"github.com/jackc/pgx/v5"

	"github.com/superman/pushkin/internal/application/ports"
	"github.com/superman/pushkin/internal/domain"
	"github.com/superman/pushkin/internal/infrastructure/postgres/gen"
	"github.com/superman/pushkin/internal/infrastructure/postgres/repo/mappers"
)

var _ ports.SourceBatchRepository = (*SourceBatchRepository)(nil)

type SourceBatchRepository struct {
	queries *gen.Queries
}

func NewSourceBatchRepository(queries *gen.Queries) *SourceBatchRepository {
	return &SourceBatchRepository{queries: queries}
}

func (r *SourceBatchRepository) Create(ctx context.Context, batch *domain.SourceBatch) error {
	if batch.Count() > math.MaxInt32 {
		return fmt.Errorf("source batch recipient count exceeds %d", math.MaxInt32)
	}
	userIDs := batch.UserIDs()
	userIDValues := make([]string, len(userIDs))
	for index, userID := range userIDs {
		userIDValues[index] = string(userID)
	}
	encodedUserIDs, err := json.Marshal(userIDValues)
	if err != nil {
		return err
	}
	return r.queries.CreateSourceBatch(ctx, gen.CreateSourceBatchParams{
		ID:             batch.ID(),
		CampaignID:     batch.CampaignID(),
		UserIds:        encodedUserIDs,
		RecipientCount: int32(batch.Count()),
	})
}

func (r *SourceBatchRepository) FindByID(
	ctx context.Context,
	id domain.SourceBatchID,
) (*domain.SourceBatch, error) {
	row, err := r.queries.GetSourceBatchByID(ctx, id)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ports.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return mappers.SourceBatchFromRow(row)
}

func (r *SourceBatchRepository) ListIDsByCampaign(
	ctx context.Context,
	campaignID domain.CampaignID,
) ([]domain.SourceBatchID, error) {
	return r.queries.ListSourceBatchIDsByCampaign(ctx, campaignID)
}

func (r *SourceBatchRepository) Tx(tx ports.Transaction) ports.SourceBatchRepository {
	return NewSourceBatchRepository(r.queries.WithTx(tx.(pgx.Tx)))
}
