package mappers

import (
	"encoding/json"
	"fmt"

	"github.com/superman/pushkin/internal/domain"
	"github.com/superman/pushkin/internal/infrastructure/postgres/gen"
)

func SourceBatchFromRow(row gen.CampaignRecipientBatch) (*domain.SourceBatch, error) {
	var userIDValues []string
	if err := json.Unmarshal(row.UserIds, &userIDValues); err != nil {
		return nil, fmt.Errorf("source batch %s user_ids: %w", row.ID, err)
	}
	if int64(len(userIDValues)) != int64(row.RecipientCount) {
		return nil, fmt.Errorf("source batch %s has inconsistent recipient count", row.ID)
	}
	userIDs := make([]domain.UserID, len(userIDValues))
	for index, userID := range userIDValues {
		userIDs[index] = domain.UserID(userID)
	}
	return domain.HydrateSourceBatch(domain.HydrateSourceBatchParams{
		ID:         row.ID,
		CampaignID: row.CampaignID,
		UserIDs:    userIDs,
	})
}
