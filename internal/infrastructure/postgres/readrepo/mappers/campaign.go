package mappers

import (
	"encoding/json"
	"fmt"

	"github.com/superman/pushkin/internal/application/queries/readmodels"
	"github.com/superman/pushkin/internal/domain"
	"github.com/superman/pushkin/internal/infrastructure/postgres/gen"
)

func CampaignFromRow(row gen.GetCampaignReadModelByIDRow) (*readmodels.Campaign, error) {
	if row.DeliveryTotal < 0 || row.DeliveryProcessed < 0 ||
		row.DeliveryAcceptedCount < 0 || row.DeliveryFailedCount < 0 {
		return nil, fmt.Errorf("campaign %s has negative delivery counts", row.ID)
	}

	data := make(map[string]string)
	if len(row.Data) > 0 {
		if err := json.Unmarshal(row.Data, &data); err != nil {
			return nil, fmt.Errorf("campaign %s payload data: %w", row.ID, err)
		}
	}
	return &readmodels.Campaign{
		ID:                    row.ID,
		TenantID:              row.TenantID,
		ChannelID:             row.ChannelID,
		Priority:              domain.Priority(row.Priority),
		Status:                domain.CampaignStatus(row.Status),
		Title:                 row.Title,
		Body:                  row.Body,
		ImageURL:              row.ImageUrl,
		Data:                  data,
		ScheduledAt:           row.ScheduledAt,
		StartedAt:             row.StartedAt,
		DeliveryTotal:         uint64(row.DeliveryTotal),
		DeliveryProcessed:     uint64(row.DeliveryProcessed),
		DeliveryAcceptedCount: uint64(row.DeliveryAcceptedCount),
		DeliveryFailedCount:   uint64(row.DeliveryFailedCount),
	}, nil
}
