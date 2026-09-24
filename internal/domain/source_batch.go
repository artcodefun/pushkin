package domain

import (
	"fmt"
	"uuid"
)

type SourceBatch struct {
	id         SourceBatchID
	campaignID CampaignID
	userIDs    []UserID
}

func NewSourceBatch(campaignID CampaignID, userIDs []UserID) (*SourceBatch, error) {
	if err := requireUUID("campaign_id", campaignID); err != nil {
		return nil, err
	}
	if len(userIDs) == 0 {
		return nil, fmt.Errorf("%w: source batch must contain at least one user_id", ErrInvalidArgument)
	}

	ids := make([]UserID, len(userIDs))
	copy(ids, userIDs)
	for _, userID := range ids {
		if err := requireIdentifier("user_id", string(userID)); err != nil {
			return nil, err
		}
	}

	return &SourceBatch{id: uuid.NewV7(), campaignID: campaignID, userIDs: ids}, nil
}

func (b *SourceBatch) ID() SourceBatchID      { return b.id }
func (b *SourceBatch) CampaignID() CampaignID { return b.campaignID }
func (b *SourceBatch) Count() int             { return len(b.userIDs) }

func (b *SourceBatch) UserIDs() []UserID {
	result := make([]UserID, len(b.userIDs))
	copy(result, b.userIDs)
	return result
}

type HydrateSourceBatchParams struct {
	ID         SourceBatchID
	CampaignID CampaignID
	UserIDs    []UserID
}

func HydrateSourceBatch(params HydrateSourceBatchParams) (*SourceBatch, error) {
	if err := requireUUID("source_batch_id", params.ID); err != nil {
		return nil, err
	}
	if err := requireUUID("campaign_id", params.CampaignID); err != nil {
		return nil, err
	}
	if len(params.UserIDs) == 0 {
		return nil, fmt.Errorf("%w: source batch must contain at least one user_id", ErrInvalidArgument)
	}
	userIDs := make([]UserID, len(params.UserIDs))
	copy(userIDs, params.UserIDs)
	for _, userID := range userIDs {
		if err := requireIdentifier("user_id", string(userID)); err != nil {
			return nil, err
		}
	}
	return &SourceBatch{id: params.ID, campaignID: params.CampaignID, userIDs: userIDs}, nil
}
