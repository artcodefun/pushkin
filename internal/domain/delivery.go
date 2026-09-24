package domain

import (
	"uuid"
)

const MaxDeliveryRetryAttempts uint = 3

type DeliveryWork struct {
	id                 DeliveryID
	campaignID         CampaignID
	tenantID           TenantID
	channelID          ChannelID
	pushInstallationID PushInstallationID
	priority           Priority
	retryAttempt       uint
}

type NewDeliveryWorkParams struct {
	CampaignID         CampaignID
	TenantID           TenantID
	ChannelID          ChannelID
	PushInstallationID PushInstallationID
	Priority           Priority
}

func NewDeliveryWork(params NewDeliveryWorkParams) (DeliveryWork, error) {
	if err := requireUUID("campaign_id", params.CampaignID); err != nil {
		return DeliveryWork{}, err
	}
	if err := requireUUID("tenant_id", params.TenantID); err != nil {
		return DeliveryWork{}, err
	}
	if err := requireUUID("channel_id", params.ChannelID); err != nil {
		return DeliveryWork{}, err
	}
	if err := requireUUID("push_installation_id", params.PushInstallationID); err != nil {
		return DeliveryWork{}, err
	}
	if !params.Priority.IsValid() {
		return DeliveryWork{}, ErrInvalidArgument
	}

	return DeliveryWork{
		id:                 uuid.NewV7(),
		campaignID:         params.CampaignID,
		tenantID:           params.TenantID,
		channelID:          params.ChannelID,
		pushInstallationID: params.PushInstallationID,
		priority:           params.Priority,
	}, nil
}

func (w DeliveryWork) ID() DeliveryID                         { return w.id }
func (w DeliveryWork) CampaignID() CampaignID                 { return w.campaignID }
func (w DeliveryWork) TenantID() TenantID                     { return w.tenantID }
func (w DeliveryWork) ChannelID() ChannelID                   { return w.channelID }
func (w DeliveryWork) PushInstallationID() PushInstallationID { return w.pushInstallationID }
func (w DeliveryWork) Priority() Priority                     { return w.priority }
func (w DeliveryWork) RetryAttempt() uint                     { return w.retryAttempt }

func (w *DeliveryWork) IncreaseRetry() error {
	if w.retryAttempt >= MaxDeliveryRetryAttempts {
		return ErrInvalidTransition
	}
	w.retryAttempt++
	return nil
}
