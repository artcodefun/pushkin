package domain

import (
	"fmt"
)

// CampaignProgress is Campaign's current delivery state. Delivery results may
// arrive before all fan-out markers because they travel through different Kafka
// topics, so temporary processed > total is valid until every source batch has
// been fanned out.
type CampaignProgress struct {
	sourceBatchesTotal     uint64
	sourceBatchesFannedOut uint64
	deliveryTotal          uint64
	deliveryProcessed      uint64
	deliveryAcceptedCount  uint64
	deliveryFailedCount    uint64
}

func NewEmptyCampaignProgress(sourceBatchesTotal uint64) *CampaignProgress {
	return &CampaignProgress{sourceBatchesTotal: sourceBatchesTotal}
}

func (p *CampaignProgress) RecordSourceBatchFanout(deliveryCount uint64) error {
	if p.sourceBatchesFannedOut == p.sourceBatchesTotal {
		return fmt.Errorf("%w: all source batches are already fanned out", ErrInvalidTransition)
	}
	p.sourceBatchesFannedOut++
	p.deliveryTotal += deliveryCount
	return nil
}

func (p *CampaignProgress) RecordDeliveryResults(accepted, failed uint64) error {
	p.deliveryProcessed += accepted + failed
	p.deliveryAcceptedCount += accepted
	p.deliveryFailedCount += failed
	return nil
}

func (p *CampaignProgress) IsComplete() bool {
	return p.sourceBatchesFannedOut == p.sourceBatchesTotal &&
		p.deliveryProcessed == p.deliveryTotal
}

func (p *CampaignProgress) Copy() *CampaignProgress {
	if p == nil {
		return nil
	}
	copy := *p
	return &copy
}

func (p *CampaignProgress) IsMonotonicFrom(previous *CampaignProgress) bool {
	if p == nil || previous == nil {
		return false
	}
	return p.sourceBatchesTotal >= previous.sourceBatchesTotal &&
		p.sourceBatchesFannedOut >= previous.sourceBatchesFannedOut &&
		p.deliveryTotal >= previous.deliveryTotal &&
		p.deliveryProcessed >= previous.deliveryProcessed &&
		p.deliveryAcceptedCount >= previous.deliveryAcceptedCount &&
		p.deliveryFailedCount >= previous.deliveryFailedCount
}

func (p *CampaignProgress) Equals(other *CampaignProgress) bool {
	if p == nil || other == nil {
		return p == other
	}
	return p.sourceBatchesTotal == other.sourceBatchesTotal &&
		p.sourceBatchesFannedOut == other.sourceBatchesFannedOut &&
		p.deliveryTotal == other.deliveryTotal &&
		p.deliveryProcessed == other.deliveryProcessed &&
		p.deliveryAcceptedCount == other.deliveryAcceptedCount &&
		p.deliveryFailedCount == other.deliveryFailedCount
}

func (p *CampaignProgress) SourceBatchesTotal() uint64     { return p.sourceBatchesTotal }
func (p *CampaignProgress) SourceBatchesFannedOut() uint64 { return p.sourceBatchesFannedOut }
func (p *CampaignProgress) DeliveryTotal() uint64          { return p.deliveryTotal }
func (p *CampaignProgress) DeliveryProcessed() uint64      { return p.deliveryProcessed }
func (p *CampaignProgress) DeliveryAcceptedCount() uint64  { return p.deliveryAcceptedCount }
func (p *CampaignProgress) DeliveryFailedCount() uint64    { return p.deliveryFailedCount }

type HydrateCampaignProgressParams struct {
	SourceBatchesTotal     uint64
	SourceBatchesFannedOut uint64
	DeliveryTotal          uint64
	DeliveryProcessed      uint64
	DeliveryAcceptedCount  uint64
	DeliveryFailedCount    uint64
}

// HydrateCampaignProgress reconstructs a previously reduced progress state.
// It is used by the Kafka snapshot adapter when an aggregator obtains a
// partition after restart or rebalance.
func HydrateCampaignProgress(params HydrateCampaignProgressParams) (*CampaignProgress, error) {
	if params.SourceBatchesFannedOut > params.SourceBatchesTotal {
		return nil, fmt.Errorf("%w: source batches fanned out exceeds total", ErrInvalidArgument)
	}
	if params.DeliveryAcceptedCount+params.DeliveryFailedCount != params.DeliveryProcessed {
		return nil, fmt.Errorf("%w: terminal delivery counts do not match processed count", ErrInvalidArgument)
	}
	return &CampaignProgress{
		sourceBatchesTotal:     params.SourceBatchesTotal,
		sourceBatchesFannedOut: params.SourceBatchesFannedOut,
		deliveryTotal:          params.DeliveryTotal,
		deliveryProcessed:      params.DeliveryProcessed,
		deliveryAcceptedCount:  params.DeliveryAcceptedCount,
		deliveryFailedCount:    params.DeliveryFailedCount,
	}, nil
}
