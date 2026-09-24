package domain

import (
	"testing"
)

func TestCampaignProgressCompletesAfterFanoutAndDelivery(t *testing.T) {
	t.Parallel()

	progress := NewEmptyCampaignProgress(2)
	if err := progress.RecordSourceBatchFanout(3); err != nil {
		t.Fatalf("record first fanout: %v", err)
	}
	if err := progress.RecordDeliveryResults(2, 1); err != nil {
		t.Fatalf("record first results: %v", err)
	}
	if progress.IsComplete() {
		t.Fatal("campaign completed before all source batches were fanned out")
	}
	if err := progress.RecordSourceBatchFanout(2); err != nil {
		t.Fatalf("record second fanout: %v", err)
	}
	if err := progress.RecordDeliveryResults(1, 1); err != nil {
		t.Fatalf("record second results: %v", err)
	}
	if !progress.IsComplete() {
		t.Fatal("campaign did not complete after all deliveries became terminal")
	}
}

func TestCampaignProgressAllowsResultsBeforeFanoutMarker(t *testing.T) {
	t.Parallel()

	progress := NewEmptyCampaignProgress(1)
	if err := progress.RecordDeliveryResults(100, 0); err != nil {
		t.Fatalf("record early results: %v", err)
	}
	if progress.IsComplete() {
		t.Fatal("campaign completed without its fanout marker")
	}
	if err := progress.RecordSourceBatchFanout(100); err != nil {
		t.Fatalf("record delayed fanout marker: %v", err)
	}
	if !progress.IsComplete() {
		t.Fatal("campaign did not complete after delayed fanout marker")
	}
}

func TestEmptyCampaignCompletesImmediately(t *testing.T) {
	t.Parallel()

	progress := NewEmptyCampaignProgress(0)
	if !progress.IsComplete() {
		t.Fatal("campaign without source batches must be complete")
	}
}

func TestCampaignProgressMonotonicity(t *testing.T) {
	t.Parallel()

	previous := NewEmptyCampaignProgress(1)
	if err := previous.RecordSourceBatchFanout(1); err != nil {
		t.Fatalf("record previous fanout: %v", err)
	}
	if err := previous.RecordDeliveryResults(1, 0); err != nil {
		t.Fatalf("record previous delivery result: %v", err)
	}
	next := previous.Copy()
	if !next.IsMonotonicFrom(previous) {
		t.Fatal("equal progress must be monotonic")
	}
	if next.IsMonotonicFrom(nil) {
		t.Fatal("nil previous progress must not be monotonic")
	}
	if NewEmptyCampaignProgress(0).IsMonotonicFrom(previous) {
		t.Fatal("decreasing progress must not be monotonic")
	}
}
