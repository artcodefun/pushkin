package domain

import (
	"testing"
	"uuid"
)

func TestSourceBatchIsImmutable(t *testing.T) {
	t.Parallel()

	input := []UserID{"user-1", "user-2"}
	batch, err := NewSourceBatch(uuid.NewV7(), input)
	if err != nil {
		t.Fatalf("new source batch: %v", err)
	}
	if batch.ID() == uuid.Nil() {
		t.Fatal("new source batch must generate an ID")
	}

	input[0] = "changed"
	output := batch.UserIDs()
	output[1] = "changed"

	if got := batch.UserIDs(); got[0] != "user-1" || got[1] != "user-2" {
		t.Fatalf("source batch was mutated: %v", got)
	}
}

func TestHydrateSourceBatchPreservesID(t *testing.T) {
	t.Parallel()

	batchID := uuid.NewV7()
	batch, err := HydrateSourceBatch(HydrateSourceBatchParams{
		ID:         batchID,
		CampaignID: uuid.NewV7(),
		UserIDs:    []UserID{"subscriber-1"},
	})
	if err != nil {
		t.Fatalf("hydrate source batch: %v", err)
	}
	if batch.ID() != batchID {
		t.Fatalf("source batch id = %s, want %s", batch.ID(), batchID)
	}
}
