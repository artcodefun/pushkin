package providers

import (
	"context"
	"errors"
	"testing"
)

func TestNewPushCallSemaphoreRejectsNonPositiveLimit(t *testing.T) {
	t.Parallel()

	for _, limit := range []int{0, -1} {
		t.Run("limit", func(t *testing.T) {
			t.Parallel()
			semaphore, err := NewPushCallSemaphore(limit)
			if err == nil {
				t.Fatal("expected an error")
			}
			if semaphore != nil {
				t.Fatal("expected no semaphore")
			}
		})
	}
}

func TestPushCallSemaphoreBlocksUntilRelease(t *testing.T) {
	t.Parallel()

	semaphore, err := NewPushCallSemaphore(1)
	if err != nil {
		t.Fatalf("new semaphore: %v", err)
	}
	if err := semaphore.Acquire(context.Background()); err != nil {
		t.Fatalf("acquire first slot: %v", err)
	}

	acquired := make(chan error, 1)
	go func() {
		acquired <- semaphore.Acquire(context.Background())
	}()

	select {
	case err := <-acquired:
		t.Fatalf("acquire returned before release: %v", err)
	default:
	}

	semaphore.Release()
	if err := <-acquired; err != nil {
		t.Fatalf("acquire after release: %v", err)
	}
	semaphore.Release()
}

func TestPushCallSemaphoreAcquireHonorsContextCancellation(t *testing.T) {
	t.Parallel()

	semaphore, err := NewPushCallSemaphore(1)
	if err != nil {
		t.Fatalf("new semaphore: %v", err)
	}
	if err := semaphore.Acquire(context.Background()); err != nil {
		t.Fatalf("acquire first slot: %v", err)
	}
	defer semaphore.Release()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := semaphore.Acquire(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("acquire error = %v, want context canceled", err)
	}
}

func TestPushCallSemaphorePanicsOnUnpairedRelease(t *testing.T) {
	t.Parallel()

	semaphore, err := NewPushCallSemaphore(1)
	if err != nil {
		t.Fatalf("new semaphore: %v", err)
	}
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic")
		}
	}()

	semaphore.Release()
}
