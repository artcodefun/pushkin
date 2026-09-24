package bootstrap

import (
	"context"
	"errors"
	"testing"
	"time"
	"uuid"

	"github.com/superman/pushkin/internal/domain"
)

func TestWorkersStopOnContextCancellation(t *testing.T) {
	t.Parallel()
	started := make(chan struct{}, 2)
	workers := &Workers{runners: []workerRunner{
		{
			name: "first",
			run: func(ctx context.Context) error {
				started <- struct{}{}
				<-ctx.Done()
				return nil
			},
		},
		{
			name: "second",
			run: func(ctx context.Context) error {
				started <- struct{}{}
				<-ctx.Done()
				return nil
			},
		},
	}}
	ctx, cancel := context.WithCancel(context.Background())
	workers.Start(ctx)
	<-started
	<-started
	cancel()

	if err := workers.Wait(); err != nil {
		t.Fatalf("wait workers: %v", err)
	}
}

func TestWorkersRetryFailureWithoutCancelingSiblings(t *testing.T) {
	t.Parallel()
	want := errors.New("worker failed")
	failed := make(chan struct{}, 2)
	siblingStarted := make(chan struct{})
	workers := &Workers{
		initialRetryDelay: time.Nanosecond,
		maximumRetryDelay: time.Nanosecond,
		runners: []workerRunner{
			{
				name: "failing",
				run: func(context.Context) error {
					failed <- struct{}{}
					return want
				},
			},
			{
				name: "sibling",
				run: func(ctx context.Context) error {
					close(siblingStarted)
					<-ctx.Done()
					return nil
				},
			},
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	workers.Start(ctx)
	<-siblingStarted
	<-failed
	<-failed
	cancel()

	if err := workers.Wait(); err != nil {
		t.Fatalf("wait workers: %v", err)
	}
}

func TestNextWorkerRetryDelayCapsAtMaximum(t *testing.T) {
	t.Parallel()

	if got := nextWorkerRetryDelay(time.Second, 30*time.Second); got != 2*time.Second {
		t.Fatalf("first retry delay = %s, want 2s", got)
	}
	if got := nextWorkerRetryDelay(16*time.Second, 30*time.Second); got != 30*time.Second {
		t.Fatalf("capped retry delay = %s, want 30s", got)
	}
	if got := nextWorkerRetryDelay(30*time.Second, 30*time.Second); got != 30*time.Second {
		t.Fatalf("maximum retry delay = %s, want 30s", got)
	}
}

func TestWorkersWaitBeforeStartFails(t *testing.T) {
	t.Parallel()
	if err := (&Workers{}).Wait(); err == nil {
		t.Fatal("wait before start must fail")
	}
}

func TestNewChannelProcessorsCreatesDeliveryAndRetryProcessors(t *testing.T) {
	t.Parallel()
	config := Config{
		KafkaBrokers:                []string{"localhost:9092"},
		InstanceID:                  "test-instance",
		DeliveryProcessingBatchSize: 100,
	}
	processors, err := newChannelProcessors(uuid.NewV7(), NewServices(&Adapters{}, config), config)
	if err != nil {
		t.Fatalf("create channel processors: %v", err)
	}
	defer closeChannelProcessors(processors)
	if len(processors) != 6 {
		t.Fatalf("processor count = %d, want 6", len(processors))
	}
}

func TestNewChannelProcessorsRejectsNilChannelID(t *testing.T) {
	t.Parallel()
	config := Config{
		KafkaBrokers:                []string{"localhost:9092"},
		InstanceID:                  "test-instance",
		DeliveryProcessingBatchSize: 100,
	}
	if _, err := newChannelProcessors(domain.ChannelID{}, NewServices(&Adapters{}, config), config); err == nil {
		t.Fatal("nil channel ID must be rejected")
	}
}

func TestRunProcessWithRetryRepeatsUntilContextCancellation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	processed := 0
	err := runProcessWithRetry(ctx, "test", func(context.Context) error {
		processed++
		if processed == 3 {
			cancel()
		}
		return nil
	}, time.Second, 30*time.Second)
	if err != nil {
		t.Fatalf("run channel processor: %v", err)
	}
	if processed != 3 {
		t.Fatalf("processed = %d, want 3", processed)
	}
}

func TestRunProcessWithRetryRetriesProcessError(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	attempts := 0
	err := runProcessWithRetry(ctx, "test", func(context.Context) error {
		attempts++
		if attempts == 1 {
			return errors.New("process failed")
		}
		cancel()
		return nil
	}, time.Nanosecond, time.Nanosecond)
	if err != nil {
		t.Fatalf("run channel processor: %v", err)
	}
	if attempts != 2 {
		t.Fatalf("attempts = %d, want 2", attempts)
	}
}
