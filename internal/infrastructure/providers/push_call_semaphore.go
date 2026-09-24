package providers

import (
	"context"
	"fmt"
)

// PushCallSemaphore bounds concurrent provider calls within one process.
// A successful Acquire must be paired with exactly one Release.
type PushCallSemaphore struct {
	slots chan struct{}
}

func NewPushCallSemaphore(maxInFlight int) (*PushCallSemaphore, error) {
	if maxInFlight <= 0 {
		return nil, fmt.Errorf("maximum in-flight push calls must be positive")
	}
	return &PushCallSemaphore{slots: make(chan struct{}, maxInFlight)}, nil
}

func (s *PushCallSemaphore) Acquire(ctx context.Context) error {
	select {
	case s.slots <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *PushCallSemaphore) Release() {
	select {
	case <-s.slots:
	default:
		panic("push call semaphore released without an acquired slot")
	}
}
