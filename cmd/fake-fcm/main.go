package main

import (
	cryptorand "crypto/rand"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/big"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

const failurePercentage = 5

type sendRequest struct {
	Token string `json:"token"`
}

type statsResponse struct {
	ReceivedCount             uint64  `json:"received_count"`
	AcceptedCount             uint64  `json:"accepted_count"`
	FailedCount               uint64  `json:"failed_count"`
	InvalidRequestCount       uint64  `json:"invalid_request_count"`
	ActiveCount               uint64  `json:"active_count"`
	MaxActiveCount            uint64  `json:"max_active_count"`
	CompletedCount            uint64  `json:"completed_count"`
	MaxCompletedPerSecond     uint64  `json:"max_completed_per_second"`
	AverageCompletedPerSecond float64 `json:"average_completed_per_second"`
}

type fakeFCM struct {
	delay                   time.Duration
	failures                int
	samplePercentage        func() (int, error)
	received                atomic.Uint64
	accepted                atomic.Uint64
	failed                  atomic.Uint64
	invalid                 atomic.Uint64
	active                  atomic.Uint64
	maxActive               atomic.Uint64
	completed               atomic.Uint64
	completedSecond         atomic.Int64
	completedInSecond       atomic.Uint64
	maxCompletedPerSecond   atomic.Uint64
	firstCompletionUnixNano atomic.Int64
	lastCompletionUnixNano  atomic.Int64
}

func main() {
	address := strings.TrimSpace(os.Getenv("FAKE_FCM_HTTP_ADDRESS"))
	if address == "" {
		address = ":8081"
	}
	delay, err := delayFromEnv()
	if err != nil {
		slog.Error("load fake FCM configuration", "error", err)
		os.Exit(1)
	}
	failures, err := failurePercentageFromEnv()
	if err != nil {
		slog.Error("load fake FCM configuration", "error", err)
		os.Exit(1)
	}

	slog.Info("starting fake FCM", "address", address, "delay", delay, "failure_percentage", failures)
	server := newFakeFCM(delay, failures, randomPercentage)
	if err := http.ListenAndServe(address, server.Handler()); err != nil {
		slog.Error("serve fake FCM", "error", err)
		os.Exit(1)
	}
}

func newFakeFCM(delay time.Duration, failures int, samplePercentage func() (int, error)) *fakeFCM {
	return &fakeFCM{delay: delay, failures: failures, samplePercentage: samplePercentage}
}

func (s *fakeFCM) Handler() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("GET /stats", s.stats)
	mux.HandleFunc("POST /send", s.send)
	return mux
}

func (s *fakeFCM) send(writer http.ResponseWriter, request *http.Request) {
	var message sendRequest
	if err := json.NewDecoder(http.MaxBytesReader(writer, request.Body, 64<<10)).Decode(&message); err != nil || strings.TrimSpace(message.Token) == "" {
		s.invalid.Add(1)
		writer.WriteHeader(http.StatusBadRequest)
		return
	}
	s.received.Add(1)
	active := s.active.Add(1)
	updateMaximum(&s.maxActive, active)
	defer s.active.Add(^uint64(0))
	if s.delay > 0 {
		select {
		case <-request.Context().Done():
			return
		case <-time.After(s.delay):
		}
	}
	percentage, err := s.samplePercentage()
	s.recordCompletion()
	if err != nil || percentage < s.failures {
		s.failed.Add(1)
		writer.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	s.accepted.Add(1)
	writer.WriteHeader(http.StatusAccepted)
}

func (s *fakeFCM) stats(writer http.ResponseWriter, _ *http.Request) {
	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(statsResponse{
		ReceivedCount:             s.received.Load(),
		AcceptedCount:             s.accepted.Load(),
		FailedCount:               s.failed.Load(),
		InvalidRequestCount:       s.invalid.Load(),
		ActiveCount:               s.active.Load(),
		MaxActiveCount:            s.maxActive.Load(),
		CompletedCount:            s.completed.Load(),
		MaxCompletedPerSecond:     s.maxCompletedPerSecond.Load(),
		AverageCompletedPerSecond: s.averageCompletedPerSecond(),
	})
}

func (s *fakeFCM) recordCompletion() {
	s.completed.Add(1)
	now := time.Now()
	s.firstCompletionUnixNano.CompareAndSwap(0, now.UnixNano())
	updateMaximumInt64(&s.lastCompletionUnixNano, now.UnixNano())
	second := now.Unix()
	for {
		currentSecond := s.completedSecond.Load()
		if currentSecond == second {
			count := s.completedInSecond.Add(1)
			updateMaximum(&s.maxCompletedPerSecond, count)
			return
		}
		if s.completedSecond.CompareAndSwap(currentSecond, second) {
			s.completedInSecond.Store(1)
			updateMaximum(&s.maxCompletedPerSecond, 1)
			return
		}
	}
}

func (s *fakeFCM) averageCompletedPerSecond() float64 {
	first := s.firstCompletionUnixNano.Load()
	last := s.lastCompletionUnixNano.Load()
	if first == 0 || last <= first {
		return 0
	}
	return float64(s.completed.Load()) / float64(last-first) * float64(time.Second)
}

func updateMaximum(target *atomic.Uint64, value uint64) {
	for {
		current := target.Load()
		if current >= value || target.CompareAndSwap(current, value) {
			return
		}
	}
}

func updateMaximumInt64(target *atomic.Int64, value int64) {
	for {
		current := target.Load()
		if current >= value || target.CompareAndSwap(current, value) {
			return
		}
	}
}

func randomPercentage() (int, error) {
	value, err := cryptorand.Int(cryptorand.Reader, big.NewInt(100))
	if err != nil {
		return 0, fmt.Errorf("sample fake FCM outcome: %w", err)
	}
	return int(value.Int64()), nil
}

func delayFromEnv() (time.Duration, error) {
	value := strings.TrimSpace(os.Getenv("FAKE_FCM_RESPONSE_DELAY"))
	if value == "" {
		return 100 * time.Millisecond, nil
	}
	delay, err := time.ParseDuration(value)
	if err != nil || delay < 0 {
		return 0, fmt.Errorf("FAKE_FCM_RESPONSE_DELAY must be a non-negative duration")
	}
	return delay, nil
}

func failurePercentageFromEnv() (int, error) {
	value := strings.TrimSpace(os.Getenv("FAKE_FCM_FAILURE_PERCENTAGE"))
	if value == "" {
		return failurePercentage, nil
	}
	percentage, err := strconv.Atoi(value)
	if err != nil || percentage < 0 || percentage > 100 {
		return 0, fmt.Errorf("FAKE_FCM_FAILURE_PERCENTAGE must be an integer from 0 through 100")
	}
	return percentage, nil
}
