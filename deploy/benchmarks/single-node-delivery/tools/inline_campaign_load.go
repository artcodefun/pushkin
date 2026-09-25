// inline_campaign_load submits one immediate inline campaign per fixture user.
// It measures control-plane submission, not final provider delivery latency.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type createCampaignRequest struct {
	ChannelKey   string       `json:"channel_key"`
	Priority     string       `json:"priority"`
	Notification notification `json:"notification"`
	UserIDs      []string     `json:"user_ids"`
}

type notification struct {
	Title string `json:"title"`
	Body  string `json:"body"`
}

type result struct {
	Target          string  `json:"target"`
	CampaignCount   int     `json:"campaign_count"`
	Concurrency     int     `json:"concurrency"`
	SuccessfulCount uint64  `json:"successful_count"`
	FailedCount     uint64  `json:"failed_count"`
	Duration        string  `json:"duration"`
	RequestsPerSec  float64 `json:"requests_per_second"`
}

func main() {
	target := flag.String("target", "http://127.0.0.1:18080", "Pushkin HTTP base URL")
	count := flag.Int("count", 100000, "number of one-recipient inline campaigns")
	concurrency := flag.Int("concurrency", 500, "concurrent HTTP requests")
	channelKey := flag.String("channel-key", "benchmark", "target channel key")
	flag.Parse()

	apiKey := os.Getenv("PUSHKIN_BENCHMARK_API_KEY")
	if apiKey == "" {
		fmt.Fprintln(os.Stderr, "PUSHKIN_BENCHMARK_API_KEY is required")
		os.Exit(2)
	}
	if *count <= 0 || *concurrency <= 0 {
		fmt.Fprintln(os.Stderr, "count and concurrency must be positive")
		os.Exit(2)
	}

	client := &http.Client{Transport: &http.Transport{
		MaxIdleConns:        *concurrency * 2,
		MaxIdleConnsPerHost: *concurrency * 2,
		IdleConnTimeout:     30 * time.Second,
	}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	jobs := make(chan int)
	var successful atomic.Uint64
	var failed atomic.Uint64
	var group sync.WaitGroup
	for range *concurrency {
		group.Go(func() {
			for index := range jobs {
				if err := createCampaign(ctx, client, *target, apiKey, *channelKey, index); err != nil {
					failed.Add(1)
					fmt.Fprintln(os.Stderr, err)
					continue
				}
				successful.Add(1)
			}
		})
	}

	startedAt := time.Now()
	progressDone := make(chan struct{})
	go reportProgress(startedAt, *count, &successful, &failed, progressDone)
	for index := range *count {
		jobs <- index
	}
	close(jobs)
	group.Wait()
	close(progressDone)

	duration := time.Since(startedAt)
	output := result{
		Target:          *target,
		CampaignCount:   *count,
		Concurrency:     *concurrency,
		SuccessfulCount: successful.Load(),
		FailedCount:     failed.Load(),
		Duration:        duration.String(),
		RequestsPerSec:  float64(successful.Load()) / duration.Seconds(),
	}
	if err := json.NewEncoder(os.Stdout).Encode(output); err != nil {
		fmt.Fprintln(os.Stderr, "encode result:", err)
		os.Exit(1)
	}
	if output.FailedCount > 0 {
		os.Exit(1)
	}
}

func createCampaign(ctx context.Context, client *http.Client, target, apiKey, channelKey string, index int) error {
	body, err := json.Marshal(createCampaignRequest{
		ChannelKey: channelKey,
		Priority:   "high",
		Notification: notification{
			Title: "Benchmark",
			Body:  "Inline delivery",
		},
		UserIDs: []string{fmt.Sprintf("benchmark-user-%d", index)},
	})
	if err != nil {
		return fmt.Errorf("encode campaign %d: %w", index, err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, target+"/api/v1/campaigns/inline", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create request for campaign %d: %w", index, err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Pushkin-API-Key", apiKey)

	response, err := client.Do(request)
	if err != nil {
		return fmt.Errorf("submit campaign %d: %w", index, err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusCreated {
		return fmt.Errorf("submit campaign %d: unexpected status %s", index, response.Status)
	}
	return nil
}

func reportProgress(startedAt time.Time, target int, successful, failed *atomic.Uint64, done <-chan struct{}) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			return
		case now := <-ticker.C:
			completed := successful.Load() + failed.Load()
			elapsed := now.Sub(startedAt).Seconds()
			fmt.Fprintf(os.Stderr, "submitted=%d/%d successful=%d failed=%d rate=%.0f/s\n", completed, target, successful.Load(), failed.Load(), float64(completed)/elapsed)
		}
	}
}
