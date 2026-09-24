package kafka

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
)

func TestNewTopicProvisionerValidatesParameters(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		params TopicProvisionerParams
	}{
		{name: "missing brokers", params: TopicProvisionerParams{WorkRetention: time.Hour, ProgressRetention: time.Hour}},
		{name: "zero work retention", params: TopicProvisionerParams{Brokers: []string{"localhost:9092"}, ProgressRetention: time.Hour}},
		{name: "zero progress retention", params: TopicProvisionerParams{Brokers: []string{"localhost:9092"}, WorkRetention: time.Hour}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if _, err := NewTopicProvisioner(test.params); err == nil {
				t.Fatal("NewTopicProvisioner error = nil, want validation error")
			}
		})
	}
}

func TestIsTopicMetadataPending(t *testing.T) {
	t.Parallel()
	if !isTopicMetadataPending(fmt.Errorf("describe: %w", kerr.UnknownTopicOrPartition)) {
		t.Fatal("isTopicMetadataPending() = false, want true")
	}
	if isTopicMetadataPending(errors.New("unavailable")) {
		t.Fatal("isTopicMetadataPending() = true, want false")
	}
}

func TestTopicProvisionerStatsUsesProgressRetention(t *testing.T) {
	t.Parallel()
	provisioner := &TopicProvisioner{progressRetention: 14 * 24 * time.Hour}
	config := provisioner.statsTopicConfig()
	if got := *config["cleanup.policy"]; got != "compact,delete" {
		t.Fatalf("stats cleanup policy = %q, want compact,delete", got)
	}
	if got := *config["retention.ms"]; got != "1209600000" {
		t.Fatalf("stats retention.ms = %q, want 1209600000", got)
	}
}
