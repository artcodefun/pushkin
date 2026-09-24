package kafka

import (
	"errors"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestIsKafkaDataLoss(t *testing.T) {
	t.Parallel()

	dataLoss := &kgo.ErrDataLoss{Topic: "pushkin.campaign.stats", Partition: 0, ConsumedTo: 20, ResetTo: 10}
	if !isKafkaDataLoss(errors.Join(errors.New("fetch failed"), dataLoss)) {
		t.Fatal("data loss error was not recognized")
	}
	if isKafkaDataLoss(errors.New("fetch failed")) {
		t.Fatal("ordinary error was recognized as data loss")
	}
}
