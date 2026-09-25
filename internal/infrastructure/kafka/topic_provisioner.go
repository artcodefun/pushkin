package kafka

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"sync"
	"time"

	"uuid"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	contracts "github.com/superman/pushkin/internal/contracts/kafka"
)

const (
	pushkinTopicPartitions     int32 = 4
	topicMetadataRetryInterval       = 100 * time.Millisecond
	topicMetadataRetryTimeout        = 10 * time.Second
)

// TopicProvisioner creates the internal Kafka topics required by Pushkin.
// Their partition count is an immutable topology decision: changing it
// requires a coordinated topic migration, rather than a configuration update.
type TopicProvisioner struct {
	mu                sync.Mutex
	client            *kgo.Client
	admin             *kadm.Client
	workRetention     time.Duration
	progressRetention time.Duration
}

type TopicProvisionerParams struct {
	Brokers           []string
	WorkRetention     time.Duration
	ProgressRetention time.Duration
}

func NewTopicProvisioner(params TopicProvisionerParams) (*TopicProvisioner, error) {
	if len(params.Brokers) == 0 {
		return nil, fmt.Errorf("Kafka topic provisioner brokers must not be empty")
	}
	if params.WorkRetention <= 0 {
		return nil, fmt.Errorf("Kafka work retention must be positive")
	}
	if params.ProgressRetention <= 0 {
		return nil, fmt.Errorf("Kafka progress retention must be positive")
	}
	client, err := kgo.NewClient(kgo.SeedBrokers(params.Brokers...))
	if err != nil {
		return nil, fmt.Errorf("create Kafka topic provisioner client: %w", err)
	}
	return &TopicProvisioner{
		client:            client,
		admin:             kadm.NewClient(client),
		workRetention:     params.WorkRetention,
		progressRetention: params.ProgressRetention,
	}, nil
}

func (p *TopicProvisioner) Close() {
	p.client.Close()
}

// EnsureSystemTopics creates Pushkin's fixed pipeline topics when absent.
func (p *TopicProvisioner) EnsureSystemTopics(ctx context.Context) error {
	if err := p.ensure(ctx, p.workTopicConfig(),
		contracts.TopicCampaignBatchedRun,
		contracts.TopicCampaignBatchedSourceBatchFanout,
		contracts.TopicCampaignInlineRun,
		contracts.TopicCampaignInlineFanout,
	); err != nil {
		return err
	}
	if err := p.ensure(ctx, p.progressTopicConfig(), contracts.TopicCampaignProgress); err != nil {
		return err
	}
	if err := p.ensure(ctx, p.statsTopicConfig(), contracts.TopicCampaignStats); err != nil {
		return err
	}
	return p.ensure(ctx, p.workTopicConfig(), contracts.TopicNotificationAccepted)
}

// EnsureChannelTopics creates the delivery and retry topics for one Channel
// when absent. Repeated calls from different pods are safe.
func (p *TopicProvisioner) EnsureChannelTopics(ctx context.Context, channelID uuid.UUID) error {
	topics := make([]contracts.Topic, 0, 6)
	for _, priority := range [...]contracts.PriorityV1{
		contracts.PriorityCriticalV1,
		contracts.PriorityHighV1,
		contracts.PriorityNormalV1,
	} {
		topic, err := contracts.DeliveryTopic(priority, channelID)
		if err != nil {
			return err
		}
		topics = append(topics, topic)
	}
	for _, bucket := range [...]contracts.RetryBucketV1{
		contracts.RetryBucketOneMinuteV1,
		contracts.RetryBucketFiveMinutesV1,
		contracts.RetryBucketThirtyMinutesV1,
	} {
		topic, err := contracts.RetryTopic(bucket, channelID)
		if err != nil {
			return err
		}
		topics = append(topics, topic)
	}
	return p.ensure(ctx, p.workTopicConfig(), topics...)
}

func (p *TopicProvisioner) ensure(
	ctx context.Context,
	configs map[string]*string,
	topics ...contracts.Topic,
) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	names := make([]string, len(topics))
	for index, topic := range topics {
		names[index] = string(topic)
	}
	responses, err := p.admin.CreateTopics(ctx, pushkinTopicPartitions, -1, configs, names...)
	if err != nil {
		return fmt.Errorf("create Kafka topics: %w", err)
	}
	for _, name := range names {
		response, found := responses[name]
		if !found {
			return fmt.Errorf("create Kafka topic %q: missing response", name)
		}
		if response.Err == nil {
			if response.NumPartitions != pushkinTopicPartitions {
				return fmt.Errorf(
					"create Kafka topic %q with %d partitions; Pushkin requires %d",
					name,
					response.NumPartitions,
					pushkinTopicPartitions,
				)
			}
			continue
		}
		if !errors.Is(response.Err, kerr.TopicAlreadyExists) {
			return fmt.Errorf("create Kafka topic %q: %w", name, response.Err)
		}
	}
	// A successful CreateTopics response does not mean a topic leader and its
	// metadata are immediately visible to separate producer/consumer clients.
	// Wait for every requested topic, including newly created ones, before
	// workers begin producing to them.
	return p.validatePartitionCount(ctx, names)
}

func (p *TopicProvisioner) validatePartitionCount(ctx context.Context, names []string) error {
	timeout := time.NewTimer(topicMetadataRetryTimeout)
	defer timeout.Stop()

	for {
		err := p.validatePartitionCountOnce(ctx, names)
		if err == nil {
			return nil
		}
		if !isTopicMetadataPending(err) {
			return err
		}

		retry := time.NewTimer(topicMetadataRetryInterval)
		select {
		case <-ctx.Done():
			retry.Stop()
			return ctx.Err()
		case <-timeout.C:
			retry.Stop()
			return err
		case <-retry.C:
		}
	}
}

func (p *TopicProvisioner) validatePartitionCountOnce(ctx context.Context, names []string) error {
	details, err := p.admin.ListTopics(ctx, names...)
	if err != nil {
		return fmt.Errorf("describe Kafka topics: %w", err)
	}
	for _, name := range names {
		detail, found := details[name]
		if !found {
			return fmt.Errorf("describe Kafka topic %q: missing response", name)
		}
		if detail.Err != nil {
			return fmt.Errorf("describe Kafka topic %q: %w", name, detail.Err)
		}
		if int32(len(detail.Partitions)) != pushkinTopicPartitions {
			return fmt.Errorf(
				"Kafka topic %q has %d partitions; Pushkin requires %d; migrate the topic topology before startup",
				name,
				len(detail.Partitions),
				pushkinTopicPartitions,
			)
		}
		for partitionID, partition := range detail.Partitions {
			if partition.Err != nil {
				return fmt.Errorf(
					"describe Kafka topic %q partition %d: %w",
					name,
					partitionID,
					partition.Err,
				)
			}
			if partition.Leader < 0 || !slices.Contains(partition.ISR, partition.Leader) {
				return fmt.Errorf(
					"Kafka topic %q partition %d has no in-sync leader: %w",
					name,
					partitionID,
					kerr.LeaderNotAvailable,
				)
			}
		}
	}
	return nil
}

func isTopicMetadataPending(err error) bool {
	return errors.Is(err, kerr.UnknownTopicOrPartition) || errors.Is(err, kerr.LeaderNotAvailable)
}

func (p *TopicProvisioner) workTopicConfig() map[string]*string {
	return map[string]*string{
		"cleanup.policy": kadm.StringPtr("delete"),
		"retention.ms":   kadm.StringPtr(milliseconds(p.workRetention)),
	}
}

func (p *TopicProvisioner) progressTopicConfig() map[string]*string {
	return map[string]*string{
		"cleanup.policy": kadm.StringPtr("delete"),
		"retention.ms":   kadm.StringPtr(milliseconds(p.progressRetention)),
	}
}

func (p *TopicProvisioner) statsTopicConfig() map[string]*string {
	return map[string]*string{
		"cleanup.policy": kadm.StringPtr("compact,delete"),
		"retention.ms":   kadm.StringPtr(milliseconds(p.progressRetention)),
	}
}

func milliseconds(value time.Duration) string {
	return strconv.FormatInt(value.Milliseconds(), 10)
}
