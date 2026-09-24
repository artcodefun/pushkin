package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"golang.org/x/sync/errgroup"

	publickafka "github.com/superman/pushkin/api/kafka/v1"
	"github.com/superman/pushkin/internal/application/ports"
	"github.com/superman/pushkin/internal/application/services"
	contracts "github.com/superman/pushkin/internal/contracts/kafka"
	"github.com/superman/pushkin/internal/domain"
	infrakafka "github.com/superman/pushkin/internal/infrastructure/kafka"
	primarykafka "github.com/superman/pushkin/internal/interfaces/kafka"
)

const (
	initialWorkerRetryDelay = time.Second
	maximumWorkerRetryDelay = 30 * time.Second

	batchedCampaignRunConsumerGroup       = "pushkin.campaign-batched-run"
	batchedSourceFanoutConsumerGroup      = "pushkin.campaign-batched-source-batch-fanout"
	inlineCampaignRunConsumerGroup        = "pushkin.campaign-inline-run"
	inlineCampaignFanoutConsumerGroup     = "pushkin.campaign-inline-fanout"
	campaignProgressConsumerGroup         = "pushkin.campaign-progress"
	campaignStatsConsumerGroup            = "pushkin.campaign-stats"
	userEventsConsumerGroup               = "pushkin.user-events"
	batchedCampaignRunTransactionalRole   = "campaign-batched-run"
	batchedSourceFanoutTransactionalRole  = "campaign-batched-source-batch-fanout"
	inlineCampaignRunTransactionalRole    = "campaign-inline-run"
	inlineCampaignFanoutTransactionalRole = "campaign-inline-fanout"
	campaignProgressTransactionalRole     = "campaign-progress"
)

// Workers owns the process loops for Pushkin's static Kafka pipeline stages
// and scheduler. Start launches every loop under one cancellation scope. A
// failed runner is restarted with backoff so a temporary dependency failure
// does not stop unrelated workers or the HTTP server.
type Workers struct {
	runners           []workerRunner
	group             *errgroup.Group
	initialRetryDelay time.Duration
	maximumRetryDelay time.Duration
}

type workerRunner struct {
	name string
	run  func(context.Context) error
}

func NewWorkers(adapters *Adapters, commands *Commands, servicesBundle *Services, config Config) *Workers {
	userEvents := primarykafka.NewUserEventHandler(commands.User)
	return &Workers{runners: []workerRunner{
		{
			name: "user events",
			run: func(ctx context.Context) error {
				return runUserEvents(ctx, adapters.userEventsConsumer, userEvents)
			},
		},
		{
			name: "campaign scheduler",
			run: func(ctx context.Context) error {
				return runCampaignScheduler(ctx, servicesBundle.CampaignScheduler, config)
			},
		},
		{
			name: "batched campaign run coordinator",
			run: func(ctx context.Context) error {
				return runBatchedCampaignRunCoordinator(ctx, servicesBundle.BatchedCampaignRunCoordinator)
			},
		},
		{
			name: "batched source batch fanout",
			run: func(ctx context.Context) error {
				return runBatchedSourceBatchFanout(ctx, servicesBundle.BatchedSourceBatchFanout)
			},
		},
		{
			name: "inline campaign run coordinator",
			run: func(ctx context.Context) error {
				return runInlineCampaignRunCoordinator(ctx, servicesBundle.InlineCampaignRunCoordinator)
			},
		},
		{
			name: "inline campaign fanout",
			run: func(ctx context.Context) error {
				return runInlineCampaignFanout(ctx, servicesBundle.InlineCampaignFanout)
			},
		},
		{
			name: "campaign progress aggregator",
			run: func(ctx context.Context) error {
				return runCampaignProgressAggregator(ctx, adapters.campaignProgressConsumer, servicesBundle.CampaignProgressAggregator)
			},
		},
		{
			name: "campaign stats projection",
			run: func(ctx context.Context) error {
				return runCampaignStatsProjection(ctx, servicesBundle.CampaignStatsProjection)
			},
		},
		{
			name: "channel provisioning",
			run: func(ctx context.Context) error {
				return runChannelProvisioning(ctx, adapters.Channels, servicesBundle, config)
			},
		},
	}}
}

// Start starts every worker under its own retry supervisor. Call Wait to
// observe normal shutdown.
func (w *Workers) Start(ctx context.Context) {
	group, workerCtx := errgroup.WithContext(ctx)
	w.group = group
	for _, runner := range w.runners {
		runner := runner
		group.Go(func() error {
			return runWorkerWithRetry(
				workerCtx,
				runner,
				w.retryInitialDelay(),
				w.retryMaximumDelay(),
			)
		})
	}
}

func (w *Workers) retryInitialDelay() time.Duration {
	if w.initialRetryDelay > 0 {
		return w.initialRetryDelay
	}
	return initialWorkerRetryDelay
}

func (w *Workers) retryMaximumDelay() time.Duration {
	if w.maximumRetryDelay > 0 {
		return w.maximumRetryDelay
	}
	return maximumWorkerRetryDelay
}

func runWorkerWithRetry(
	ctx context.Context,
	runner workerRunner,
	initialDelay time.Duration,
	maximumDelay time.Duration,
) error {
	delay := initialDelay
	for ctx.Err() == nil {
		err := runner.run(ctx)
		if ctx.Err() != nil {
			return nil
		}
		if err == nil {
			err = errors.New("worker stopped unexpectedly")
		}
		slog.Error("Pushkin worker failed; retrying", "worker", runner.name, "retry_in", delay, "error", err)
		if !waitForWorkerRetry(ctx, delay) {
			return nil
		}
		delay = nextWorkerRetryDelay(delay, maximumDelay)
	}
	return nil
}

func waitForWorkerRetry(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func nextWorkerRetryDelay(current time.Duration, maximum time.Duration) time.Duration {
	if current >= maximum/2 {
		return maximum
	}
	return current * 2
}

func (w *Workers) Wait() error {
	if w.group == nil {
		return fmt.Errorf("workers have not been started")
	}
	return w.group.Wait()
}

func runUserEvents(
	ctx context.Context,
	consumer *infrakafka.ExternalConsumer,
	handler *primarykafka.UserEventHandler,
) error {
	for {
		record, found, err := consumer.Poll(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		if !found {
			continue
		}
		event, ok := record.Value.(publickafka.UserEventV1)
		if !ok {
			return fmt.Errorf("unexpected external Kafka message type %T in user events topic", record.Value)
		}
		if err := handler.Handle(ctx, record.Key, event); err != nil {
			return err
		}
		if err := consumer.Commit(ctx, record); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
	}
}

func runCampaignScheduler(
	ctx context.Context,
	scheduler *services.CampaignSchedulerService,
	config Config,
) error {
	for {
		result, err := scheduler.ProcessDue(ctx, config.SchedulerBatchSize)
		if err != nil {
			return err
		}
		if result.BatchLimitReached {
			continue
		}
		wait := time.NewTimer(config.SchedulerInterval)
		select {
		case <-ctx.Done():
			if !wait.Stop() {
				<-wait.C
			}
			return nil
		case <-wait.C:
		}
	}
}

func runBatchedCampaignRunCoordinator(ctx context.Context, service *services.BatchedCampaignRunCoordinatorService) error {
	for ctx.Err() == nil {
		if err := service.Process(ctx); err != nil {
			return err
		}
	}
	return nil
}

func runBatchedSourceBatchFanout(ctx context.Context, service *services.BatchedSourceBatchFanoutService) error {
	for ctx.Err() == nil {
		if err := service.Process(ctx); err != nil {
			return err
		}
	}
	return nil
}

func runInlineCampaignRunCoordinator(ctx context.Context, service *services.InlineCampaignRunCoordinatorService) error {
	for ctx.Err() == nil {
		if err := service.Process(ctx); err != nil {
			return err
		}
	}
	return nil
}

func runInlineCampaignFanout(ctx context.Context, service *services.InlineCampaignFanoutService) error {
	for ctx.Err() == nil {
		if err := service.Process(ctx); err != nil {
			return err
		}
	}
	return nil
}

func runCampaignProgressAggregator(
	ctx context.Context,
	pipeline *infrakafka.TransactionalConsumer,
	service *services.CampaignProgressAggregatorService,
) error {
	restored := false
	for ctx.Err() == nil {
		if !restored {
			if err := service.Restore(ctx); err != nil {
				if ctx.Err() != nil {
					return nil
				}
				return err
			}
			restored = true
		}
		select {
		case <-pipeline.AssignmentChanges():
			restored = false
			continue
		default:
		}

		processCtx, cancel := context.WithCancel(ctx)
		result := make(chan error, 1)
		go func() { result <- service.Process(processCtx) }()
		select {
		case err := <-result:
			cancel()
			if ctx.Err() != nil {
				return nil
			}
			if err != nil {
				return err
			}
		case <-pipeline.AssignmentChanges():
			cancel()
			<-result
			restored = false
		case <-ctx.Done():
			cancel()
			<-result
			return nil
		}
	}
	return nil
}

func runCampaignStatsProjection(ctx context.Context, service *services.CampaignStatsProjectionService) error {
	for ctx.Err() == nil {
		if err := service.Process(ctx); err != nil {
			return err
		}
	}
	return nil
}

type runningChannelWorker struct {
	cancel   context.CancelFunc
	stopping bool
}

type channelWorkerResult struct {
	channelID domain.ChannelID
	err       error
}

type channelProcessor struct {
	consumer *infrakafka.TransactionalConsumer
	name     string
	process  func(context.Context) error
}

// runChannelProvisioning provisions pending Channels, then reconciles active
// Channels from PostgreSQL with the local set of channel workers.
func runChannelProvisioning(
	ctx context.Context,
	channels ports.ChannelRepository,
	servicesBundle *Services,
	config Config,
) error {
	ticker := time.NewTicker(config.ChannelProvisioningInterval)
	defer ticker.Stop()
	results := make(chan channelWorkerResult)
	running := make(map[domain.ChannelID]runningChannelWorker)
	defer func() {
		for _, worker := range running {
			worker.cancel()
		}
		for len(running) > 0 {
			result := <-results
			delete(running, result.channelID)
		}
	}()

	reconcile := func() error {
		for {
			processed, err := servicesBundle.ChannelProvisioning.Process(ctx)
			if err != nil {
				return fmt.Errorf("provision channel: %w", err)
			}
			if !processed {
				break
			}
		}
		ids, err := channels.ListActiveIDs(ctx)
		if err != nil {
			return fmt.Errorf("list active channels: %w", err)
		}
		desired := make(map[domain.ChannelID]struct{}, len(ids))
		for _, channelID := range ids {
			desired[channelID] = struct{}{}
		}
		for channelID, worker := range running {
			if _, found := desired[channelID]; !found && !worker.stopping {
				worker.cancel()
				worker.stopping = true
				running[channelID] = worker
			}
		}
		for channelID := range desired {
			if _, found := running[channelID]; found {
				continue
			}
			workerCtx, cancel := context.WithCancel(ctx)
			running[channelID] = runningChannelWorker{cancel: cancel}
			go func() {
				results <- channelWorkerResult{
					channelID: channelID,
					err:       runChannelWorker(workerCtx, channelID, servicesBundle, config),
				}
			}()
		}
		return nil
	}

	if err := reconcile(); err != nil {
		return err
	}
	for {
		select {
		case result := <-results:
			worker, found := running[result.channelID]
			if !found {
				continue
			}
			delete(running, result.channelID)
			if ctx.Err() == nil && !worker.stopping {
				if result.err == nil {
					return fmt.Errorf("channel worker %s stopped unexpectedly", result.channelID)
				}
				return fmt.Errorf("channel worker %s: %w", result.channelID, result.err)
			}
		case <-ticker.C:
			if err := reconcile(); err != nil {
				return err
			}
		case <-ctx.Done():
			return nil
		}
	}
}

func runChannelWorker(
	ctx context.Context,
	channelID domain.ChannelID,
	servicesBundle *Services,
	config Config,
) error {
	processors, err := newChannelProcessors(channelID, servicesBundle, config)
	if err != nil {
		return err
	}
	defer closeChannelProcessors(processors)

	group, workerCtx := errgroup.WithContext(ctx)
	for _, processor := range processors {
		processor := processor
		group.Go(func() error {
			return runProcessWithRetry(
				workerCtx,
				processor.name,
				processor.process,
				initialWorkerRetryDelay,
				maximumWorkerRetryDelay,
			)
		})
	}
	return group.Wait()
}

func runProcessWithRetry(
	ctx context.Context,
	name string,
	process func(context.Context) error,
	initialDelay time.Duration,
	maximumDelay time.Duration,
) error {
	delay := initialDelay
	for ctx.Err() == nil {
		if err := process(ctx); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			slog.Error("Pushkin channel processor failed; retrying", "processor", name, "retry_in", delay, "error", err)
			if !waitForWorkerRetry(ctx, delay) {
				return nil
			}
			delay = nextWorkerRetryDelay(delay, maximumDelay)
			continue
		}
		delay = initialDelay
	}
	return nil
}

func newChannelProcessors(
	channelID domain.ChannelID,
	servicesBundle *Services,
	config Config,
) ([]channelProcessor, error) {
	processors := make([]channelProcessor, 0, 6)
	for _, priority := range [...]domain.Priority{
		domain.PriorityCritical,
		domain.PriorityHigh,
		domain.PriorityNormal,
	} {
		topic, err := contracts.DeliveryTopic(contracts.PriorityV1(priority), channelID)
		if err != nil {
			closeChannelProcessors(processors)
			return nil, err
		}
		consumer, err := newTransactionalConsumer(
			config,
			fmt.Sprintf("pushkin.delivery.%s.%s", priority, channelID),
			fmt.Sprintf("delivery.%s.%s", priority, channelID),
			topic,
		)
		if err != nil {
			closeChannelProcessors(processors)
			return nil, err
		}
		service, err := servicesBundle.NewDeliveryService(DeliveryServiceFactoryParams{
			ChannelID:     channelID,
			Priority:      priority,
			KafkaConsumer: consumer,
		})
		if err != nil {
			consumer.Close()
			closeChannelProcessors(processors)
			return nil, err
		}
		processors = append(processors, channelProcessor{
			consumer: consumer,
			name:     fmt.Sprintf("delivery %s for channel %s", priority, channelID),
			process:  service.Process,
		})
	}
	for _, bucket := range [...]contracts.RetryBucketV1{
		contracts.RetryBucketOneMinuteV1,
		contracts.RetryBucketFiveMinutesV1,
		contracts.RetryBucketThirtyMinutesV1,
	} {
		topic, err := contracts.RetryTopic(bucket, channelID)
		if err != nil {
			closeChannelProcessors(processors)
			return nil, err
		}
		consumer, err := newTransactionalConsumer(
			config,
			fmt.Sprintf("pushkin.retry.%s.%s", bucket, channelID),
			fmt.Sprintf("retry.%s.%s", bucket, channelID),
			topic,
		)
		if err != nil {
			closeChannelProcessors(processors)
			return nil, err
		}
		service, err := servicesBundle.NewRetryDeliveryService(RetryDeliveryServiceFactoryParams{
			ChannelID:     channelID,
			Bucket:        bucket,
			KafkaConsumer: consumer,
		})
		if err != nil {
			consumer.Close()
			closeChannelProcessors(processors)
			return nil, err
		}
		processors = append(processors, channelProcessor{
			consumer: consumer,
			name:     fmt.Sprintf("retry %s for channel %s", bucket, channelID),
			process:  service.Process,
		})
	}
	return processors, nil
}

func closeChannelProcessors(processors []channelProcessor) {
	for _, processor := range processors {
		processor.consumer.Close()
	}
}
