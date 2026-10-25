package kafka

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/superman/pushkin/internal/application/ports"
	contracts "github.com/superman/pushkin/internal/contracts/kafka"
)

const (
	defaultSessionTimeout     = 45 * time.Second
	defaultTransactionTimeout = 30 * time.Second
)

// TransactionalConsumerParams configures one transactional consumer. InputTopic is
// empty only for a producer-only service such as the campaign scheduler.
// TransactionalID must be unique per concurrently running worker instance.
type TransactionalConsumerParams struct {
	Brokers            []string
	ConsumerGroup      string
	TransactionalID    string
	InputTopic         contracts.Topic
	SessionTimeout     time.Duration
	TransactionTimeout time.Duration
}

// TransactionalConsumer implements KafkaTransactionalConsumer for one internal input topic.
// Its methods must not be used concurrently. One runner owns one TransactionalConsumer
// instance and calls a finite application service method sequentially.
type TransactionalConsumer struct {
	batch       pollBatch
	terminalErr error
	session     *kgo.GroupTransactSession
	params      TransactionalConsumerParams
	inputTopic  contracts.Topic

	assignmentMu      sync.Mutex
	assignment        []ports.KafkaPartition
	assignmentReady   bool
	assignmentCancel  context.CancelFunc
	assignmentChanges chan struct{}
	sessionGeneration uint64
}

func NewTransactionalConsumer(params TransactionalConsumerParams) (*TransactionalConsumer, error) {
	if len(params.Brokers) == 0 {
		return nil, fmt.Errorf("Kafka brokers must not be empty")
	}
	if strings.TrimSpace(params.ConsumerGroup) == "" {
		return nil, fmt.Errorf("Kafka consumer group must not be empty")
	}
	if strings.TrimSpace(params.TransactionalID) == "" {
		return nil, fmt.Errorf("Kafka transactional ID must not be empty")
	}
	sessionTimeout := params.SessionTimeout
	if sessionTimeout == 0 {
		sessionTimeout = defaultSessionTimeout
	}
	transactionTimeout := params.TransactionTimeout
	if transactionTimeout == 0 {
		transactionTimeout = defaultTransactionTimeout
	}
	if sessionTimeout <= 0 || transactionTimeout <= 0 || transactionTimeout >= sessionTimeout {
		return nil, fmt.Errorf("Kafka transaction timeout must be positive and less than session timeout")
	}
	params.Brokers = append([]string(nil), params.Brokers...)
	params.SessionTimeout = sessionTimeout
	params.TransactionTimeout = transactionTimeout

	consumer := &TransactionalConsumer{
		inputTopic:        params.InputTopic,
		params:            params,
		assignmentChanges: make(chan struct{}, 1),
	}
	session, err := consumer.newSession()
	if err != nil {
		return nil, err
	}
	consumer.session = session
	return consumer, nil
}

func (l *TransactionalConsumer) newSession() (*kgo.GroupTransactSession, error) {
	l.assignmentMu.Lock()
	l.sessionGeneration++
	generation := l.sessionGeneration
	l.assignmentMu.Unlock()
	opts := []kgo.Opt{
		kgo.SeedBrokers(l.params.Brokers...),
		kgo.ConsumerGroup(l.params.ConsumerGroup),
		kgo.TransactionalID(l.params.TransactionalID),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.SessionTimeout(l.params.SessionTimeout),
		kgo.TransactionTimeout(l.params.TransactionTimeout),
		kgo.OnPartitionsAssigned(func(ctx context.Context, client *kgo.Client, assignments map[string][]int32) {
			l.onPartitionsAssigned(generation, ctx, client, assignments)
		}),
	}
	if l.params.InputTopic != "" {
		opts = append(opts, kgo.ConsumeTopics(string(l.params.InputTopic)))
	}
	session, err := kgo.NewGroupTransactSession(opts...)
	if err != nil {
		return nil, fmt.Errorf("create Kafka transaction session: %w", err)
	}
	return session, nil
}

func (l *TransactionalConsumer) Close() {
	if l.session != nil {
		l.session.CloseAllowingRebalance()
	}
}

// recreate replaces a transactional session after franz-go reports an error
// that leaves it unsafe to reuse. The consumer is used sequentially by one
// worker, so no operation is in flight while it is replaced.
func (l *TransactionalConsumer) recreate() error {
	oldSession := l.session
	l.assignmentMu.Lock()
	l.assignment = nil
	l.assignmentReady = false
	if l.assignmentCancel != nil {
		l.assignmentCancel()
		l.assignmentCancel = nil
	}
	l.assignmentMu.Unlock()
	if oldSession != nil {
		oldSession.CloseAllowingRebalance()
	}
	session, err := l.newSession()
	if err != nil {
		return err
	}
	l.session = session
	l.batch = pollBatch{}
	l.terminalErr = nil
	return nil
}

func (l *TransactionalConsumer) recreateIfTerminal() error {
	if l.terminalErr == nil {
		return nil
	}
	if err := l.recreate(); err != nil {
		return fmt.Errorf("recreate Kafka transactional consumer: %w", err)
	}
	return nil
}

// WaitForAssignment triggers the initial consumer-group join and returns the
// partitions assigned for this pipeline's configured input topic. If the
// bootstrap poll receives records while assignment is established, it rewinds
// them so application processing sees them on its first normal poll.
func (l *TransactionalConsumer) WaitForAssignment(ctx context.Context) ([]ports.KafkaPartition, error) {
	if err := l.recreateIfTerminal(); err != nil {
		return nil, err
	}
	l.assignmentMu.Lock()
	if l.assignmentReady {
		assignment := append([]ports.KafkaPartition(nil), l.assignment...)
		l.assignmentMu.Unlock()
		return assignment, nil
	}
	pollCtx, cancel := context.WithCancel(ctx)
	l.assignmentCancel = cancel
	l.assignmentMu.Unlock()
	defer func() {
		cancel()
		l.assignmentMu.Lock()
		if l.assignmentCancel != nil {
			l.assignmentCancel = nil
		}
		l.assignmentMu.Unlock()
	}()

	fetches := l.session.PollRecords(pollCtx, 1)
	rewindOffsets := make(ports.KafkaPartitionOffsets)
	for _, record := range fetches.Records() {
		if contracts.Topic(record.Topic) != l.inputTopic {
			return nil, fmt.Errorf("Kafka bootstrap record topic %q does not match configured input topic %q", record.Topic, l.inputTopic)
		}
		partition := ports.KafkaPartition{Topic: l.inputTopic, Partition: record.Partition}
		if current, found := rewindOffsets[partition]; !found || record.Offset < current {
			rewindOffsets[partition] = record.Offset
		}
	}
	if len(rewindOffsets) > 0 {
		if err := l.Seek(ctx, rewindOffsets); err != nil {
			return nil, fmt.Errorf("rewind Kafka bootstrap records: %w", err)
		}
	}

	l.assignmentMu.Lock()
	assignment := append([]ports.KafkaPartition(nil), l.assignment...)
	ready := l.assignmentReady
	l.assignmentMu.Unlock()
	if ready {
		return assignment, nil
	}
	if err := fetches.Err(); err != nil {
		return nil, fmt.Errorf("wait for Kafka assignment: %w", err)
	}
	return nil, fmt.Errorf("Kafka assignment was not established")
}

func (l *TransactionalConsumer) onPartitionsAssigned(generation uint64, _ context.Context, _ *kgo.Client, assignments map[string][]int32) {
	partitions := assignments[string(l.inputTopic)]
	assignment := make([]ports.KafkaPartition, 0, len(partitions))
	for _, partition := range partitions {
		assignment = append(assignment, ports.KafkaPartition{Topic: l.inputTopic, Partition: partition})
	}

	l.assignmentMu.Lock()
	if generation != l.sessionGeneration {
		l.assignmentMu.Unlock()
		return
	}
	wasAssigned := l.assignmentReady
	l.assignment = assignment
	l.assignmentReady = true
	if l.assignmentCancel != nil {
		l.assignmentCancel()
	}
	l.assignmentMu.Unlock()
	if wasAssigned {
		select {
		case l.assignmentChanges <- struct{}{}:
		default:
		}
	}
}

// AssignmentChanges reports assignments received after the initial group join.
// Notifications are coalesced: a worker need only rebuild its local state for
// the latest assignment before it resumes processing.
func (l *TransactionalConsumer) AssignmentChanges() <-chan struct{} {
	return l.assignmentChanges
}

func (l *TransactionalConsumer) Poll(ctx context.Context) (ports.KafkaRecord, bool, error) {
	records, err := l.PollMany(ctx, 1)
	if err != nil {
		return ports.KafkaRecord{}, false, err
	}
	if len(records) == 0 {
		return ports.KafkaRecord{}, false, nil
	}
	return records[0], true, nil
}

func (l *TransactionalConsumer) PollMany(ctx context.Context, limit int) (result []ports.KafkaRecord, err error) {
	defer func() {
		if err != nil {
			err = errors.Join(err, l.Rollback(ctx))
		}
	}()
	if err := l.recreateIfTerminal(); err != nil {
		return nil, err
	}
	if limit <= 0 {
		return nil, fmt.Errorf("Kafka poll limit must be positive")
	}

	if len(l.batch.offsets) != 0 {
		return nil, fmt.Errorf("Kafka batch is still pending")
	}
	fetches := l.session.PollRecords(ctx, limit)
	l.batch.capture(fetches.Records())
	if len(fetches.Records()) == 0 {
		l.batch.finish(l.session.Client())
	}
	if err := fetches.Err(); err != nil {
		return nil, fmt.Errorf("poll Kafka records: %w", err)
	}

	records := fetches.Records()
	decoded := make([]ports.KafkaRecord, 0, len(records))
	for _, record := range records {
		if contracts.Topic(record.Topic) != l.inputTopic {
			return nil, fmt.Errorf("Kafka record topic %q does not match configured input topic %q", record.Topic, l.inputTopic)
		}
		value, err := DecodeMessage(record.Value)
		if err != nil {
			return nil, fmt.Errorf("decode Kafka record at %s[%d] offset %d: %w", record.Topic, record.Partition, record.Offset, err)
		}
		decoded = append(decoded, ports.KafkaRecord{
			Value:  value,
			Offset: ports.KafkaOffset{Topic: l.inputTopic, Partition: record.Partition, Offset: record.Offset},
		})
	}
	return decoded, nil
}

func (l *TransactionalConsumer) Complete(ctx context.Context, messages []ports.OutboundKafkaMessage) error {
	if l.terminalErr != nil {
		return fmt.Errorf("Kafka transactional consumer is unavailable: %w", l.terminalErr)
	}
	if err := l.session.Begin(); err != nil {
		l.terminalErr = err
		return fmt.Errorf("begin Kafka transaction: %w", err)
	}
	if err := produce(ctx, l.session.ProduceSync, messages); err != nil {
		if _, abortErr := l.session.End(ctx, kgo.TryAbort); abortErr != nil {
			l.terminalErr = abortErr
			return fmt.Errorf("Kafka transaction failed: %w", errors.Join(err, abortErr))
		}
		var brokerErr *brokerProduceError
		if errors.As(err, &brokerErr) {
			l.terminalErr = err
		}
		return err
	}

	committed, err := l.session.End(ctx, kgo.TryCommit)
	if err != nil {
		l.terminalErr = err
		return fmt.Errorf("commit Kafka transaction: %w", err)
	}
	if !committed {
		l.terminalErr = errors.New("Kafka transaction aborted before commit")
		return l.terminalErr
	}
	l.batch.finish(l.session.Client())
	return nil
}

// brokerProduceError distinguishes a Kafka broker failure from local output
// validation. A broker failure can fence a transactional producer; local
// validation cannot, and the successfully aborted session remains reusable.
type brokerProduceError struct {
	err error
}

func (e *brokerProduceError) Error() string { return e.err.Error() }

func (e *brokerProduceError) Unwrap() error { return e.err }

func produce(ctx context.Context, produceSync func(context.Context, ...*kgo.Record) kgo.ProduceResults, messages []ports.OutboundKafkaMessage) error {
	if len(messages) == 0 {
		return nil
	}
	records := make([]*kgo.Record, 0, len(messages))
	for _, message := range messages {
		if message.Topic == "" {
			return fmt.Errorf("Kafka output topic must not be empty")
		}
		value, err := EncodeMessage(message.Value)
		if err != nil {
			return fmt.Errorf("encode Kafka message for topic %q: %w", message.Topic, err)
		}
		records = append(records, &kgo.Record{Topic: string(message.Topic), Key: message.Key, Value: value})
	}
	if err := produceSync(ctx, records...).FirstErr(); err != nil {
		return fmt.Errorf("produce Kafka messages: %w", &brokerProduceError{err: err})
	}
	return nil
}

func (l *TransactionalConsumer) Pause(_ context.Context, partitions []ports.KafkaPartition) error {
	l.session.Client().PauseFetchPartitions(kgoPartitions(partitions))
	return nil
}

func (l *TransactionalConsumer) PauseAll(_ context.Context) error {
	l.session.Client().PauseFetchTopics(string(l.inputTopic))
	return nil
}

func (l *TransactionalConsumer) Resume(_ context.Context, partitions []ports.KafkaPartition) error {
	l.session.Client().ResumeFetchPartitions(kgoPartitions(partitions))
	return nil
}

func (l *TransactionalConsumer) ResumeAll(_ context.Context) error {
	l.session.Client().ResumeFetchTopics(string(l.inputTopic))
	return nil
}

func (l *TransactionalConsumer) Seek(_ context.Context, offsets ports.KafkaPartitionOffsets) error {
	if len(offsets) == 0 {
		return nil
	}
	positions := make(map[string]map[int32]kgo.EpochOffset, len(offsets))
	for partition, offset := range offsets {
		if partition.Topic == "" || partition.Partition < 0 || offset < 0 {
			return fmt.Errorf("invalid Kafka seek position")
		}
		partitions := positions[string(partition.Topic)]
		if partitions == nil {
			partitions = make(map[int32]kgo.EpochOffset)
			positions[string(partition.Topic)] = partitions
		}
		partitions[partition.Partition] = kgo.EpochOffset{Offset: offset}
	}
	l.batch.seek(l.session.Client(), positions)
	return nil
}

func kgoPartitions(partitions []ports.KafkaPartition) map[string][]int32 {
	result := make(map[string][]int32)
	for _, partition := range partitions {
		result[string(partition.Topic)] = append(result[string(partition.Topic)], partition.Partition)
	}
	return result
}

func (l *TransactionalConsumer) Rollback(_ context.Context) error {
	if l.terminalErr != nil {
		return fmt.Errorf("rollback Kafka transaction: %w", l.terminalErr)
	}
	l.batch.rollback(l.session.Client())
	return nil
}
