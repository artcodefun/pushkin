package bootstrap

import (
	"encoding/base64"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	defaultSourceBatchMaxSize             = 5_000
	defaultDeliveryProcessingBatchSize    = 100
	defaultDeliveryMaxInFlight            = 500
	defaultCampaignStartingTimeout        = time.Minute
	defaultSchedulerInterval              = time.Second
	defaultSchedulerBatchSize             = 100
	defaultCampaignProgressBatchSize      = 100
	defaultCampaignStatsBatchSize         = 1_000
	defaultInlineCampaignRunBatchSize     = 100
	defaultInlineCampaignFanoutBatchSize  = 100
	defaultChannelProvisioningInterval    = time.Second
	defaultKafkaWorkRetention             = 7 * 24 * time.Hour
	defaultKafkaProgressRetention         = 14 * 24 * time.Hour
	defaultTelemetryMetricsExportInterval = 10 * time.Second
)

// Config contains the process-wide settings required to construct Pushkin.
// It is parsed once at the composition root; application and domain code do
// not read environment variables.
type Config struct {
	PostgresDSN                    string
	HTTPAddress                    string
	KafkaBrokers                   []string
	InstanceID                     string
	RedisAddress                   string
	CredentialsCipherKey           []byte
	APIKeyHashPepper               []byte
	AdminMasterKey                 string
	SourceBatchMaxSize             int
	DeliveryProcessingBatchSize    int
	DeliveryMaxInFlight            int
	CampaignStartingTimeout        time.Duration
	SchedulerInterval              time.Duration
	SchedulerBatchSize             int
	CampaignProgressBatchSize      int
	CampaignStatsBatchSize         int
	InlineCampaignRunBatchSize     int
	InlineCampaignFanoutBatchSize  int
	ChannelProvisioningInterval    time.Duration
	KafkaWorkRetention             time.Duration
	KafkaProgressRetention         time.Duration
	UseTestSender                  bool
	TestSenderURL                  string
	TelemetryOTLPEndpoint          string
	TelemetryServiceName           string
	TelemetryMetricsExportInterval time.Duration
}

// LoadConfigFromEnv parses and validates the environment configuration for
// the current process.
func LoadConfigFromEnv() (Config, error) {
	return loadConfig(os.Getenv)
}

func loadConfig(getenv func(string) string) (Config, error) {
	postgresDSN, err := requiredEnv(getenv, "PUSHKIN_POSTGRES_DSN")
	if err != nil {
		return Config{}, err
	}
	httpAddress := strings.TrimSpace(getenv("PUSHKIN_HTTP_ADDRESS"))
	if httpAddress == "" {
		httpAddress = ":8080"
	}
	brokers, err := commaSeparatedEnv(getenv, "PUSHKIN_KAFKA_BROKERS")
	if err != nil {
		return Config{}, err
	}
	instanceID, err := requiredEnv(getenv, "PUSHKIN_INSTANCE_ID")
	if err != nil {
		return Config{}, err
	}
	redisAddress, err := requiredEnv(getenv, "PUSHKIN_REDIS_ADDRESS")
	if err != nil {
		return Config{}, err
	}
	schedulerInterval, err := positiveDurationEnv(getenv, "PUSHKIN_SCHEDULER_INTERVAL", defaultSchedulerInterval)
	if err != nil {
		return Config{}, err
	}
	schedulerBatchSize, err := positiveIntEnv(getenv, "PUSHKIN_SCHEDULER_BATCH_SIZE", defaultSchedulerBatchSize)
	if err != nil {
		return Config{}, err
	}
	campaignProgressBatchSize, err := positiveIntEnv(
		getenv,
		"PUSHKIN_CAMPAIGN_PROGRESS_BATCH_SIZE",
		defaultCampaignProgressBatchSize,
	)
	if err != nil {
		return Config{}, err
	}
	campaignStatsBatchSize, err := positiveIntEnv(
		getenv,
		"PUSHKIN_CAMPAIGN_STATS_BATCH_SIZE",
		defaultCampaignStatsBatchSize,
	)
	if err != nil {
		return Config{}, err
	}
	inlineCampaignRunBatchSize, err := positiveIntEnv(
		getenv,
		"PUSHKIN_INLINE_CAMPAIGN_RUN_BATCH_SIZE",
		defaultInlineCampaignRunBatchSize,
	)
	if err != nil {
		return Config{}, err
	}
	inlineCampaignFanoutBatchSize, err := positiveIntEnv(
		getenv,
		"PUSHKIN_INLINE_CAMPAIGN_FANOUT_BATCH_SIZE",
		defaultInlineCampaignFanoutBatchSize,
	)
	if err != nil {
		return Config{}, err
	}
	channelProvisioningInterval, err := positiveDurationEnv(
		getenv,
		"PUSHKIN_CHANNEL_PROVISIONING_INTERVAL",
		defaultChannelProvisioningInterval,
	)
	if err != nil {
		return Config{}, err
	}
	cipherKey, err := base64KeyEnv(getenv, "PUSHKIN_CREDENTIALS_CIPHER_KEY_BASE64")
	if err != nil {
		return Config{}, err
	}
	apiKeyHashPepper, err := requiredEnv(getenv, "PUSHKIN_API_KEY_HASH_PEPPER")
	if err != nil {
		return Config{}, err
	}
	adminMasterKey, err := requiredEnv(getenv, "PUSHKIN_ADMIN_MASTER_KEY")
	if err != nil {
		return Config{}, err
	}
	sourceBatchMaxSize, err := positiveIntEnv(getenv, "PUSHKIN_SOURCE_BATCH_MAX_SIZE", defaultSourceBatchMaxSize)
	if err != nil {
		return Config{}, err
	}
	deliveryBatchSize, err := positiveIntEnv(getenv, "PUSHKIN_DELIVERY_PROCESSING_BATCH_SIZE", defaultDeliveryProcessingBatchSize)
	if err != nil {
		return Config{}, err
	}
	deliveryMaxInFlight, err := positiveIntEnv(getenv, "PUSHKIN_DELIVERY_MAX_IN_FLIGHT", defaultDeliveryMaxInFlight)
	if err != nil {
		return Config{}, err
	}
	startingTimeout, err := positiveDurationEnv(getenv, "PUSHKIN_CAMPAIGN_STARTING_TIMEOUT", defaultCampaignStartingTimeout)
	if err != nil {
		return Config{}, err
	}
	workRetention, err := positiveDurationEnv(getenv, "PUSHKIN_KAFKA_WORK_RETENTION", defaultKafkaWorkRetention)
	if err != nil {
		return Config{}, err
	}
	progressRetention, err := positiveDurationEnv(
		getenv,
		"PUSHKIN_KAFKA_PROGRESS_RETENTION",
		defaultKafkaProgressRetention,
	)
	if err != nil {
		return Config{}, err
	}
	useTestSender, err := boolEnv(getenv, "PUSHKIN_USE_TEST_SENDER", false)
	if err != nil {
		return Config{}, err
	}
	testSenderURL := strings.TrimSpace(getenv("PUSHKIN_TEST_SENDER_URL"))
	if useTestSender && testSenderURL == "" {
		return Config{}, fmt.Errorf("PUSHKIN_TEST_SENDER_URL must not be empty when PUSHKIN_USE_TEST_SENDER is true")
	}
	telemetryExportInterval, err := positiveDurationEnv(
		getenv,
		"PUSHKIN_TELEMETRY_METRICS_EXPORT_INTERVAL",
		defaultTelemetryMetricsExportInterval,
	)
	if err != nil {
		return Config{}, err
	}
	telemetryServiceName := strings.TrimSpace(getenv("OTEL_SERVICE_NAME"))
	if telemetryServiceName == "" {
		telemetryServiceName = "pushkin"
	}

	return Config{
		PostgresDSN:                    postgresDSN,
		HTTPAddress:                    httpAddress,
		KafkaBrokers:                   brokers,
		InstanceID:                     instanceID,
		RedisAddress:                   redisAddress,
		CredentialsCipherKey:           cipherKey,
		APIKeyHashPepper:               []byte(apiKeyHashPepper),
		AdminMasterKey:                 adminMasterKey,
		SourceBatchMaxSize:             sourceBatchMaxSize,
		DeliveryProcessingBatchSize:    deliveryBatchSize,
		DeliveryMaxInFlight:            deliveryMaxInFlight,
		CampaignStartingTimeout:        startingTimeout,
		SchedulerInterval:              schedulerInterval,
		SchedulerBatchSize:             schedulerBatchSize,
		CampaignProgressBatchSize:      campaignProgressBatchSize,
		CampaignStatsBatchSize:         campaignStatsBatchSize,
		InlineCampaignRunBatchSize:     inlineCampaignRunBatchSize,
		InlineCampaignFanoutBatchSize:  inlineCampaignFanoutBatchSize,
		ChannelProvisioningInterval:    channelProvisioningInterval,
		KafkaWorkRetention:             workRetention,
		KafkaProgressRetention:         progressRetention,
		UseTestSender:                  useTestSender,
		TestSenderURL:                  testSenderURL,
		TelemetryOTLPEndpoint:          strings.TrimSpace(getenv("OTEL_EXPORTER_OTLP_ENDPOINT")),
		TelemetryServiceName:           telemetryServiceName,
		TelemetryMetricsExportInterval: telemetryExportInterval,
	}, nil
}

func boolEnv(getenv func(string) string, name string, fallback bool) (bool, error) {
	value := strings.TrimSpace(getenv(name))
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return false, fmt.Errorf("%s must be a boolean", name)
	}
	return parsed, nil
}

func requiredEnv(getenv func(string) string, name string) (string, error) {
	value := strings.TrimSpace(getenv(name))
	if value == "" {
		return "", fmt.Errorf("%s must not be empty", name)
	}
	return value, nil
}

func commaSeparatedEnv(getenv func(string) string, name string) ([]string, error) {
	value, err := requiredEnv(getenv, name)
	if err != nil {
		return nil, err
	}
	parts := strings.Split(value, ",")
	brokers := make([]string, 0, len(parts))
	for _, part := range parts {
		broker := strings.TrimSpace(part)
		if broker == "" {
			return nil, fmt.Errorf("%s contains an empty broker", name)
		}
		brokers = append(brokers, broker)
	}
	return brokers, nil
}

func base64KeyEnv(getenv func(string) string, name string) ([]byte, error) {
	value, err := requiredEnv(getenv, name)
	if err != nil {
		return nil, err
	}
	key, err := base64.StdEncoding.DecodeString(value)
	if err != nil {
		return nil, fmt.Errorf("decode %s: %w", name, err)
	}
	if len(key) != 32 {
		return nil, fmt.Errorf("%s must decode to exactly 32 bytes", name)
	}
	return key, nil
}

func positiveIntEnv(getenv func(string) string, name string, fallback int) (int, error) {
	value := strings.TrimSpace(getenv(name))
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer", name)
	}
	return parsed, nil
}

func positiveDurationEnv(getenv func(string) string, name string, fallback time.Duration) (time.Duration, error) {
	value := strings.TrimSpace(getenv(name))
	if value == "" {
		return fallback, nil
	}
	parsed, err := time.ParseDuration(value)
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("%s must be a positive duration", name)
	}
	return parsed, nil
}
