package bootstrap

import (
	"encoding/base64"
	"testing"
	"time"
)

func TestLoadConfigUsesDefaults(t *testing.T) {
	t.Parallel()
	config, err := loadConfig(testEnvironment(map[string]string{
		"PUSHKIN_POSTGRES_DSN":                  "postgres://pushkin:secret@localhost:5432/pushkin",
		"PUSHKIN_KAFKA_BROKERS":                 "kafka-1:9092, kafka-2:9092",
		"PUSHKIN_INSTANCE_ID":                   "test-instance",
		"PUSHKIN_REDIS_ADDRESS":                 "redis:6379",
		"PUSHKIN_CREDENTIALS_CIPHER_KEY_BASE64": base64.StdEncoding.EncodeToString(make([]byte, 32)),
		"PUSHKIN_API_KEY_HASH_PEPPER":           "test-api-key-pepper",
		"PUSHKIN_ADMIN_MASTER_KEY":              "test-admin-master-key",
	}))
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if config.SourceBatchMaxSize != defaultSourceBatchMaxSize ||
		config.DeliveryProcessingBatchSize != defaultDeliveryProcessingBatchSize ||
		config.DeliveryMaxInFlight != defaultDeliveryMaxInFlight ||
		config.CampaignStartingTimeout != defaultCampaignStartingTimeout ||
		config.SchedulerInterval != defaultSchedulerInterval ||
		config.SchedulerBatchSize != defaultSchedulerBatchSize ||
		config.CampaignProgressBatchSize != defaultCampaignProgressBatchSize ||
		config.CampaignStatsBatchSize != defaultCampaignStatsBatchSize ||
		config.InlineCampaignRunBatchSize != defaultInlineCampaignRunBatchSize ||
		config.InlineCampaignFanoutBatchSize != defaultInlineCampaignFanoutBatchSize ||
		config.ChannelProvisioningInterval != defaultChannelProvisioningInterval ||
		config.KafkaWorkRetention != defaultKafkaWorkRetention ||
		config.KafkaProgressRetention != defaultKafkaProgressRetention ||
		config.TelemetryMetricsExportInterval != defaultTelemetryMetricsExportInterval {
		t.Fatalf("unexpected defaults: %+v", config)
	}
	if len(config.KafkaBrokers) != 2 || config.KafkaBrokers[1] != "kafka-2:9092" {
		t.Fatalf("unexpected Kafka brokers: %v", config.KafkaBrokers)
	}
	if config.TelemetryOTLPEndpoint != "" || config.TelemetryServiceName != "pushkin" {
		t.Fatalf("unexpected default telemetry configuration: %+v", config)
	}
}

func TestLoadConfigRejectsInvalidValues(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		env  map[string]string
	}{
		{name: "missing PostgreSQL DSN", env: validConfigEnvironment()},
		{name: "missing instance ID", env: validConfigEnvironment()},
		{name: "invalid cipher key", env: withEnvironment(validConfigEnvironment(), "PUSHKIN_CREDENTIALS_CIPHER_KEY_BASE64", "not-base64")},
		{name: "wrong cipher key size", env: withEnvironment(validConfigEnvironment(), "PUSHKIN_CREDENTIALS_CIPHER_KEY_BASE64", base64.StdEncoding.EncodeToString(make([]byte, 31)))},
		{name: "empty Kafka broker", env: withEnvironment(validConfigEnvironment(), "PUSHKIN_KAFKA_BROKERS", "kafka-1:9092, ")},
		{name: "invalid batch size", env: withEnvironment(validConfigEnvironment(), "PUSHKIN_DELIVERY_PROCESSING_BATCH_SIZE", "0")},
		{name: "invalid inline run batch size", env: withEnvironment(validConfigEnvironment(), "PUSHKIN_INLINE_CAMPAIGN_RUN_BATCH_SIZE", "0")},
		{name: "invalid inline fanout batch size", env: withEnvironment(validConfigEnvironment(), "PUSHKIN_INLINE_CAMPAIGN_FANOUT_BATCH_SIZE", "0")},
		{name: "invalid campaign stats batch size", env: withEnvironment(validConfigEnvironment(), "PUSHKIN_CAMPAIGN_STATS_BATCH_SIZE", "0")},
		{name: "invalid telemetry export interval", env: withEnvironment(validConfigEnvironment(), "PUSHKIN_TELEMETRY_METRICS_EXPORT_INTERVAL", "never")},
		{name: "invalid starting timeout", env: withEnvironment(validConfigEnvironment(), "PUSHKIN_CAMPAIGN_STARTING_TIMEOUT", "never")},
		{name: "invalid test sender flag", env: withEnvironment(validConfigEnvironment(), "PUSHKIN_USE_TEST_SENDER", "perhaps")},
		{name: "missing test sender URL", env: withEnvironment(validConfigEnvironment(), "PUSHKIN_USE_TEST_SENDER", "true")},
	}
	delete(tests[0].env, "PUSHKIN_POSTGRES_DSN")
	delete(tests[1].env, "PUSHKIN_INSTANCE_ID")

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := loadConfig(testEnvironment(test.env)); err == nil {
				t.Fatal("load config must fail")
			}
		})
	}
}

func TestLoadConfigParsesOverrides(t *testing.T) {
	t.Parallel()
	env := validConfigEnvironment()
	env["PUSHKIN_SOURCE_BATCH_MAX_SIZE"] = "10"
	env["PUSHKIN_DELIVERY_PROCESSING_BATCH_SIZE"] = "20"
	env["PUSHKIN_DELIVERY_MAX_IN_FLIGHT"] = "30"
	env["PUSHKIN_CAMPAIGN_STARTING_TIMEOUT"] = "45s"
	env["PUSHKIN_SCHEDULER_INTERVAL"] = "2s"
	env["PUSHKIN_SCHEDULER_BATCH_SIZE"] = "40"
	env["PUSHKIN_CAMPAIGN_PROGRESS_BATCH_SIZE"] = "50"
	env["PUSHKIN_CAMPAIGN_STATS_BATCH_SIZE"] = "55"
	env["PUSHKIN_INLINE_CAMPAIGN_RUN_BATCH_SIZE"] = "60"
	env["PUSHKIN_INLINE_CAMPAIGN_FANOUT_BATCH_SIZE"] = "70"
	env["PUSHKIN_CHANNEL_PROVISIONING_INTERVAL"] = "3m"
	env["PUSHKIN_KAFKA_WORK_RETENTION"] = "48h"
	env["PUSHKIN_KAFKA_PROGRESS_RETENTION"] = "72h"
	env["PUSHKIN_USE_TEST_SENDER"] = "true"
	env["PUSHKIN_TEST_SENDER_URL"] = "http://fake-fcm:8081"
	env["OTEL_EXPORTER_OTLP_ENDPOINT"] = "telemetry:4317"
	env["OTEL_SERVICE_NAME"] = "pushkin-test"
	env["PUSHKIN_TELEMETRY_METRICS_EXPORT_INTERVAL"] = "5s"

	config, err := loadConfig(testEnvironment(env))
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if config.SourceBatchMaxSize != 10 || config.DeliveryProcessingBatchSize != 20 ||
		config.DeliveryMaxInFlight != 30 || config.CampaignStartingTimeout != 45*time.Second ||
		config.SchedulerInterval != 2*time.Second || config.SchedulerBatchSize != 40 ||
		config.CampaignProgressBatchSize != 50 || config.CampaignStatsBatchSize != 55 || config.ChannelProvisioningInterval != 3*time.Minute ||
		config.InlineCampaignRunBatchSize != 60 || config.InlineCampaignFanoutBatchSize != 70 ||
		config.KafkaWorkRetention != 48*time.Hour || config.KafkaProgressRetention != 72*time.Hour {
		t.Fatalf("unexpected overrides: %+v", config)
	}
	if !config.UseTestSender || config.TestSenderURL != "http://fake-fcm:8081" {
		t.Fatalf("unexpected test sender configuration: %+v", config)
	}
	if config.TelemetryOTLPEndpoint != "telemetry:4317" || config.TelemetryServiceName != "pushkin-test" ||
		config.TelemetryMetricsExportInterval != 5*time.Second {
		t.Fatalf("unexpected telemetry configuration: %+v", config)
	}
}

func validConfigEnvironment() map[string]string {
	return map[string]string{
		"PUSHKIN_POSTGRES_DSN":                  "postgres://pushkin:secret@localhost:5432/pushkin",
		"PUSHKIN_KAFKA_BROKERS":                 "kafka:9092",
		"PUSHKIN_INSTANCE_ID":                   "test-instance",
		"PUSHKIN_REDIS_ADDRESS":                 "redis:6379",
		"PUSHKIN_CREDENTIALS_CIPHER_KEY_BASE64": base64.StdEncoding.EncodeToString(make([]byte, 32)),
		"PUSHKIN_API_KEY_HASH_PEPPER":           "test-api-key-pepper",
		"PUSHKIN_ADMIN_MASTER_KEY":              "test-admin-master-key",
	}
}

func withEnvironment(env map[string]string, key, value string) map[string]string {
	copy := make(map[string]string, len(env)+1)
	for currentKey, currentValue := range env {
		copy[currentKey] = currentValue
	}
	copy[key] = value
	return copy
}

func testEnvironment(values map[string]string) func(string) string {
	return func(name string) string { return values[name] }
}
