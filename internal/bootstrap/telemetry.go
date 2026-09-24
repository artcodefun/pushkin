package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.37.0"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const telemetryShutdownTimeout = 10 * time.Second

// Telemetry owns Pushkin's process-wide metrics provider.
type Telemetry struct {
	shutdown func(context.Context) error
}

// NewTelemetry configures metrics export for the process. Without an OTLP
// endpoint OpenTelemetry keeps its default no-op meter provider, so local and
// production runs do not require a telemetry backend.
func NewTelemetry(ctx context.Context, config Config) (*Telemetry, error) {
	if config.TelemetryOTLPEndpoint == "" {
		return &Telemetry{shutdown: func(context.Context) error { return nil }}, nil
	}

	connection, err := grpc.NewClient(
		config.TelemetryOTLPEndpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("connect OTLP metrics exporter: %w", err)
	}
	exporter, err := otlpmetricgrpc.New(ctx, otlpmetricgrpc.WithGRPCConn(connection))
	if err != nil {
		_ = connection.Close()
		return nil, fmt.Errorf("create OTLP metrics exporter: %w", err)
	}
	res, err := resource.New(ctx, resource.WithAttributes(
		semconv.ServiceName(config.TelemetryServiceName),
		semconv.ServiceInstanceID(config.InstanceID),
	))
	if err != nil {
		_ = connection.Close()
		return nil, fmt.Errorf("create telemetry resource: %w", err)
	}
	provider := metric.NewMeterProvider(
		metric.WithReader(metric.NewPeriodicReader(exporter, metric.WithInterval(config.TelemetryMetricsExportInterval))),
		metric.WithResource(res),
	)
	otel.SetMeterProvider(provider)

	return &Telemetry{shutdown: func(shutdownCtx context.Context) error {
		return errors.Join(provider.Shutdown(shutdownCtx), connection.Close())
	}}, nil
}

// Shutdown flushes pending metrics without allowing telemetry to delay process
// termination indefinitely.
func (t *Telemetry) Shutdown() error {
	ctx, cancel := context.WithTimeout(context.Background(), telemetryShutdownTimeout)
	defer cancel()
	return t.shutdown(ctx)
}
