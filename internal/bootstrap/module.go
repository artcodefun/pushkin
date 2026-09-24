package bootstrap

import (
	"context"
	"fmt"
	"time"

	httpapi "github.com/superman/pushkin/internal/interfaces/http"
)

// Module is Pushkin's single-process composition root. It owns secondary
// adapter lifetimes and supplies assembled use cases to runtime adapters.
type Module struct {
	Config     Config
	Adapters   *Adapters
	Commands   *Commands
	Queries    *Queries
	Services   *Services
	Workers    *Workers
	Telemetry  *Telemetry
	HTTPServer *httpapi.Server
}

func NewModule(config Config) (*Module, error) {
	telemetry, err := NewTelemetry(context.Background(), config)
	if err != nil {
		return nil, err
	}
	adapters, err := NewAdapters(config)
	if err != nil {
		_ = telemetry.Shutdown()
		return nil, err
	}
	servicesBundle := NewServices(adapters, config)
	commands := NewCommands(adapters, config)
	queries := NewQueries(adapters)
	return &Module{
		Config:    config,
		Adapters:  adapters,
		Commands:  commands,
		Queries:   queries,
		Services:  servicesBundle,
		Workers:   NewWorkers(adapters, commands, servicesBundle, config),
		Telemetry: telemetry,
		HTTPServer: httpapi.NewServer(httpapi.NewRouter(
			httpapi.Commands{
				Campaign:          commands.Campaign,
				Provider:          commands.Provider,
				MobileApplication: commands.MobileApplication,
				Channel:           commands.Channel,
				Tenant:            commands.Tenant,
				TenantAPIKey:      commands.TenantAPIKey,
				PushInstallation:  commands.PushInstallation,
			}, httpapi.Queries{
				Campaign:          queries.Campaign,
				Tenant:            queries.Tenant,
				Provider:          queries.Provider,
				MobileApplication: queries.MobileApplication,
				Channel:           queries.Channel,
			},
			adapters.TenantAPIKeyValidator,
			config.AdminMasterKey),
			config.HTTPAddress),
	}, nil
}

// Run checks dependencies, starts the static runtime workers, and waits until
// process cancellation or the first worker failure. It owns the lifecycle of
// all secondary adapters constructed by the module.
func (m *Module) Run(ctx context.Context) (runErr error) {
	defer m.Adapters.Close()
	defer func() {
		if err := m.Telemetry.Shutdown(); err != nil && runErr == nil {
			runErr = fmt.Errorf("shutdown metrics telemetry: %w", err)
		}
	}()
	if err := m.Adapters.CheckReadiness(ctx); err != nil {
		return fmt.Errorf("Pushkin dependencies are not ready: %w", err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	m.Workers.Start(runCtx)

	workerResult := make(chan error, 1)
	go func() { workerResult <- m.Workers.Wait() }()
	httpResult := make(chan error, 1)
	go func() { httpResult <- m.HTTPServer.Serve() }()

	var processErr error
	workersStopped := false
	select {
	case <-runCtx.Done():
	case err := <-workerResult:
		processErr = err
		workersStopped = true
	case err := <-httpResult:
		processErr = fmt.Errorf("serve HTTP: %w", err)
	}

	cancel()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer shutdownCancel()
	if err := m.HTTPServer.Shutdown(shutdownCtx); err != nil && processErr == nil {
		processErr = fmt.Errorf("shutdown HTTP: %w", err)
	}

	if !workersStopped {
		if err := <-workerResult; err != nil && processErr == nil {
			processErr = err
		}
	}

	return processErr
}
