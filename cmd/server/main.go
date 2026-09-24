package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/superman/pushkin/internal/bootstrap"
)

func main() {
	config, err := bootstrap.LoadConfigFromEnv()
	if err != nil {
		slog.Error("load configuration", "error", err)
		os.Exit(1)
	}
	module, err := bootstrap.NewModule(config)
	if err != nil {
		slog.Error("create module", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := module.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		slog.Error("run Pushkin", "error", err)
		os.Exit(1)
	}
}
