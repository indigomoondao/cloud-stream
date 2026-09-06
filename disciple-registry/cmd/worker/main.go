package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"cs-dr/internal/config"
	"cs-dr/internal/kafka"
	"cs-dr/internal/outbox"
	"cs-dr/internal/postgres"

	"github.com/joho/godotenv"
)

func main() {
	logger := slog.New(
		slog.NewJSONHandler(
			os.Stdout,
			&slog.HandlerOptions{Level: slog.LevelInfo},
		),
	)
	slog.SetDefault(logger)

	if err := godotenv.Load(); err != nil {
		logger.Debug(".env not found, using system environment variables")
	}

	cfg, err := config.LoadWorker()
	if err != nil {
		logger.Error("load worker config", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	pool, err := postgres.NewPool(ctx, cfg.DatabaseURL)
	if err != nil {
		logger.Error("connect PostgreSQL", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	kafkaClient, err := kafka.NewClient(
		ctx,
		cfg.KafkaBrokers,
	)
	if err != nil {
		logger.Error("connect Kafka", "error", err)
		os.Exit(1)
	}
	defer kafkaClient.Close()

	repository := postgres.NewOutboxRepository(pool)
	publisher := kafka.NewCultivationPublisher(
		kafkaClient,
		cfg.CultivationTopic,
	)
	relay := outbox.NewRelay(
		repository,
		publisher,
		logger,
		cfg.OutboxRelayBatchSize,
		cfg.OutboxRelayPollInterval,
	)

	if err := relay.Run(ctx); err != nil &&
		!errors.Is(err, context.Canceled) {
		logger.Error("outbox relay stopped with error", "error", err)
		os.Exit(1)
	}
}
