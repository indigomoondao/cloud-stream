package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

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

	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		logger.Error("DATABASE_URL is required")
		os.Exit(1)
	}

	kafkaBrokersValue := os.Getenv("KAFKA_BROKERS")
	if kafkaBrokersValue == "" {
		logger.Error("KAFKA_BROKERS is required")
		os.Exit(1)
	}

	cultivationTopic := os.Getenv(
		"KAFKA_CULTIVATION_ADVANCED_TOPIC",
	)
	if cultivationTopic == "" {
		logger.Error(
			"KAFKA_CULTIVATION_ADVANCED_TOPIC is required",
		)
		os.Exit(1)
	}

	batchSizeValue := os.Getenv("OUTBOX_RELAY_BATCH_SIZE")
	if batchSizeValue == "" {
		logger.Error("OUTBOX_RELAY_BATCH_SIZE is required")
		os.Exit(1)
	}

	batchSize, err := strconv.Atoi(batchSizeValue)
	if err != nil {
		logger.Error(
			"invalid outbox relay batch size",
			"value",
			batchSizeValue,
			"error",
			err,
		)
		os.Exit(1)
	}

	pollIntervalValue := os.Getenv("OUTBOX_RELAY_POLL_INTERVAL")
	if pollIntervalValue == "" {
		logger.Error("OUTBOX_RELAY_POLL_INTERVAL is required")
		os.Exit(1)
	}

	pollInterval, err := time.ParseDuration(pollIntervalValue)
	if err != nil {
		logger.Error(
			"invalid outbox relay poll interval",
			"value",
			pollIntervalValue,
			"error",
			err,
		)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	pool, err := postgres.NewPool(ctx, databaseURL)
	if err != nil {
		logger.Error("connect PostgreSQL", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	kafkaClient, err := kafka.NewClient(
		ctx,
		strings.Split(kafkaBrokersValue, ","),
	)
	if err != nil {
		logger.Error("connect Kafka", "error", err)
		os.Exit(1)
	}
	defer kafkaClient.Close()

	repository := postgres.NewOutboxRepository(pool)
	publisher := kafka.NewCultivationPublisher(
		kafkaClient,
		cultivationTopic,
	)
	relay := outbox.NewRelay(
		repository,
		publisher,
		logger,
		batchSize,
		pollInterval,
	)

	if err := relay.Run(ctx); err != nil &&
		!errors.Is(err, context.Canceled) {
		logger.Error("outbox relay stopped with error", "error", err)
		os.Exit(1)
	}
}
