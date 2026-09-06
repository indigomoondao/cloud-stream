package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"cs-ra/internal/config"
	"cs-ra/internal/kafka"
	"cs-ra/internal/postgres"
	"cs-ra/internal/resource"

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
		logger.Warn(
			".env not found, using system environment variables",
		)
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

	// PostgreSQL

	pool, err := postgres.NewPool(
		ctx,
		cfg.DatabaseURL,
	)
	if err != nil {
		logger.Error("connect PostgreSQL", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	logger.Info("PostgreSQL connected")

	// Application

	unitOfWork := postgres.NewUnitOfWork(pool)
	resourceService := resource.NewService(unitOfWork)

	// Kafka

	kafkaClient, err := kafka.NewConsumerClient(
		ctx,
		cfg.KafkaBrokers,
		cfg.CultivationAdvancedTopic,
		cfg.ConsumerGroup,
	)
	if err != nil {
		logger.Error("connect Kafka", "error", err)
		os.Exit(1)
	}
	defer kafkaClient.Close()

	logger.Info("Kafka connected", "brokers", cfg.KafkaBrokers)

	logger.Info(
		"Kafka consumer ready",
		"topic", cfg.CultivationAdvancedTopic,
		"group", cfg.ConsumerGroup,
	)

	dlqPublisher := kafka.NewCultivationDLQPublisher(
		kafkaClient,
		cfg.CultivationAdvancedDLQTopic,
	)

	consumer := kafka.NewCultivationConsumer(
		kafkaClient,
		resourceService,
		dlqPublisher,
		cfg.MaxProcessingAttempts,
		cfg.RetryBackoff,
	)

	logger.Info("resource allocation worker started")

	if err := consumer.Run(ctx); err != nil {
		logger.Error("run cultivation consumer", "error", err)
		os.Exit(1)
	}

	logger.Info("resource allocation worker stopped")
}
