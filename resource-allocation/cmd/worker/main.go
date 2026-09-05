package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	kafkaadapter "cs-ra/internal/kafka"
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

	kafkaTopic := os.Getenv(
		"KAFKA_CULTIVATION_ADVANCED_TOPIC",
	)
	if kafkaTopic == "" {
		logger.Error(
			"KAFKA_CULTIVATION_ADVANCED_TOPIC is required",
		)
		os.Exit(1)
	}

	kafkaConsumerGroup := os.Getenv(
		"KAFKA_CONSUMER_GROUP",
	)
	if kafkaConsumerGroup == "" {
		logger.Error("KAFKA_CONSUMER_GROUP is required")
		os.Exit(1)
	}

	kafkaBrokers := strings.Split(
		kafkaBrokersValue,
		",",
	)

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	// PostgreSQL

	pool, err := postgres.NewPool(
		ctx,
		databaseURL,
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

	kafkaClient, err := kafkaadapter.NewConsumerClient(
		ctx,
		kafkaBrokers,
		kafkaTopic,
		kafkaConsumerGroup,
	)
	if err != nil {
		logger.Error("connect Kafka", "error", err)
		os.Exit(1)
	}
	defer kafkaClient.Close()

	logger.Info("Kafka connected", "brokers", kafkaBrokers)

	logger.Info(
		"Kafka consumer ready",
		"topic", kafkaTopic,
		"group", kafkaConsumerGroup,
	)

	consumer := kafkaadapter.NewCultivationConsumer(
		kafkaClient,
		resourceService,
	)

	logger.Info("resource allocation worker started")

	if err := consumer.Run(ctx); err != nil {
		logger.Error("run cultivation consumer", "error", err)
		os.Exit(1)
	}

	logger.Info("resource allocation worker stopped")
}
