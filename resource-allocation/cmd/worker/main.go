package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

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

	kafkaCultivationAdvancedTopic := os.Getenv(
		"KAFKA_CULTIVATION_ADVANCED_TOPIC",
	)
	if kafkaCultivationAdvancedTopic == "" {
		logger.Error(
			"KAFKA_CULTIVATION_ADVANCED_TOPIC is required",
		)
		os.Exit(1)
	}

	kafkaCultivationDLQTopic := os.Getenv(
		"KAFKA_CULTIVATION_ADVANCED_DLQ_TOPIC",
	)
	if kafkaCultivationDLQTopic == "" {
		logger.Error(
			"KAFKA_CULTIVATION_ADVANCED_DLQ_TOPIC is required",
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

	kafkaConsumerMaxProcessingAttemptsValue := os.Getenv(
		"KAFKA_CONSUMER_MAX_PROCESSING_ATTEMPTS",
	)
	if kafkaConsumerMaxProcessingAttemptsValue == "" {
		logger.Error(
			"KAFKA_CONSUMER_MAX_PROCESSING_ATTEMPTS is required",
		)
		os.Exit(1)
	}

	kafkaConsumerMaxProcessingAttempts, err := strconv.Atoi(
		kafkaConsumerMaxProcessingAttemptsValue,
	)
	if err != nil {
		logger.Error(
			"invalid KAFKA_CONSUMER_MAX_PROCESSING_ATTEMPTS",
			"error", err,
		)
		os.Exit(1)
	}

	kafkaConsumerRetryBackoffValue := os.Getenv(
		"KAFKA_CONSUMER_RETRY_BACKOFF",
	)
	if kafkaConsumerRetryBackoffValue == "" {
		logger.Error(
			"KAFKA_CONSUMER_RETRY_BACKOFF is required",
		)
		os.Exit(1)
	}

	retryBackoff, err := time.ParseDuration(
		kafkaConsumerRetryBackoffValue,
	)
	if err != nil {
		logger.Error(
			"invalid KAFKA_CONSUMER_RETRY_BACKOFF",
			"error", err,
		)
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

	kafkaClient, err := kafka.NewConsumerClient(
		ctx,
		kafkaBrokers,
		kafkaCultivationAdvancedTopic,
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
		"topic", kafkaCultivationAdvancedTopic,
		"group", kafkaConsumerGroup,
	)

	dlqPublisher := kafka.NewCultivationDLQPublisher(
		kafkaClient,
		kafkaCultivationDLQTopic,
	)

	consumer := kafka.NewCultivationConsumer(
		kafkaClient,
		resourceService,
		dlqPublisher,
		kafkaConsumerMaxProcessingAttempts,
		retryBackoff,
	)

	logger.Info("resource allocation worker started")

	if err := consumer.Run(ctx); err != nil {
		logger.Error("run cultivation consumer", "error", err)
		os.Exit(1)
	}

	logger.Info("resource allocation worker stopped")
}
