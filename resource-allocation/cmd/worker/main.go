package main

import (
	"context"
	"log"
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
	if err := godotenv.Load(); err != nil {
		log.Println(
			".env not found, using system environment variables",
		)
	}

	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		log.Fatal("DATABASE_URL is required")
	}

	kafkaBrokersValue := os.Getenv("KAFKA_BROKERS")
	if kafkaBrokersValue == "" {
		log.Fatal("KAFKA_BROKERS is required")
	}

	kafkaTopic := os.Getenv(
		"KAFKA_CULTIVATION_ADVANCED_TOPIC",
	)
	if kafkaTopic == "" {
		log.Fatal(
			"KAFKA_CULTIVATION_ADVANCED_TOPIC is required",
		)
	}

	kafkaConsumerGroup := os.Getenv(
		"KAFKA_CONSUMER_GROUP",
	)
	if kafkaConsumerGroup == "" {
		log.Fatal("KAFKA_CONSUMER_GROUP is required")
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
		log.Fatalf("connect PostgreSQL: %v", err)
	}
	defer pool.Close()

	log.Println("PostgreSQL connected")

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
		log.Fatalf("connect Kafka: %v", err)
	}
	defer kafkaClient.Close()

	log.Printf(
		"Kafka connected brokers=%v",
		kafkaBrokers,
	)

	log.Printf(
		"Kafka consumer ready topic=%s group=%s",
		kafkaTopic,
		kafkaConsumerGroup,
	)

	consumer := kafkaadapter.NewCultivationConsumer(
		kafkaClient,
		resourceService,
	)

	log.Println("resource allocation worker started")

	if err := consumer.Run(ctx); err != nil {
		log.Fatalf(
			"run cultivation consumer: %v",
			err,
		)
	}

	log.Println("resource allocation worker stopped")
}
