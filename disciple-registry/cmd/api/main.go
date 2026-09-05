package main

import (
	"context"
	"log"
	"os"
	"strings"

	"cs-dr/internal/disciple"
	httptransport "cs-dr/internal/http"
	kafkaadapter "cs-dr/internal/kafka"
	"cs-dr/internal/postgres"

	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"
)

func main() {
	if err := godotenv.Load(); err != nil {
		log.Println(".env not found, using system environment variables")
	}

	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		log.Fatal("DATABASE_URL is required")
	}

	httpPort := os.Getenv("HTTP_PORT")
	if httpPort == "" {
		log.Fatal("HTTP_PORT is required")
	}

	kafkaBrokersValue := os.Getenv("KAFKA_BROKERS")
	if kafkaBrokersValue == "" {
		log.Fatal("KAFKA_BROKERS is required")
	}

	cultivationTopic := os.Getenv(
		"KAFKA_CULTIVATION_ADVANCED_TOPIC",
	)
	if cultivationTopic == "" {
		log.Fatal(
			"KAFKA_CULTIVATION_ADVANCED_TOPIC is required",
		)
	}

	kafkaBrokers := strings.Split(
		kafkaBrokersValue,
		",",
	)

	ctx := context.Background()

	// PostgreSQL

	pool, err := postgres.NewPool(ctx, databaseURL)
	if err != nil {
		log.Fatalf("connect PostgreSQL: %v", err)
	}
	defer pool.Close()

	log.Println("PostgreSQL connected")

	discipleRepository :=
		postgres.NewDiscipleRepository(pool)

	// Kafka

	kafkaClient, err := kafkaadapter.NewClient(
		ctx,
		kafkaBrokers,
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
		"Kafka publisher ready topic=%s",
		cultivationTopic,
	)

	// Application service

	discipleService := disciple.NewService(
		discipleRepository,
	)

	// HTTP

	discipleHandler :=
		httptransport.NewDiscipleHandler(
			discipleService,
		)

	router := gin.Default()

	httptransport.RegisterRoutes(
		router,
		discipleHandler,
	)

	address := ":" + httpPort

	log.Printf(
		"disciple registry listening on http://localhost:%s",
		httpPort,
	)

	if err := router.Run(address); err != nil {
		log.Fatalf("start HTTP server: %v", err)
	}
}
