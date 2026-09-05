package main

import (
	"context"
	"log/slog"
	"os"

	"cs-dr/internal/disciple"
	httptransport "cs-dr/internal/http"
	"cs-dr/internal/postgres"

	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"
)

func main() {
	logger := slog.New(
		slog.NewJSONHandler(
			os.Stdout,
			&slog.HandlerOptions{Level: slog.LevelInfo},
		),
	)

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

	httpPort := os.Getenv("HTTP_PORT")
	if httpPort == "" {
		logger.Error("HTTP_PORT is required")
		os.Exit(1)
	}

	ctx := context.Background()

	// PostgreSQL

	pool, err := postgres.NewPool(ctx, databaseURL)
	if err != nil {
		logger.Error("connect PostgreSQL", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	logger.Info("PostgreSQL connected")

	discipleRepository :=
		postgres.NewDiscipleRepository(pool)

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

	logger.Info(
		"disciple registry listening",
		"address", "http://localhost:"+httpPort,
	)

	if err := router.Run(address); err != nil {
		logger.Error("start HTTP server", "error", err)
		os.Exit(1)
	}
}
