package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"cs-dr/internal/config"
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

	cfg, err := config.LoadAPI()
	if err != nil {
		logger.Error("load API config", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	// PostgreSQL

	pool, err := postgres.NewPool(ctx, cfg.DatabaseURL)
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

	address := ":" + cfg.HTTPPort

	server := &http.Server{
		Addr:    address,
		Handler: router,
	}

	serverErr := make(chan error, 1)

	go func() {
		logger.Info(
			"disciple registry listening",
			"address", "http://localhost:"+cfg.HTTPPort,
		)

		serverErr <- server.ListenAndServe()
	}()

	select {
	case err := <-serverErr:
		if !errors.Is(err, http.ErrServerClosed) {
			logger.Error("start HTTP server", "error", err)
			os.Exit(1)
		}

	case <-ctx.Done():
		logger.Info("shutdown signal received")

		shutdownCtx, cancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer cancel()

		if err := server.Shutdown(shutdownCtx); err != nil {
			logger.Error("shutdown HTTP server", "error", err)
			os.Exit(1)
		}

		logger.Info("disciple registry stopped")
	}
}
