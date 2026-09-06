package config

import (
	"fmt"
	"time"

	"github.com/caarlos0/env/v11"
)

type APIConfig struct {
	DatabaseURL string `env:"DATABASE_URL,required"`
	HTTPPort    string `env:"HTTP_PORT,required"`
}

type WorkerConfig struct {
	DatabaseURL             string        `env:"DATABASE_URL,required"`
	KafkaBrokers            []string      `env:"KAFKA_BROKERS,required" envSeparator:","`
	CultivationTopic        string        `env:"KAFKA_CULTIVATION_ADVANCED_TOPIC,required"`
	OutboxRelayBatchSize    int           `env:"OUTBOX_RELAY_BATCH_SIZE,required"`
	OutboxRelayPollInterval time.Duration `env:"OUTBOX_RELAY_POLL_INTERVAL,required"`
}

func LoadAPI() (APIConfig, error) {
	var cfg APIConfig

	if err := env.Parse(&cfg); err != nil {
		return APIConfig{}, fmt.Errorf("parse API config: %w", err)
	}

	return cfg, nil
}

func LoadWorker() (WorkerConfig, error) {
	var cfg WorkerConfig

	if err := env.Parse(&cfg); err != nil {
		return WorkerConfig{}, fmt.Errorf("parse worker config: %w", err)
	}

	if cfg.OutboxRelayBatchSize <= 0 {
		return WorkerConfig{}, fmt.Errorf(
			"outbox relay batch size must be > 0",
		)
	}

	if cfg.OutboxRelayPollInterval <= 0 {
		return WorkerConfig{}, fmt.Errorf(
			"outbox relay poll interval must be > 0",
		)
	}

	return cfg, nil
}
