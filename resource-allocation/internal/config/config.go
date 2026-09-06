package config

import (
	"fmt"
	"time"

	"github.com/caarlos0/env/v11"
)

type WorkerConfig struct {
	DatabaseURL                 string        `env:"DATABASE_URL,required"`
	KafkaBrokers                []string      `env:"KAFKA_BROKERS,required" envSeparator:","`
	CultivationAdvancedTopic    string        `env:"KAFKA_CULTIVATION_ADVANCED_TOPIC,required"`
	CultivationAdvancedDLQTopic string        `env:"KAFKA_CULTIVATION_ADVANCED_DLQ_TOPIC,required"`
	ConsumerGroup               string        `env:"KAFKA_CONSUMER_GROUP,required"`
	MaxProcessingAttempts       int           `env:"KAFKA_CONSUMER_MAX_PROCESSING_ATTEMPTS,required"`
	RetryBackoff                time.Duration `env:"KAFKA_CONSUMER_RETRY_BACKOFF,required"`
}

func LoadWorker() (WorkerConfig, error) {
	var cfg WorkerConfig

	if err := env.Parse(&cfg); err != nil {
		return WorkerConfig{}, fmt.Errorf("parse worker config: %w", err)
	}

	if cfg.MaxProcessingAttempts <= 0 {
		return WorkerConfig{}, fmt.Errorf(
			"consumer max processing attempts must be > 0",
		)
	}

	if cfg.RetryBackoff < 0 {
		return WorkerConfig{}, fmt.Errorf(
			"consumer retry backoff must be >= 0",
		)
	}

	return cfg, nil
}
