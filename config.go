package rmq

import (
	"context"
	"log/slog"
	"time"

	"github.com/danlock/rmq/internal"
)

// CommonConfig contains options shared by danlock/rmq classes.
type CommonConfig struct {
	// AMQPTimeout sets a timeout on AMQP operations. Defaults to 1 minute.
	AMQPTimeout time.Duration
	// Delay returns the delay between retry attempts. Defaults to FibonacciDelay.
	Delay func(attempt int) time.Duration
	// Log can be left nil, set with slog.Log or wrapped around your favorite logging library
	Log func(ctx context.Context, level slog.Level, msg string, args ...any)
}

func (cfg *CommonConfig) setDefaults() {
	if cfg.AMQPTimeout == 0 {
		cfg.AMQPTimeout = time.Minute
	}

	if cfg.Delay == nil {
		cfg.Delay = FibonacciDelay
	}

	internal.WrapLogFunc(&cfg.Log)
}

func FibonacciDelay(attempt int) time.Duration {
	if attempt < len(internal.FibonacciDurations) {
		return internal.FibonacciDurations[attempt]
	} else {
		return internal.FibonacciDurations[len(internal.FibonacciDurations)-1]
	}
}
