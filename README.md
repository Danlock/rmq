# rmq
![Coverage](https://img.shields.io/badge/Coverage-90.5%25-brightgreen)

An AMQP library for Go, built on top of amqp091.

streadway/amqp, the library the RabbitMQ authors forked to amqp091, is a stable, thin client for communicating to RabbitMQ, but lacks many of the features present in RabbitMQ libraries from other languages. Many redialable AMQP connections have been reinvented in Go codebases everywhere.

This package attempts to provide a wrapper of useful features on top of amqp091, in the hopes of preventing at least one more unnecessary reinvention.

# Logging

This library has functionality that runs in different goroutines than the caller and tends to handle errors by retrying instead of returning. When the library encounters an error in a situation like this, it logs if you let it to facilitate easier debugging.

All classes accept a Log function pointer that can be ignored entirely, set easily with slog.Log, or set with a wrapper around your favorite logging library.

Here is an example logrus wrapper. danlock/rmq only uses the predefined slog.Level's, and doesn't send any args.

    PublisherConfig{
        Log: func(ctx context.Context, level slog.Level, msg string, args ...any) {
            logruslevel, _ := logrus.ParseLevel(level.String())
            logrus.StandardLogger().WithContext(ctx).Logf(logruslevel, msg, args...)
        }
    }
