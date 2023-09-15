# rmq
![Coverage](https://img.shields.io/badge/Coverage-87.0%25-brightgreen)
[![Go Report Card](https://goreportcard.com/badge/github.com/danlock/rmq)](https://goreportcard.com/report/github.com/danlock/rmq)
[![Go Reference](https://pkg.go.dev/badge/github.com/danlock/rmq.svg)](https://pkg.go.dev/github.com/danlock/rmq)

An AMQP library for Go, built on top of amqp091.

streadway/amqp, the library the RabbitMQ authors forked to amqp091, is a stable, thin client for communicating to RabbitMQ, but lacks many of the features present in RabbitMQ libraries from other languages. Many redialable AMQP connections have been reinvented in Go codebases everywhere.

This package attempts to provide a wrapper of useful features on top of amqp091, in the hopes of preventing at least one more unnecessary reinvention.

# Design Goals

- Minimal API that doesn't get in the way of lower level access. The amqp091.Connection or Channel is always there if you need it.

- Network aware message delivery. Infra can fail so danlock/rmq uses context.Context wherever possible.

- As few dependencies as possible.

# Logging

danlock/rmq sometimes handles errors by retrying instead of returning. In situations like this, danlock/rmq logs if you allow it to for easier debugging.

All classes accept a Log function pointer that can be ignored entirely, set easily with slog.Log, or wrapped around your favorite logging library.

Here is an example logrus wrapper. danlock/rmq only uses the predefined slog.Level's, and doesn't send any args.

    PublisherConfig{
        Log: func(ctx context.Context, level slog.Level, msg string, args ...any) {
            logruslevel, _ := logrus.ParseLevel(level.String())
            logrus.StandardLogger().WithContext(ctx).Logf(logruslevel, msg, args...)
        }
    }
