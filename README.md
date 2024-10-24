# rmq
![Coverage](https://img.shields.io/badge/Coverage-85.6%25-brightgreen)
[![Go Report Card](https://goreportcard.com/badge/github.com/danlock/rmq)](https://goreportcard.com/report/github.com/danlock/rmq)
[![Go Reference](https://pkg.go.dev/badge/github.com/danlock/rmq.svg)](https://pkg.go.dev/github.com/danlock/rmq)

An AMQP library for Go, built on top of amqp091.

[streadway/amqp](https://github.com/streadway/amqp), the library the RabbitMQ maintainers forked to [amqp-091](https://github.com/rabbitmq/amqp091-go), is a stable, thin client for communicating to RabbitMQ, but lacks many of the features present in RabbitMQ libraries from other languages. Many redialable AMQP connections have been reinvented in Go codebases everywhere.

This package attempts to provide a wrapper of useful features on top of amqp091, in the hopes of preventing at least one more unnecessary reinvention (other than itself!)

# Design Goals

- Minimal API that doesn't get in the way of lower level access. The amqp091.Connection is there if you need it. amqp-091 knowledge is more transferable since danlock/rmq builds on top of those concepts rather than encapsulating things it doesn't need to.

- Network aware message delivery. Networks fail so danlock/rmq uses context.Context and default timeouts wherever possible, and tries to redeliver across network failures, unlike amqp091-go.

- One dependency (rabbitmq/amqp091-go).

- Prioritize readability. This means no functions with 5 boolean args.

# Examples

Using an AMQP publisher to publish a message with at least once delivery, that retries for up to a minute on failures.

```
ctx, cancel := context.WithTimeout(context.TODO(), time.Minute)
defer cancel()
cfg := rmq.Args{Log: slog.Log}

rmqConn := rmq.ConnectWithURLs(ctx, rmq.ConnectArgs{Args: cfg}, os.Getenv("AMQP_URL_1"), os.Getenv("AMQP_URL_2"))

rmqPub := rmq.NewPublisher(ctx, rmqConn, rmq.PublisherArgs{Args: cfg})

msg := rmq.Publishing{Exchange: "amq.topic", RoutingKey: "somewhere", Mandatory: true}
msg.Body = []byte(`{"life": 42}`)

if err := rmqPub.PublishUntilAcked(ctx, time.Minute, msg); err != nil {
    return fmt.Errorf("PublishUntilAcked timed out because %w", err)
}
```

Using a reliable AMQP consumer that receives deliveries through transient network failures while processing work concurrently with bounded goroutines.

```
ctx, := context.TODO()
cfg := rmq.Args{Log: slog.Log}

rmqConn := rmq.ConnectWithAMQPConfig(ctx, rmq.ConnectArgs{Args: cfg}, os.Getenv("AMQP_URL"), amqp.Config{})

consCfg := rmq.ConsumerArgs{
    Args: cfg,
    Queue: rmq.Queue{Name: "q2d2", AutoDelete: true},
    Qos: rmq.Qos{PrefetchCount: 1000},
}

rmq.NewConsumer(rmqConn, consCfg).ConsumeConcurrently(ctx, 100, func(ctx context.Context, msg amqp.Delivery) {
    process(msg)
    handleAckErr(msg.Ack(false))
})
```

Creating an AMQP topology that is automatically applied on reconnections as seen in the Java and C# RabbitMQ client drivers.

```
ctx, := context.TODO()
cfg := rmq.Args{Log: slog.Log}

topology := rmq.Topology{
    Args: cfg,
    Exchanges:     []rmq.Exchange{{Name: "xchg", Kind: amqp.ExchangeDirect, AutoDelete: true}},
    Queues:        []rmq.Queue{{Name: "huehue", Durable: true, AutoDelete: true}},
    QueueBindings: []rmq.QueueBinding{{QueueName: "huehue", ExchangeName: "xchg"}},
}

// It may be desired to read your AMQP topology from disk as JSON or some other config format. rmq.Topology is a simple struct so it can be done like so.
// err := json.NewDecoder(topologyFile).Decode(&topology)
// topology.Args = cfg

rmqConn := rmq.ConnectWithURLs(ctx, rmq.ConnectArgs{Args: cfg, Topology: topology}, os.Getenv("AMQP_URL"))
```

Take a look at healthcheck_int_test.go for a more complete example of using all of danlock/rmq together, or hang_int_test.go for an example of danlock/rmq being more network-aware than amqp091-go.

# Logging

danlock/rmq sometimes handles errors by retrying instead of returning. In situations like this, danlock/rmq logs if you allow it to for easier debugging.

All classes accept a Log function pointer that can be ignored entirely, set easily with slog.Log, or wrapped around your favorite logging library.

Here is an example logrus wrapper. danlock/rmq only uses the predefined slog.Level's, and doesn't send any args.
```
    Args{
        Log: func(ctx context.Context, level slog.Level, msg string, _ ...any) {
            logruslevel, _ := logrus.ParseLevel(level.String())
            logrus.StandardLogger().WithContext(ctx).Logf(logruslevel, msg)
        }
    }
```