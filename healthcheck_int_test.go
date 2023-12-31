//go:build rabbit

package rmq_test

import (
	"context"
	"errors"
	"log"
	"log/slog"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/danlock/rmq"
	amqp "github.com/rabbitmq/amqp091-go"
)

func TestHealthcheck(t *testing.T) {
	Example_healthcheck()
}

// Example shows how to write an unsophisticated healthcheck for a service intending to ensure it's rmq.Connection is capable of processing messages.
// Even though rmq.Connection reconnects on errors, there can always be unforeseen networking/DNS/RNGesus issues
// that necessitate a docker/kubernetes healthcheck restarting the service when unhealthy.
// While this is useful as an example, it wouldn't be used on production for several reasons, only one of which is the lack of reuse of AMQP connections and AMQP channels.
func Example_healthcheck() {
	// Real applications should use a real context. If this healthcheck was called via HTTP request for example,
	// that HTTP request's context would be a good candidate.
	ctx, cancel := context.WithTimeout(context.TODO(), time.Minute)
	defer cancel()
	// If we want to use a different log library instead of log/slog.Log, wrap the function instead.
	// If call depth is being logged, add to it so it doesn't just print this log function.
	// Here we use log instead of slog
	customLog := func(ctx context.Context, level slog.Level, msg string, _ ...any) {
		log.Printf("[%s] trace_id=%v msg="+msg, level, ctx.Value("your_embedded_trace_id"))
	}
	commonCfg := rmq.Args{Log: customLog}
	// Create an AMQP topology for our healthcheck, which uses a temporary exchange.
	// Design goals of danlock/rmq include reducing the amount of naked booleans in function signatures.
	topology := rmq.Topology{
		Args:          commonCfg,
		Exchanges:     []rmq.Exchange{{Name: "healthcheck", Kind: amqp.ExchangeDirect, AutoDelete: true}},
		Queues:        []rmq.Queue{{Name: "healthcheck", AutoDelete: true}},
		QueueBindings: []rmq.QueueBinding{{QueueName: "healthcheck", ExchangeName: "healthcheck"}},
	}
	// danlock/rmq best practice is including your applications topology in your ConnectConfig
	cfg := rmq.ConnectArgs{Args: commonCfg, Topology: topology}
	// RabbitMQ best practice is to pub and sub on different AMQP connections to avoid TCP backpressure causing issues with message consumption.
	pubRMQConn := rmq.ConnectWithURLs(ctx, cfg, os.Getenv("TEST_AMQP_URI"))
	subRMQConn := rmq.ConnectWithURLs(ctx, cfg, os.Getenv("TEST_AMQP_URI"))

	// A rudimentary healthcheck of a rmq.Connection is to ensure it can get a Channel, but we can do better
	_, err := subRMQConn.MustChannel(ctx)
	if err != nil {
		panic("couldn't get a channel")
	}

	rmqCons := rmq.NewConsumer(subRMQConn, rmq.ConsumerArgs{
		Args:  commonCfg,
		Queue: topology.Queues[0],
		Qos:   rmq.Qos{PrefetchCount: 10},
	})
	// Now we have a RabbitMQ queue with messages incoming on the deliveries channel, even if the network flakes.
	deliveries := rmqCons.Consume(ctx)

	rmqPub := rmq.NewPublisher(ctx, pubRMQConn, rmq.PublisherArgs{Args: commonCfg})
	// Now we have an AMQP publisher that can sends messages with at least once delivery.
	// Generate "unique" messages for our healthchecker to check later
	baseMsg := rmq.Publishing{Exchange: topology.Exchanges[0].Name, Mandatory: true}
	msgOne := baseMsg
	msgOne.Body = []byte(time.Now().String())
	msgTwo := baseMsg
	msgTwo.Body = []byte(time.Now().String())

	pubCtx, pubCtxCancel := context.WithTimeoutCause(ctx, 10*time.Second, errors.New("He's dead, Jim"))
	defer pubCtxCancel()

	conf, err := rmqPub.PublishUntilConfirmed(pubCtx, 0, msgOne)
	if err != nil {
		panic("uh oh, context timed out?")
	}
	// PublishUntilConfirmed only returns once the amqp.DeferredConfirmation is Done(),
	// so you can check Acked() without fear that the return value is simply telling you that it's not Done() yet.
	if !conf.Acked() {
		panic("uh oh, nacked")
	}
	// PublishUntilAcked resends on nacks. A Healthcheck may want to do that instead, since restarting a service whenever a nack happens probably won't help.
	if err = rmqPub.PublishUntilAcked(ctx, 0, msgTwo); err != nil {
		panic("uh oh, context timed out?")
	}

	// Now that we've sent, make sure we can receive.
	for i := 0; i < 2; i++ {
		select {
		case <-time.After(time.Second):
			panic("where's my message?")
		case msg := <-deliveries:
			if !reflect.DeepEqual(msg.Body, msgOne.Body) && !reflect.DeepEqual(msg.Body, msgTwo.Body) {
				panic("realistically this would probably be an error with another instance using this healthcheck simultaneously. Prevent this with an unique exchange or topic exchange with unique routing keys.")
			}
		}
	}

	// We sent and received 2 messages, so we're probably healthy enough to survive until the next docker/kubernetes health check.
}
