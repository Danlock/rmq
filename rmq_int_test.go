//go:build rabbit

package rmq_test

import (
	"context"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/danlock/rmq"
	"github.com/danlock/rmq/internal"
	amqp "github.com/rabbitmq/amqp091-go"
)

func TestRMQPubSub(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logf := internal.LogTillCtx(t, ctx)

	rmqConn := rmq.ConnectWithAMQPConfig(ctx, rmq.ConnectConfig{Logf: logf}, os.Getenv("TEST_AMQP_URI"), amqp.Config{})

	rmqCons := rmq.NewConsumer(rmq.ConsumerConfig{
		Exchange: rmq.ConsumerExchange{
			Name: "amq.topic",
			Kind: amqp.ExchangeTopic,
		},
		Queue: rmq.ConsumerQueue{
			Name:       "TestRMQPubSub" + time.Now().String(),
			AutoDelete: true,
		},
		Bindings: []rmq.ConsumerBinding{
			{ExchangeName: "amq.topic", RoutingKey: "TestRMQPubSub"},
		},
		Consume: rmq.ConsumerConsume{
			Consumer: "TestRMQPubSub",
		},

		Logf: logf,
	})

	wantedPub := rmq.Publishing{
		Exchange: "amq.topic",
		Publishing: amqp.Publishing{
			Body: []byte("TestRMQPubSub"),
		},
	}
	t.Parallel()
	t.Run("Consumer", func(t *testing.T) {
		t.Parallel()
		rmqCons.Process(ctx, rmqConn, func(ctx context.Context, msg amqp.Delivery) {
			if !reflect.DeepEqual(msg.Body, wantedPub.Body) {
				t.Fatalf("Consumer got unwanted publishing!")
			}
		})
	})

	rmqPub := rmq.NewPublisher(ctx, rmqConn, rmq.PublisherConfig{Logf: logf})

	pubCtx, pubCancel := context.WithTimeout(ctx, time.Minute)
	defer pubCancel()
	err := rmqPub.PublishUntilConfirmed(pubCtx, time.Minute, false, wantedPub)
	if err != nil {
		t.Fatalf("PublishUntilConfirmed failed with %v", err)
	}
}
