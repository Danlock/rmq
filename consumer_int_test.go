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

func TestRMQConsumer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logf := internal.LogTillCtx(t, ctx)

	rmqConn := rmq.ConnectWithAMQPConfig(ctx, rmq.ConnectConfig{Logf: logf}, os.Getenv("TEST_AMQP_URI"), amqp.Config{})

	baseConsConfig := rmq.ConsumerConfig{
		Exchange: rmq.ConsumerExchange{
			Name:    "amq.topic",
			Kind:    amqp.ExchangeTopic,
			Durable: true,
		},
		Queue: rmq.ConsumerQueue{
			Name:       "TestRMQPublisher" + time.Now().String(),
			AutoDelete: true,
		},
		Bindings: []rmq.ConsumerBinding{
			{ExchangeName: "amq.topic", RoutingKey: "TestRMQConsumer"},
		},
		Consume: rmq.ConsumerConsume{
			Consumer: "TestRMQPublisherBase",
		},
		Qos: rmq.ConsumerQos{
			PrefetchCount: 10,
		},

		Logf: logf,
	}
	_ = rmq.NewConsumer(rmq.ConsumerConfig{})
	rmqBaseConsMessages := make(chan amqp.Delivery, 10)
	go rmq.NewConsumer(baseConsConfig).Process(ctx, rmqConn, func(ctx context.Context, msg amqp.Delivery) {
		rmqBaseConsMessages <- msg
		_ = msg.Ack(false)
	})

	unreliableRMQPub := rmq.NewPublisher(ctx, rmqConn, rmq.PublisherConfig{DontConfirm: true, Logf: logf})
	rmqPub := rmq.NewPublisher(ctx, rmqConn, rmq.PublisherConfig{MaxConcurrentPublishes: 2, Logf: logf})

	// forceRedial := func() {
	// 	amqpConn, err := rmqConn.CurrentConnection(ctx)
	// 	if err != nil {
	// 		t.Fatalf("failed to get rmqConn's current connection %v", err)
	// 	}
	// 	// close the current connection to force a redial
	// 	_ = amqpConn.Close()
	// }
	// forceRedial()
	pubCtx, pubCancel := context.WithTimeout(ctx, 10*time.Second)
	defer pubCancel()
	wantedPub := rmq.Publishing{Exchange: baseConsConfig.Bindings[0].ExchangeName, RoutingKey: baseConsConfig.Bindings[0].RoutingKey}
	wantedPub.Body = []byte("TestRMQPublisher")

	pubCount := 10
	errChan := make(chan error, pubCount)
	for i := 0; i < pubCount/2; i++ {
		go func() {
			errChan <- rmqPub.PublishUntilConfirmed(pubCtx, time.Minute, true, wantedPub)
		}()
	}
	for i := 0; i < pubCount/2; i++ {
		go func() {
			_, err := unreliableRMQPub.Publish(pubCtx, wantedPub)
			errChan <- err
		}()
	}
	for i := 0; i < pubCount; i++ {
		if err := <-errChan; err != nil {
			t.Fatalf("PublishUntilConfirmed returned unexpected error %v", err)
		}
	}
	for i := 0; i < pubCount; i++ {
		var msg amqp.Delivery
		select {
		case <-pubCtx.Done():
			t.Fatalf("timed out waiting for the %d published message", i)
		case msg = <-rmqBaseConsMessages:
		}
		if !reflect.DeepEqual(msg.Body, wantedPub.Body) {
			t.Fatalf("Received unexpected message %s", string(msg.Body))
		}
	}
}
