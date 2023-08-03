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
		Exchanges: []rmq.ConsumerExchange{{
			Name:    "amq.topic",
			Kind:    amqp.ExchangeTopic,
			Durable: true,
		}},
		Queue: rmq.ConsumerQueue{
			Name: "TestRMQConsumer " + time.Now().Format(time.RFC3339Nano),
			Args: amqp.Table{"x-expires": time.Minute.Milliseconds()},
		},
		Bindings: []rmq.ConsumerBinding{
			{ExchangeName: "amq.topic", RoutingKey: "TestRMQConsumer"},
		},
		Consume: rmq.ConsumerConsume{
			Consumer: "TestRMQConsumerBase",
		},
		ProcessSkipDeclare: false,
		Logf:               logf,
	}
	baseConsumer := rmq.NewConsumer(baseConsConfig)
	mqChan, err := baseConsumer.Declare(ctx, rmqConn)
	if err != nil {
		t.Fatalf("failed initial consumer setup")
	}
	defer mqChan.Close()

	passiveConsConfig := rmq.ConsumerConfig{Exchanges: []rmq.ConsumerExchange{baseConsConfig.Exchanges[0]}, Queue: baseConsConfig.Queue}
	passiveConsConfig.Exchanges[0].Passive = true
	passiveConsConfig.Queue.Passive = true
	passiveCons := rmq.NewConsumer(passiveConsConfig)
	passiveMQChan, err := passiveCons.Declare(ctx, rmqConn)
	if err != nil {
		t.Fatalf("failed initial passive consumer setup")
	}
	defer passiveMQChan.Close()

	canceledCtx, canceledCancel := context.WithCancel(ctx)
	canceledCancel()
	_, err = passiveCons.Declare(canceledCtx, rmqConn)
	if err == nil {
		t.Fatalf("Declare succeeded with a canceled context")
	}
	_, err = passiveCons.Consume(canceledCtx, mqChan)
	if err == nil {
		t.Fatalf("Consume succeeded with a canceled context")
	}

	rmqBaseConsMessages := make(chan amqp.Delivery, 10)
	go baseConsumer.Process(ctx, rmqConn, func(ctx context.Context, msg amqp.Delivery) {
		rmqBaseConsMessages <- msg
		_ = msg.Ack(false)
	})

	unreliableRMQPub := rmq.NewPublisher(ctx, rmqConn, rmq.PublisherConfig{DontConfirm: true, Logf: logf})
	unreliableRMQPub.Publish(ctx, rmq.Publishing{Exchange: "amq.fanout"})
	rmqPub := rmq.NewPublisher(ctx, rmqConn, rmq.PublisherConfig{MaxConcurrentPublishes: 4, Logf: logf})

	forceRedial := func() {
		amqpConn, err := rmqConn.CurrentConnection(ctx)
		if err != nil {
			t.Fatalf("failed to get rmqConn's current connection %v", err)
		}
		// close the current connection to force a redial
		_ = amqpConn.Close()
	}
	forceRedial()
	pubCtx, pubCancel := context.WithTimeout(ctx, 20*time.Second)
	defer pubCancel()
	wantedPub := rmq.Publishing{Exchange: baseConsConfig.Bindings[0].ExchangeName, RoutingKey: baseConsConfig.Bindings[0].RoutingKey}
	wantedPub.Body = []byte("TestRMQPublisher")

	pubCount := 10
	errChan := make(chan error, pubCount)
	for i := 0; i < pubCount; i++ {
		go func() {
			errChan <- rmqPub.PublishUntilConfirmed(pubCtx, time.Minute, true, wantedPub)
		}()
	}
	forceRedial()

	for i := 0; i < pubCount; i++ {
		if err := <-errChan; err != nil {
			t.Fatalf("PublishUntilConfirmed returned unexpected error %v", err)
		}
		if i%2 == 0 {
			forceRedial()
		}
	}
	for i := 0; i < pubCount; i++ {
		var msg amqp.Delivery
		select {
		case <-pubCtx.Done():
			t.Fatalf("timed out waiting for published message %d", i)
		case msg = <-rmqBaseConsMessages:
		}
		if !reflect.DeepEqual(msg.Body, wantedPub.Body) {
			t.Fatalf("Received unexpected message %s", string(msg.Body))
		}
	}
}

func TestRMQConsumer_Stress(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	logf := internal.LogTillCtx(t, ctx)

	rmqConn := rmq.ConnectWithAMQPConfig(ctx, rmq.ConnectConfig{Logf: logf}, os.Getenv("TEST_AMQP_URI"), amqp.Config{})

	baseConsConfig := rmq.ConsumerConfig{
		Exchanges: []rmq.ConsumerExchange{{
			Name:    "amq.topic",
			Kind:    amqp.ExchangeTopic,
			Durable: true,
		}},
		Queue: rmq.ConsumerQueue{
			Args: amqp.Table{"x-expires": time.Minute.Milliseconds()},
		},
		Bindings: []rmq.ConsumerBinding{
			{ExchangeName: "amq.topic", RoutingKey: "TestRMQConsumer_Stress"},
		},
		Consume: rmq.ConsumerConsume{
			Consumer: "TestRMQConsumerBase",
		},
		ProcessSkipDeclare: false,
		Logf:               logf,
	}
	baseCons := rmq.NewConsumer(baseConsConfig)
	mqChan, err := baseCons.Declare(ctx, rmqConn)
	if err != nil {
		t.Fatalf(err.Error())
	}
	mqChan.Close()

	skipConsConfig := baseConsConfig
	skipConsConfig.ProcessSkipDeclare = true
	skipCons := rmq.NewConsumer(skipConsConfig)
	mqChan, err = skipCons.Declare(ctx, rmqConn)
	if err != nil {
		t.Fatalf(err.Error())
	}
	mqChan.Close()

	lowPrefetchConsConfig := baseConsConfig
	lowPrefetchConsConfig.Qos.PrefetchCount = 10
	lowPrefetchCons := rmq.NewConsumer(lowPrefetchConsConfig)
	mqChan, err = lowPrefetchCons.Declare(ctx, rmqConn)
	if err != nil {
		t.Fatalf(err.Error())
	}
	mqChan.Close()

	msgCount := 100_000
	msgChan := make(chan amqp.Delivery, msgCount)

	consumeAndDeliver := func(cons *rmq.RMQConsumer) {
		delivered := 0
		cons.Process(ctx, rmqConn, func(ctx context.Context, msg amqp.Delivery) {
			msgChan <- msg
			delivered++
		})
		logf("Consumer deliver %d messages", delivered)
	}
	// Send msgCount messages over pubsubCount of consumers and publishers and see if everything processes smoothly
	pubsubCount := 5
	for i := 0; i < pubsubCount; i++ {
		go consumeAndDeliver(baseCons)
		go consumeAndDeliver(skipCons)
		go consumeAndDeliver(lowPrefetchCons)
	}

	testPub := rmq.Publishing{
		Exchange:   baseConsConfig.Bindings[0].ExchangeName,
		RoutingKey: baseConsConfig.Bindings[0].RoutingKey,
		Mandatory:  true,
	}
	testPub.Body = []byte(time.Now().Format(time.RFC3339))

	for i := 0; i < pubsubCount; i++ {
		pub := rmq.NewPublisher(ctx, rmqConn, rmq.PublisherConfig{Logf: logf})
		go func() {
			for i := 0; i < msgCount/pubsubCount; i++ {
				pub.PublishUntilConfirmed(ctx, time.Minute, true, testPub)
			}
		}()
	}

	for i := 0; i < msgCount; i++ {
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for message %d", i)
		case msg := <-msgChan:
			if !reflect.DeepEqual(testPub.Body, msg.Body) {
				t.Fatalf("message %d was unexpected %s", i, string(msg.Body))
			}
		}
	}
}
