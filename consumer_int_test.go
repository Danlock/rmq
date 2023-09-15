//go:build rabbit

package rmq_test

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/danlock/rmq"
	"github.com/danlock/rmq/internal"
	amqp "github.com/rabbitmq/amqp091-go"
)

func TestConsumer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logf := slog.Log

	rmqConn := rmq.ConnectWithAMQPConfig(ctx, rmq.ConnectConfig{Log: slog.Log}, os.Getenv("TEST_AMQP_URI"), amqp.Config{})

	baseConsConfig := rmq.ConsumerConfig{
		Exchanges: []rmq.ConsumerExchange{{
			Name:    "amq.topic",
			Kind:    amqp.ExchangeTopic,
			Durable: true,
		}, {
			Name:    "someotherxchg",
			Kind:    amqp.ExchangeTopic,
			Durable: true,
		}, {
			Name:    "djkhaled",
			Kind:    amqp.ExchangeTopic,
			Durable: true,
		}},
		ExchangeBindings: []rmq.ConsumerExchangeBinding{{
			Destination: "someotherxchg",
			RoutingKey:  "thisisunused",
			Source:      "djkhaled",
		}},
		Queue: rmq.ConsumerQueue{
			Name: "TestRMQConsumer " + time.Now().Format(time.RFC3339Nano),
			Args: amqp.Table{"x-expires": time.Minute.Milliseconds()},
		},
		QueueBindings: []rmq.ConsumerQueueBinding{
			{ExchangeName: "amq.topic", RoutingKey: "TestRMQConsumer"},
		},
		Consume: rmq.ConsumerConsume{
			Consumer: "TestRMQConsumerBase",
		},
		Log: logf,
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

	rmqBaseConsMessages := make(chan amqp.Delivery, 10)
	go baseConsumer.ConsumeConcurrently(ctx, rmqConn, 0, func(ctx context.Context, msg amqp.Delivery) {
		rmqBaseConsMessages <- msg
		_ = msg.Ack(false)
	})

	unreliableRMQPub := rmq.NewPublisher(ctx, rmqConn, rmq.PublisherConfig{DontConfirm: true, Log: logf})
	unreliableRMQPub.Publish(ctx, rmq.Publishing{Exchange: "amq.fanout"})
	rmqPub := rmq.NewPublisher(ctx, rmqConn, rmq.PublisherConfig{Log: logf})

	forceRedial := func() {
		amqpConn, err := rmqConn.CurrentConnection(ctx)
		if err != nil {
			t.Fatalf("failed to get rmqConn's current connection %v", err)
		}
		// close the current connection to force a redial
		amqpConn.CloseDeadline(time.Now().Add(time.Minute))
	}
	forceRedial()
	pubCtx, pubCancel := context.WithTimeout(ctx, 20*time.Second)
	defer pubCancel()
	wantedPub := rmq.Publishing{Exchange: baseConsConfig.QueueBindings[0].ExchangeName, RoutingKey: baseConsConfig.QueueBindings[0].RoutingKey}
	wantedPub.Body = []byte("TestRMQPublisher")

	pubCount := 10
	errChan := make(chan error, pubCount)
	for i := 0; i < pubCount; i++ {
		go func() {
			errChan <- rmqPub.PublishUntilAcked(pubCtx, 0, wantedPub)
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

func TestConsumer_Load(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute/2)
	defer cancel()

	logf := slog.Log
	internal.WrapLogFunc(&logf)

	rmqConn := rmq.ConnectWithAMQPConfig(ctx, rmq.ConnectConfig{Log: logf}, os.Getenv("TEST_AMQP_URI"), amqp.Config{})

	periodicallyCloseConn := func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
				amqpConn, _ := rmqConn.CurrentConnection(ctx)
				amqpConn.CloseDeadline(time.Now().Add(time.Minute))
			}
		}
	}
	go periodicallyCloseConn()

	baseName := fmt.Sprint("TestRMQConsumer_Load_Base_", rand.Uint64())
	baseConsConfig := rmq.ConsumerConfig{
		Exchanges: []rmq.ConsumerExchange{{
			Name:    "amq.topic",
			Kind:    amqp.ExchangeTopic,
			Durable: true,
		}},
		Queue: rmq.ConsumerQueue{
			Name: baseName,
			Args: amqp.Table{amqp.QueueTTLArg: time.Minute.Milliseconds()},
		},
		QueueBindings: []rmq.ConsumerQueueBinding{
			{ExchangeName: "amq.topic", RoutingKey: baseName},
		},
		Consume: rmq.ConsumerConsume{
			Consumer: baseName,
		},
		Log: logf,
	}
	// Here we Declare our Consumer's before Process is called in another goroutine, to ensure published messages will be placed on a queue.
	mqChan, err := rmq.NewConsumer(baseConsConfig).Declare(ctx, rmqConn)
	if err != nil {
		t.Fatalf(err.Error())
	}
	mqChan.Close()

	prefetchName := fmt.Sprint("TestRMQConsumer_Load_Prefetch_", rand.Uint64())
	prefetchConsConfig := baseConsConfig
	prefetchConsConfig.Qos.PrefetchCount = 10
	prefetchConsConfig.Queue.Name = prefetchName
	prefetchConsConfig.QueueBindings = []rmq.ConsumerQueueBinding{baseConsConfig.QueueBindings[0]}
	prefetchConsConfig.QueueBindings[0].RoutingKey = prefetchName

	mqChan, err = rmq.NewConsumer(prefetchConsConfig).Declare(ctx, rmqConn)
	if err != nil {
		t.Fatalf(err.Error())
	}
	mqChan.Close()

	consumers := []rmq.ConsumerConfig{baseConsConfig, prefetchConsConfig}
	publisher := rmq.NewPublisher(ctx, rmqConn, rmq.PublisherConfig{ /*Log: logf*/ })

	msgCount := 5_000
	errChan := make(chan error, (msgCount+1)*len(consumers))
	for _, c := range consumers {
		c := c
		go func() {
			ctx, cancel := context.WithCancel(ctx)
			receives := make(map[int]struct{})
			var msgRecv uint64
			var consMu sync.Mutex
			rmq.NewConsumer(c).ConsumeConcurrently(ctx, rmqConn, 0, func(ctx context.Context, msg amqp.Delivery) {
				if !c.Consume.AutoAck {
					defer msg.Ack(false)
				}
				indexBytes := bytes.TrimPrefix(msg.Body, []byte(c.QueueBindings[0].RoutingKey+":"))
				index, err := strconv.Atoi(string(indexBytes))
				consMu.Lock()
				defer consMu.Unlock()
				if err != nil {
					logf(ctx, slog.LevelError, "%s got %d msgs. Last msg %s", c.Queue.Name, msgRecv, string(msg.Body))
					errChan <- err
					cancel()
				} else {
					msgRecv++
					receives[index] = struct{}{}
					if len(receives) == msgCount {
						logf(ctx, slog.LevelError, "%s got %d msgs", c.Queue.Name, msgRecv)
						errChan <- nil
						cancel()
					}
				}
			})
		}()
		go func() {
			// Send half of the messages in parallel, then the rest serially
			// The listen() goroutine will serially execute all of these publishes anyway. Even the underlying *amqp.Channel will lock it's mutex on publishes.
			for i := 0; i < msgCount/2; i++ {
				go func(i int) {
					errChan <- publisher.PublishUntilAcked(ctx, 0, rmq.Publishing{
						Exchange:   c.QueueBindings[0].ExchangeName,
						RoutingKey: c.QueueBindings[0].RoutingKey,
						Mandatory:  true,
						Publishing: amqp.Publishing{
							Body: []byte(fmt.Sprint(c.QueueBindings[0].RoutingKey, ":", i)),
						},
					})
				}(i)
			}
			for i := msgCount / 2; i < msgCount; i++ {
				errChan <- publisher.PublishUntilAcked(ctx, 0, rmq.Publishing{
					Exchange:   c.QueueBindings[0].ExchangeName,
					RoutingKey: c.QueueBindings[0].RoutingKey,
					Mandatory:  true,
					Publishing: amqp.Publishing{
						Body: []byte(fmt.Sprint(c.QueueBindings[0].RoutingKey, ":", i)),
					},
				})
			}
		}()
	}

	for i := 0; i < cap(errChan); i++ {
		select {
		case <-ctx.Done():
			t.Fatalf("timed out after %d receives waiting for consumers to finish", i)
		case err := <-errChan:
			if err != nil {
				t.Fatalf("after %d receives got err from consumer %+v", i, err)
			}
		}
	}
}

// RabbitMQ behaviour around auto generated names and restricting declaring queues with amq prefix
func TestRMQConsumer_AutogeneratedQueueNames(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute/2)
	defer cancel()

	logf := slog.Log
	internal.WrapLogFunc(&logf)

	rmqConn := rmq.ConnectWithAMQPConfig(ctx, rmq.ConnectConfig{Log: logf}, os.Getenv("TEST_AMQP_URI"), amqp.Config{})

	// NewConsumer with an empty config will only declare a queue with an anonymous, RabbitMQ generated name
	cons := rmq.NewConsumer(rmq.ConsumerConfig{})
	mqChan, err := cons.Declare(ctx, rmqConn)
	if err != nil {
		t.Fatalf("failed to declare %v", err)
	}
	_ = mqChan.Close()

	amqpConn, err := rmqConn.CurrentConnection(ctx)
	if err != nil {
		t.Fatalf("failed getting current connection %v", err)
	}
	amqpConn.CloseDeadline(time.Now().Add(time.Minute))

	// Declaring this twice should work without errors
	mqChan, err = cons.Declare(ctx, rmqConn)
	if err != nil {
		t.Fatalf("failed to declare %v", err)
	}
	_ = mqChan.Close()
}
