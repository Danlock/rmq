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
	amqp "github.com/rabbitmq/amqp091-go"
)

func ForceRedial(ctx context.Context, rmqConn *rmq.Connection) error {
	amqpConn, err := rmqConn.CurrentConnection(ctx)
	if err != nil {
		return fmt.Errorf("rmqConn.CurrentConnection failed because %w", err)
	}
	// close the current connection to force a redial
	return amqpConn.CloseDeadline(time.Now().Add(time.Minute))
}

func TestConsumer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	slogLog := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug})).Log
	connectCfg := rmq.ConnectArgs{Args: rmq.Args{Log: slogLog}}
	rmqConn := rmq.ConnectWithURLs(ctx, connectCfg, "amqp://dont.exist", os.Getenv("TEST_AMQP_URI"))

	baseConsConfig := rmq.ConsumerArgs{
		Args: connectCfg.Args,
		Queue: rmq.Queue{
			Name: fmt.Sprintf("TestRMQConsumer.%p", t),
			Args: amqp.Table{amqp.QueueTTLArg: time.Minute.Milliseconds()},
		},
		Consume: rmq.Consume{
			Consumer: "TestConsumer",
		},
		Qos: rmq.Qos{
			PrefetchCount: 2000,
		},
	}

	baseConsumer := rmq.NewConsumer(rmqConn, baseConsConfig)

	canceledCtx, canceledCancel := context.WithCancel(ctx)
	canceledCancel()
	// ConsumeConcurrently should exit immediately on canceled contexts.
	baseConsumer.ConsumeConcurrently(canceledCtx, 0, nil)

	rmqBaseConsMessages := make(chan amqp.Delivery, 10)
	go baseConsumer.ConsumeConcurrently(ctx, 0, func(ctx context.Context, msg amqp.Delivery) {
		rmqBaseConsMessages <- msg
		_ = msg.Ack(false)
	})
	time.Sleep(time.Second / 10)
	unreliableRMQPub := rmq.NewPublisher(ctx, rmqConn, rmq.PublisherArgs{DontConfirm: true})
	unreliableRMQPub.Publish(ctx, rmq.Publishing{Exchange: "amq.fanout"})
	rmqPub := rmq.NewPublisher(ctx, rmqConn, rmq.PublisherArgs{Args: connectCfg.Args})

	ForceRedial(ctx, rmqConn)
	pubCtx, pubCancel := context.WithTimeout(ctx, 20*time.Second)
	defer pubCancel()

	wantedPub := rmq.Publishing{RoutingKey: baseConsConfig.Queue.Name}
	wantedPub.Body = []byte("TestRMQPublisher")

	pubCount := 3
	errChan := make(chan error, pubCount)
	for i := 0; i < pubCount; i++ {
		go func() {
			errChan <- rmqPub.PublishUntilAcked(pubCtx, 0, wantedPub)
		}()
	}
	ForceRedial(ctx, rmqConn)

	for i := 0; i < pubCount; i++ {
		if err := <-errChan; err != nil {
			t.Fatalf("PublishUntilAcked returned unexpected error %v", err)
		}
		if i%2 == 0 {
			ForceRedial(ctx, rmqConn)
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

	baseName := fmt.Sprint("TestRMQConsumer_Load_Base_", rand.Uint64())
	prefetchName := fmt.Sprint("TestRMQConsumer_Load_Prefetch_", rand.Uint64())
	baseCfg := rmq.Args{Log: logf}
	topology := rmq.Topology{
		Args: baseCfg,
		Queues: []rmq.Queue{{
			Name: baseName,
			Args: amqp.Table{amqp.QueueTTLArg: time.Minute.Milliseconds()},
		}, {
			Name: prefetchName,
			Args: amqp.Table{amqp.QueueTTLArg: time.Minute.Milliseconds()},
		}},
	}

	rmqConn := rmq.ConnectWithAMQPConfig(ctx, rmq.ConnectArgs{Args: baseCfg, Topology: topology}, os.Getenv("TEST_AMQP_URI"), amqp.Config{})

	periodicallyCloseConn := func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
				ForceRedial(ctx, rmqConn)
			}
		}
	}
	go periodicallyCloseConn()

	baseConsConfig := rmq.ConsumerArgs{
		Args:  baseCfg,
		Queue: topology.Queues[0],
		Consume: rmq.Consume{
			Consumer: baseName,
		},
	}

	prefetchConsConfig := baseConsConfig
	prefetchConsConfig.Queue = topology.Queues[1]
	prefetchConsConfig.Qos.PrefetchCount = 10
	prefetchConsConfig.Queue.Name = prefetchName

	consumers := []rmq.ConsumerArgs{baseConsConfig, prefetchConsConfig}
	publisher := rmq.NewPublisher(ctx, rmqConn, rmq.PublisherArgs{Args: baseCfg})

	msgCount := 5_000
	errChan := make(chan error, (msgCount/2+1)*len(consumers))
	for _, c := range consumers {
		c := c
		go func() {
			ctx, cancel := context.WithCancel(ctx)
			receives := make(map[int]struct{})
			var msgRecv uint64
			var consMu sync.Mutex
			rmq.NewConsumer(rmqConn, c).ConsumeConcurrently(ctx, 0, func(ctx context.Context, msg amqp.Delivery) {
				if !c.Consume.AutoAck {
					defer msg.Ack(false)
				}
				indexBytes := bytes.TrimPrefix(msg.Body, []byte(c.Queue.Name+":"))
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
			// Send half of the messages with an incredibly inefficient use of goroutines, and the rest in a PublishBatchUntilAcked.
			// Publishing all of this stuff in different goroutines should not cause any races.
			for i := 0; i < msgCount/2; i++ {
				go func(i int) {
					errChan <- publisher.PublishUntilAcked(ctx, 0, rmq.Publishing{
						RoutingKey: c.Queue.Name,
						Mandatory:  true,
						Publishing: amqp.Publishing{
							Body: []byte(fmt.Sprint(c.Queue.Name, ":", i)),
						},
					})
				}(i)
			}
			pubs := make([]rmq.Publishing, msgCount/2)
			for i := range pubs {
				pubs[i] = rmq.Publishing{
					RoutingKey: c.Queue.Name,
					Mandatory:  true,
					Publishing: amqp.Publishing{
						Body: []byte(fmt.Sprint(c.Queue.Name, ":", i+len(pubs))),
					},
				}
			}
			errChan <- publisher.PublishBatchUntilAcked(ctx, 0, pubs...)
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
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug})))
	baseCfg := rmq.Args{Log: slog.Log}
	rmqConn := rmq.ConnectWithAMQPConfig(ctx, rmq.ConnectArgs{Args: baseCfg}, os.Getenv("TEST_AMQP_URI"), amqp.Config{})

	// NewConsumer with an empty Queue.Name will declare a queue with a RabbitMQ generated name
	// This is useless unless the config also includes QueueBindings, since reconnections cause RabbitMQ to generate a different name anyway
	cons := rmq.NewConsumer(rmqConn, rmq.ConsumerArgs{
		Args: baseCfg,
		QueueBindings: []rmq.QueueBinding{
			{ExchangeName: "amq.fanout", RoutingKey: "TestRMQConsumer_AutogeneratedQueueNames"},
		},
		Qos: rmq.Qos{PrefetchCount: 1},
	})
	deliveries := cons.Consume(ctx)
	// Wait a sec for Consume to actually bring up the queue, since otherwise a published message could happen before a queue is declared.
	// danlock/rmq best practice to only use queues named in your Topology so you won't have to remember this.
	time.Sleep(time.Second / 3)
	amqpConn, err := rmqConn.CurrentConnection(ctx)
	if err != nil {
		t.Fatalf("failed getting current connection %v", err)
	}
	amqpConn.CloseDeadline(time.Now().Add(time.Minute))

	// Declaring again should work without errors, but it will create a different queue rather than consuming from the first one.
	// rmq.Consumer could remember the last queue name to consume from it again, but that wouldn't be reliable with auto-deleted or expiring queues.
	// It's simpler to disallow that use case by not making RabbitMQ generated queue names available from rmq.Consumer.
	secondDeliveries := cons.Consume(ctx)
	publisher := rmq.NewPublisher(ctx, rmqConn, rmq.PublisherArgs{Args: baseCfg, LogReturns: true})
	pubCount := 10
	time.Sleep(time.Second / 3)

	for i := 0; i < pubCount; i++ {
		go publisher.PublishUntilAcked(ctx, 0, rmq.Publishing{Exchange: "amq.fanout", RoutingKey: "TestRMQConsumer_AutogeneratedQueueNames", Mandatory: true})
	}

	for i := 0; i < pubCount; i++ {
		select {
		case msg := <-deliveries:
			msg.Ack(false)
		case msg := <-secondDeliveries:
			msg.Ack(false)
		case <-ctx.Done():
			t.Fatalf("timed out on delivery %d", i)
		}
	}
}
