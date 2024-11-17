//go:build rabbit

package rmq_test

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/danlock/rmq"
	"github.com/danlock/rmq/internal/test"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/wagslane/go-rabbitmq"
	wagslane "github.com/wagslane/go-rabbitmq"
)

const benchNumPubs = 1000

func generatePublishings(num int, routingKey string) []rmq.Publishing {
	publishings := make([]rmq.Publishing, num)
	for i := range publishings {
		publishings[i] = rmq.Publishing{
			RoutingKey: routingKey,
			Mandatory:  true,
			Publishing: amqp.Publishing{
				Body: []byte(fmt.Sprintf("%d.%d", i, time.Now().UnixNano())),
			},
		}
	}
	return publishings
}

func BenchmarkPublishAndConsumeMany(b *testing.B) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	randSuffix := fmt.Sprintf("%d.%p", time.Now().UnixNano(), b)

	queueName := "BenchmarkPublishAndConsumeMany" + randSuffix
	baseCfg := rmq.Args{Log: slog.Log}
	topology := rmq.Topology{
		Args: baseCfg,
		Queues: []rmq.Queue{{
			Name: queueName,
			Args: amqp.Table{
				amqp.QueueTTLArg: time.Minute.Milliseconds(),
			},
		}},
	}

	subRMQConn := rmq.ConnectWithURLs(ctx, rmq.ConnectArgs{Args: baseCfg, Topology: topology}, os.Getenv("TEST_AMQP_URI"))
	pubRMQConn := rmq.ConnectWithURLs(ctx, rmq.ConnectArgs{Args: baseCfg, Topology: topology}, os.Getenv("TEST_AMQP_URI"))

	consumer := rmq.NewConsumer(subRMQConn, rmq.ConsumerArgs{
		Args:  baseCfg,
		Queue: topology.Queues[0],
	})

	publisher := rmq.NewPublisher(ctx, pubRMQConn, rmq.PublisherArgs{
		Args:       baseCfg,
		LogReturns: true,
	})

	// publisher2, publisher3 := rmq.NewPublisher(ctx, pubRMQConn, rmq.PublisherArgs{
	// 	Args:       baseCfg,
	// 	LogReturns: true,
	// }), rmq.NewPublisher(ctx, pubRMQConn, rmq.PublisherArgs{
	// 	Args:       baseCfg,
	// 	LogReturns: true,
	// })

	dot := []byte(".")
	errChan := make(chan error)
	consumeChan := consumer.Consume(ctx)

	publishings := generatePublishings(benchNumPubs, queueName)

	cases := []struct {
		name        string
		publishFunc func(b *testing.B)
	}{
		{
			"PublishBatchUntilAcked",
			func(b *testing.B) {
				if err := publisher.PublishBatchUntilAcked(ctx, 0, publishings...); err != nil {
					b.Fatalf("PublishBatchUntilAcked err %v", err)
				}
			},
		},
		// {
		// 	"PublishBatchUntilAcked into thirds",
		// 	func(b *testing.B) {
		// 		errChan := make(chan error)
		// 		publishers := []*rmq.Publisher{publisher, publisher, publisher}
		// 		for i := range publishers {
		// 			go func(i int) {
		// 				errChan <- publishers[i].PublishBatchUntilAcked(ctx, 0, publishings[i:i+1]...)
		// 			}(i)
		// 		}
		// 		successes := 0
		// 		for {
		// 			select {
		// 			case err := <-errChan:
		// 				if err != nil {
		// 					b.Fatalf("PublishBatchUntilAcked err %v", err)
		// 				}
		// 				successes++
		// 				if successes == len(publishers) {
		// 					return
		// 				}
		// 			case <-ctx.Done():
		// 				b.Fatalf("PublishBatchUntilAcked timed out")
		// 			}
		// 		}
		// 	},
		// },
		// {
		// 	"PublishBatchUntilAcked on three Publishers",
		// 	func(b *testing.B) {
		// 		errChan := make(chan error)
		// 		publishers := []*rmq.Publisher{publisher, publisher2, publisher3}
		// 		for i := range publishers {
		// 			go func(i int) {
		// 				errChan <- publishers[i].PublishBatchUntilAcked(ctx, 0, publishings[i:i+1]...)
		// 			}(i)
		// 		}
		// 		successes := 0
		// 		for {
		// 			select {
		// 			case err := <-errChan:
		// 				if err != nil {
		// 					b.Fatalf("PublishBatchUntilAcked err %v", err)
		// 				}
		// 				successes++
		// 				if successes == len(publishers) {
		// 					return
		// 				}
		// 			case <-ctx.Done():
		// 				b.Fatalf("PublishBatchUntilAcked timed out")
		// 			}
		// 		}
		// 	},
		// },
		// {
		// 	"Concurrent PublishUntilAcked",
		// 	func(b *testing.B) {
		// 		errChan := make(chan error)
		// 		for i := range publishings {
		// 			go func(i int) {
		// 				errChan <- publisher.PublishUntilAcked(ctx, 0, publishings[i])
		// 			}(i)
		// 		}
		// 		successes := 0
		// 		for {
		// 			select {
		// 			case err := <-errChan:
		// 				if err != nil {
		// 					b.Fatalf("PublishUntilAcked err %v", err)
		// 				}
		// 				successes++
		// 				if successes == len(publishings) {
		// 					return
		// 				}
		// 			case <-ctx.Done():
		// 				b.Fatalf("PublishUntilAcked timed out")
		// 			}
		// 		}
		// 	},
		// },
	}

	for _, bb := range cases {
		b.Run(bb.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				go func(i int) (err error) {
					received := make(map[uint64]struct{}, len(publishings))
					defer func() { errChan <- err }()
					for {
						select {
						case msg := <-consumeChan:
							rawIndex := bytes.Split(msg.Body, dot)[0]
							index, err := strconv.ParseUint(string(rawIndex), 10, 64)
							if err != nil {
								return fmt.Errorf("strconv.ParseUint err %w", err)
							}
							received[index] = struct{}{}
							if err := msg.Ack(false); err != nil {
								return fmt.Errorf("msg.Ack err %w", err)
							}
							if len(received) == len(publishings) {
								return nil
							}
						case <-ctx.Done():
							return fmt.Errorf("timed out after consuming %d publishings on bench run %d", len(received), i)
						}
					}
				}(i)

				bb.publishFunc(b)

				select {
				case <-ctx.Done():
					b.Fatalf("timed out on bench run %d", i)
				case err := <-errChan:
					if err != nil {
						b.Fatalf("on bench run %d consumer err %v", i, err)

					}
				}
			}
		})
	}
}

func BenchmarkPublishAndConsumeManyWagslane(b *testing.B) {
	setupStart := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	randSuffix := fmt.Sprintf("%d.%p", time.Now().UnixNano(), b)

	queueName := "BenchmarkPublishAndConsumeManyWagslane" + randSuffix

	conn, err := wagslane.NewConn(
		os.Getenv("TEST_AMQP_URI"),
		wagslane.WithConnectionOptionsLogging,
	)
	test.FailOnError(b, err)
	defer conn.Close()

	consumer, err := wagslane.NewConsumer(
		conn,
		queueName,
		wagslane.WithConsumerOptionsQueueArgs(wagslane.Table{
			amqp.QueueTTLArg: time.Minute.Milliseconds(),
		}),
	)
	test.FailOnError(b, err)

	defer consumer.Close()

	consumeChan := make(chan wagslane.Delivery)
	go func() {
		err = consumer.Run(func(d wagslane.Delivery) rabbitmq.Action {
			consumeChan <- d
			return rabbitmq.Manual
		})
		test.FailOnError(b, err)
	}()

	dot := []byte(".")
	errChan := make(chan error)

	publishings := generatePublishings(1000, queueName)

	publisher, err := wagslane.NewPublisher(
		conn,
		wagslane.WithPublisherOptionsLogging,
		wagslane.WithPublisherOptionsConfirm,
	)
	test.FailOnError(b, err)
	defer publisher.Close()

	b.Logf("setup took %s", time.Since(setupStart))
	cases := []struct {
		name        string
		publishFunc func(b *testing.B)
	}{
		{
			"PublishBatchUntilAcked",
			func(b *testing.B) {
				// wagslane doesn't have a batch publisher, implement our own
				toBeConfirmed := make([]wagslane.PublisherConfirmation, 0, len(publishings))
				for _, p := range publishings {
					c, err := publisher.PublishWithDeferredConfirmWithContext(
						ctx,
						p.Body,
						[]string{p.RoutingKey},
						wagslane.WithPublishOptionsMandatory,
					)
					test.FailOnError(b, err)
					toBeConfirmed = append(toBeConfirmed, c)
				}

				for i, confirms := range toBeConfirmed {
					if len(confirms) != 1 {
						b.Fatalf("wagslane published a single message to a single routing key and got %d confirms somehow?", len(confirms))
					}
					acked, err := confirms[0].WaitContext(ctx)
					test.FailOnError(b, err)
					if !acked {
						b.Errorf("wagslane published message %d got nacked", i)
					}
				}
			},
		},
	}

	for _, bb := range cases {
		b.Run(bb.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				go func(i int) (err error) {
					received := make(map[uint64]struct{}, len(publishings))
					defer func() { errChan <- err }()
					for {
						select {
						case msg := <-consumeChan:
							rawIndex := bytes.Split(msg.Body, dot)[0]
							index, err := strconv.ParseUint(string(rawIndex), 10, 64)
							if err != nil {
								return fmt.Errorf("strconv.ParseUint err %w", err)
							}
							received[index] = struct{}{}
							if err := msg.Ack(false); err != nil {
								return fmt.Errorf("msg.Ack err %w", err)
							}
							if len(received) == len(publishings) {
								return nil
							}
						case <-ctx.Done():
							return fmt.Errorf("timed out after consuming %d publishings on bench run %d", len(received), i)
						}
					}
				}(i)

				bb.publishFunc(b)

				select {
				case <-ctx.Done():
					b.Fatalf("timed out on bench run %d", i)
				case err := <-errChan:
					if err != nil {
						b.Fatalf("on bench run %d consumer err %v", i, err)

					}
				}
			}
		})
	}
}
