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

func TestRMQPublisher(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logf := internal.LogTillCtx(t, ctx)

	rmqConn := rmq.ConnectWithAMQPConfig(ctx, rmq.ConnectConfig{Logf: logf}, os.Getenv("TEST_AMQP_URI"), amqp.Config{})

	// rmqCons := rmq.NewConsumer(rmq.ConsumerConfig{
	// 	Exchange: rmq.ConsumerExchange{
	// 		Name: "amq.topic",
	// 		Kind: amqp.ExchangeTopic,
	// 	},
	// 	Queue: rmq.ConsumerQueue{
	// 		Name:       "TestRMQPublisher" + time.Now().String(),
	// 		AutoDelete: true,
	// 	},
	// 	Bindings: []rmq.ConsumerBinding{
	// 		{ExchangeName: "amq.topic", RoutingKey: "TestRMQPublisher"},
	// 	},
	// 	Consume: rmq.ConsumerConsume{
	// 		Consumer: "TestRMQPublisher",
	// 	},

	// 	Logf: logf,
	// })

	wantedPub := rmq.Publishing{
		Exchange: "amq.topic",
		Publishing: amqp.Publishing{
			Body: []byte("TestRMQPublisher"),
		},
	}

	_ = rmq.NewPublisher(ctx, rmqConn, rmq.PublisherConfig{})
	returnChan, flowChan := make(chan amqp.Return, 5), make(chan bool, 5)
	rmqPub := rmq.NewPublisher(ctx, rmqConn, rmq.PublisherConfig{
		Logf:         logf,
		NotifyReturn: returnChan,
		NotifyFlow:   flowChan,
	})
	forceRedial := func() {
		amqpConn, err := rmqConn.CurrentConnection(ctx)
		if err != nil {
			t.Fatalf("failed to get rmqConn's current connection %v", err)
		}
		// close the current connection to force a redial
		_ = amqpConn.Close()
	}
	forceRedial()
	pubCtx, pubCancel := context.WithTimeout(ctx, 10*time.Second)
	defer pubCancel()
	err := rmqPub.PublishUntilConfirmed(pubCtx, time.Minute, false, wantedPub)
	if err != nil {
		t.Fatalf("PublishUntilConfirmed failed with %v", err)
	}
	// Publish a message to get it returned
	retPub := rmq.Publishing{Exchange: "amq.topic", RoutingKey: "nowhere", Mandatory: true}
	retPub.Body = []byte("return me")

	_, err = rmqPub.Publish(pubCtx, retPub)
	if err != nil {
		t.Fatalf("PublishUntilConfirmed failed with %v", err)
	}
	forceRedial()
	select {
	case <-pubCtx.Done():
		t.Fatalf("didnt get return")
	case ret := <-returnChan:
		if !reflect.DeepEqual(retPub.Body, ret.Body) {
			t.Fatalf("got different return message")
		}
	}

	// Cancel everything, now the publisher stopped processing
	cancel()
	_, err = rmqPub.Publish(context.Background(), wantedPub)
	if err == nil {
		t.Fatalf("publish shouldn't succeed")
	}
	_, err = rmqPub.Publish(ctx, wantedPub)
	if err == nil {
		t.Fatalf("publish shouldn't succeed")
	}
}
