//go:build rabbit

package rmq_test

import (
	"context"
	"log/slog"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/danlock/rmq"
	amqp "github.com/rabbitmq/amqp091-go"
)

func TestRMQPublisher(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	logf := slog.Log

	rmqConn := rmq.ConnectWithAMQPConfig(ctx, rmq.ConnectConfig{Log: logf}, os.Getenv("TEST_AMQP_URI"), amqp.Config{})

	unreliableRMQPub := rmq.NewPublisher(ctx, rmqConn, rmq.PublisherConfig{DontConfirm: true})
	_, err := unreliableRMQPub.PublishUntilConfirmed(ctx, time.Minute, rmq.Publishing{})
	if err == nil {
		t.Fatalf("PublishUntilConfirmed succeeded despite the publisher set dont confirm")
	}

	returnChan := make(chan amqp.Return, 5)
	rmqPub := rmq.NewPublisher(ctx, rmqConn, rmq.PublisherConfig{
		Log:          logf,
		NotifyReturn: returnChan,
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

	wantedPub := rmq.Publishing{Exchange: "amq.topic"}
	wantedPub.Body = []byte("TestRMQPublisher")
	_, err = rmqPub.PublishUntilConfirmed(pubCtx, time.Minute, wantedPub)
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

	// Publish a few messages at the same time.
	pubCount := 10
	errChan := make(chan error, pubCount)
	for i := 0; i < pubCount; i++ {
		go func() {
			errChan <- rmqPub.PublishUntilAcked(pubCtx, 0, wantedPub)
		}()
	}
	for i := 0; i < pubCount; i++ {
		if err := <-errChan; err != nil {
			t.Fatalf("PublishUntilAcked returned unexpected error %v", err)
		}
	}
	// Publishing mandatory and immediate messages to nonexistent queues should get confirmed, just not acked.
	mandatoryPub := rmq.Publishing{Exchange: "Idontexist", Mandatory: true}
	immediatePub := rmq.Publishing{Exchange: "Idontexist", Immediate: true}
	mandatoryPub.Body = wantedPub.Body
	immediatePub.Body = wantedPub.Body

	defConf, err := rmqPub.PublishUntilConfirmed(pubCtx, 0, mandatoryPub)
	if err != nil {
		t.Fatalf("PublishUntilConfirmed returned unexpected error %v", err)
	}
	if defConf.Acked() {
		t.Fatalf("PublishUntilConfirmed returned unexpected ack for mandatory pub")
	}

	defConf, err = rmqPub.PublishUntilConfirmed(pubCtx, 0, immediatePub)
	if err != nil {
		t.Fatalf("PublishUntilConfirmed returned unexpected error %v", err)
	}
	if defConf.Acked() {
		t.Fatalf("PublishUntilConfirmed returned unexpected ack for immediate pub")
	}

	returnedPub := rmq.Publishing{Exchange: "amq.topic", RoutingKey: "whereverhueyislooking", Mandatory: true}
	returnedPub.Body = []byte("oops")

	err = rmqPub.PublishUntilAcked(pubCtx, 0, returnedPub)
	if err != nil {
		t.Fatalf("PublishUntilAcked got err for return %v", err)
	}

	select {
	case <-pubCtx.Done():
		t.Fatalf("didnt get return")
	case ret := <-returnChan:
		if !reflect.DeepEqual(returnedPub.Body, ret.Body) {
			t.Fatalf("got different return message")
		}
	}

	err = rmqPub.PublishUntilAcked(pubCtx, 0, wantedPub)
	if err != nil {
		t.Fatalf("PublishUntilAcked returned unexpected error %v", err)
	}

	// Cancel everything, now the publisher stopped processing
	cancel()
	_, err = rmqPub.Publish(context.Background(), wantedPub)
	if err == nil {
		t.Fatalf("publish shouldn't succeed")
	}
	_, err = rmqPub.PublishUntilConfirmed(ctx, 0, wantedPub)
	if err == nil {
		t.Fatalf("PublishUntilConfirmed shouldn't succeed")
	}

	err = rmqPub.PublishUntilAcked(ctx, 0, wantedPub)
	if err == nil {
		t.Fatalf("PublishUntilAcked shouldn't succeed")
	}
}
