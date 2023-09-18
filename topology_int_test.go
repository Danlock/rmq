//go:build rabbit

package rmq_test

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/danlock/rmq"
	amqp "github.com/rabbitmq/amqp091-go"
)

func TestDeclareTopology(t *testing.T) {
	ctx := context.Background()
	rmqConn := rmq.ConnectWithAMQPConfig(ctx, rmq.ConnectConfig{Log: slog.Log}, os.Getenv("TEST_AMQP_URI"), amqp.Config{})
	suffix := fmt.Sprintf("%s|%p", time.Now(), t)
	baseTopology := rmq.Topology{
		Exchanges: []rmq.Exchange{{
			Name:       "temporary",
			Kind:       amqp.ExchangeTopic,
			AutoDelete: true,
		}, {
			Name:       "ephemeral",
			Kind:       amqp.ExchangeTopic,
			AutoDelete: true,
		}, {
			Name:       "ephemeral",
			Kind:       amqp.ExchangeTopic,
			AutoDelete: true,
			Passive:    true,
		}},
		ExchangeBindings: []rmq.ExchangeBinding{{
			Destination: "temporary",
			RoutingKey:  "dopeopleevenuseexchangebindings",
			Source:      "ephemeral",
		}},
		Queues: []rmq.Queue{{
			// DeclareTopology skips queues without Names. Those are for Consumers instead
		}, {
			Name:      "transient" + suffix,
			Durable:   true,
			Exclusive: true,
			Args:      amqp.Table{amqp.QueueTTLArg: time.Minute.Milliseconds()},
		}, {
			Name:      "transient" + suffix,
			Durable:   true,
			Exclusive: true,
			Passive:   true,
			Args:      amqp.Table{amqp.QueueTTLArg: time.Minute.Milliseconds()},
		}},
		QueueBindings: []rmq.QueueBinding{{
			QueueName:    "transient" + suffix,
			ExchangeName: "temporary",
			RoutingKey:   "route66",
		}},
	}

	jsonTopologyString := `{
		"Exchanges": [{"Name":"jsonXchg", "Kind": "topic", "AutoDelete": true}],
		"Queues": [{"Name": "jsonQueue", "AutoDelete": true}]
	}`
	jsonTopology, err := rmq.ImportJSONTopology(strings.NewReader(jsonTopologyString))
	if err != nil {
		t.Fatalf("failed to ImportJSONTopology %v", err)
	}

	amqpConn, err := rmqConn.CurrentConnection(ctx)
	if err != nil {
		t.Fatalf("failed to CurrentConnection %v", err)
	}

	tests := []struct {
		name     string
		timeout  time.Duration
		topology rmq.Topology
		wantErr  bool
	}{
		{
			"success",
			time.Minute,
			baseTopology,
			false,
		},
		{
			"empty success",
			time.Minute,
			rmq.Topology{},
			false,
		},
		{
			"failure due to timeout",
			time.Millisecond,
			baseTopology,
			true,
		},
		{
			"json success",
			time.Minute,
			jsonTopology,
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(ctx, tt.timeout)
			defer cancel()
			tt.topology.Log = slog.Log
			if err := rmq.DeclareTopology(ctx, amqpConn, tt.topology); (err != nil) != tt.wantErr {
				t.Errorf("DeclareTopology() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
