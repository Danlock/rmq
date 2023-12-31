package rmq

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Exchange contains args for amqp.Channel.ExchangeDeclare
type Exchange struct {
	Name       string // Name is required by ExchangeDeclare.
	Kind       string // Kind is required by ExchangeDeclare. amqp091-go exports valid values like amqp.ExchangeDirect, etc
	Durable    bool
	AutoDelete bool
	Internal   bool
	NoWait     bool
	Passive    bool
	Args       amqp.Table
}

// ExchangeBinding contains args for amqp.Channel.ExchangeBind
type ExchangeBinding struct {
	Destination string
	RoutingKey  string
	Source      string
	NoWait      bool
	Args        amqp.Table
}

// DeclareTopology declares an AMQP topology once.
// If you want this to be redeclared automatically on connections, add your Topology to ConnectArgs instead.
func DeclareTopology(ctx context.Context, amqpConn AMQPConnection, topology Topology) error {
	logPrefix := fmt.Sprintf("rmq.DeclareTopology for AMQPConnection (%s -> %s)", amqpConn.LocalAddr(), amqpConn.RemoteAddr())

	if topology.empty() {
		return nil
	}
	dontLog := topology.Log == nil
	topology.setDefaults()
	ctx, cancel := context.WithTimeout(ctx, topology.AMQPTimeout)
	defer cancel()

	// amqp091 currently does not use contexts all throughout, and therefore any call could block forever if the network is temperamental that day.
	// Call them in a goroutine so we can bail if necessary
	start := time.Now()
	errChan := make(chan error, 1)
	shouldLog := make(chan struct{})

	go func() {
		mqChan, err := amqpConn.Channel()
		if err != nil {
			errChan <- fmt.Errorf(logPrefix+" failed to get amqp.Channel due to %w", err)
			return
		}
		err = topology.declare(ctx, mqChan)
		// An amqp.Channel must not be used from multiple goroutines simultaneously, so close it inside this goroutine to prevent cryptic RabbitMQ errors.
		mqChanErr := mqChan.Close()
		// Should we join mqChanErr if err is nil? When declare succeeeds a Close error is fairly inconsequential. Unless it leaves the channel in a bad state...
		// But we don't actually use the channel after this. Maybe just log it in that case? Food for thought.
		if mqChanErr != nil && !errors.Is(mqChanErr, amqp.ErrClosed) {
			err = errors.Join(err, mqChanErr)
		}

		select {
		case <-shouldLog:
			topology.Log(ctx, slog.LevelWarn, logPrefix+" completed after it's context finished. It took %s. Err: %+v", time.Since(start), err)
		default:
			errChan <- err
		}
	}()

	select {
	case <-ctx.Done():
		// Log our leaked goroutine's response whenever it finally finishes since it may have useful debugging information.
		if !dontLog {
			close(shouldLog)
		}
		return fmt.Errorf(logPrefix+" unable to complete before context due to %w", context.Cause(ctx))
	case err := <-errChan:
		return err
	}
}

// Topology contains all the exchange, queue and binding information needed for your application to use RabbitMQ.
type Topology struct {
	Args

	Exchanges        []Exchange
	ExchangeBindings []ExchangeBinding
	Queues           []Queue
	QueueBindings    []QueueBinding
}

func (t *Topology) empty() bool {
	return len(t.Exchanges) == 0 && len(t.Queues) == 0 &&
		len(t.ExchangeBindings) == 0 && len(t.QueueBindings) == 0
}

func (t *Topology) declare(ctx context.Context, mqChan *amqp.Channel) (err error) {
	logPrefix := fmt.Sprintf("rmq.Topology.declare ")

	for _, e := range t.Exchanges {
		exchangeDeclare := mqChan.ExchangeDeclare
		if e.Passive {
			exchangeDeclare = mqChan.ExchangeDeclarePassive
		}
		err = exchangeDeclare(e.Name, e.Kind, e.Durable, e.AutoDelete, e.Internal, e.NoWait, e.Args)

		if err != nil {
			return fmt.Errorf(logPrefix+" failed to declare exchange %s due to %w", e.Name, err)
		} else if err = context.Cause(ctx); err != nil {
			return fmt.Errorf(logPrefix+" failed to declare exchanges before context ended due to %w", err)
		}
	}

	for _, eb := range t.ExchangeBindings {
		err = mqChan.ExchangeBind(eb.Destination, eb.RoutingKey, eb.Source, eb.NoWait, eb.Args)
		if err != nil {
			return fmt.Errorf(logPrefix+" failed to bind exchange %s to %s due to %w", eb.Destination, eb.Source, err)
		} else if err = context.Cause(ctx); err != nil {
			return fmt.Errorf(logPrefix+" failed to declare exchange bindings before context ended due to %w", err)
		}
	}

	for _, q := range t.Queues {
		if q.Name == "" {
			// Anonymous Queues auto generate different names on different amqp.Channel's.
			// Queues like this must be declared by the Consumer instead so it can receive messages from the queue, so we skip them here.
			// Should we log a warning? Seems like that would get annoying if you just wanted to pass the same Queue struct around.
			continue
		}

		queueDeclare := mqChan.QueueDeclare
		if q.Passive {
			queueDeclare = mqChan.QueueDeclarePassive
		}
		_, err = queueDeclare(q.Name, q.Durable, q.AutoDelete, q.Exclusive, q.NoWait, q.Args)

		if err != nil {
			return fmt.Errorf(logPrefix+" failed to declare queue due to %w", err)
		} else if err = context.Cause(ctx); err != nil {
			return fmt.Errorf(logPrefix+" failed to declare queues before context ended due to %w", err)
		}
	}

	for _, b := range t.QueueBindings {
		err = mqChan.QueueBind(b.QueueName, b.RoutingKey, b.ExchangeName, b.NoWait, b.Args)
		if err != nil {
			return fmt.Errorf(logPrefix+" unable to bind queue to exchange '%s' via key '%s' due to %w", b.ExchangeName, b.RoutingKey, err)
		} else if err = context.Cause(ctx); err != nil {
			return fmt.Errorf(logPrefix+" failed to declare queue bindings before context ended due to %w", err)
		}
	}

	return nil
}
