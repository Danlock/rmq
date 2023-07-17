package pubsub

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/danlock/rmq/redial"
	amqp "github.com/rabbitmq/amqp091-go"
)

// ConsumerConfig contains all the information needed to declare and consume from a queue.
type ConsumerConfig struct {
	Exchange ConsumerExchange
	Queue    ConsumerQueue
	Bindings []ConsumerBinding
	Consume  ConsumerConsume

	// AMQPTimeout sets a timeout on all AMQP requests.
	// Defaults to 30 seconds.
	AMQPTimeout time.Duration
	// Set Logf with your favorite logging library
	Logf func(msg string, args ...any)
}

// ConsumerExchange are args for amqp.Channel.ExchangeDeclare
type ConsumerExchange struct {
	Name       string
	Kind       string
	Durable    bool
	AutoDelete bool
	Internal   bool
	NoWait     bool
	Passive    bool
	Args       amqp.Table
}

// ConsumerQueue are args for amqp.Channel.QueueDeclare
type ConsumerQueue struct {
	Name       string
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	NoWait     bool
	Passive    bool
	Args       amqp.Table
}

// ConsumerBinding are args for amqp.Channel.QueueBind
type ConsumerBinding struct {
	RoutingKey   string
	ExchangeName string
	NoWait       bool
	Args         amqp.Table
}

// ConsumerBinding are args for amqp.Channel.Consume
type ConsumerConsume struct {
	AutoAck   bool
	Consumer  string
	Exclusive bool
	NoLocal   bool
	NoWait    bool
	Args      amqp.Table
}

type RMQConsumer struct {
	config ConsumerConfig
}

func NewConsumer(config ConsumerConfig) *RMQConsumer {
	if config.AMQPTimeout == 0 {
		config.AMQPTimeout = 30 * time.Second
	}
	if config.Logf == nil {
		config.Logf = func(msg string, args ...any) {}
	}
	return &RMQConsumer{config: config}
}

// Declare will declare an exchange, queue, bindings in preparation for a future Consume call.
// Only closes the channel on errors.
func (c *RMQConsumer) Declare(ctx context.Context, rmqConn redial.RMQConnection) (_ *amqp.Channel, err error) {
	if c.config.AMQPTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.config.AMQPTimeout)
		defer cancel()
	}

	mqChan, err := rmqConn.Channel(ctx)
	if err != nil {
		return nil, fmt.Errorf("RMQConsumer.Declare failed to get a channel for queue %s", c.config.Queue.Name)
	}
	defer func() {
		if err != nil {
			mqChanErr := mqChan.Close()
			if mqChanErr != nil && !errors.Is(mqChanErr, amqp.ErrClosed) {
				c.config.Logf("RMQConsumer.Declare got an error while closing the amqp.Channel. err %+v", mqChanErr)
			}
		}
	}()

	// Network calls that don't take a context are dangerous, and can block indefintely.
	// Call them in a goroutine so we can timeout if necessary

	type resp struct {
		queue amqp.Queue
		err   error
	}
	respChan := make(chan resp, 1)
	start := time.Now()
	go func() {
		var err error
		var queue amqp.Queue

		defer func() {
			respChan <- resp{queue, err}
		}()

		if c.config.Exchange.Passive {
			err = mqChan.ExchangeDeclarePassive(
				c.config.Exchange.Name,
				c.config.Exchange.Kind,
				c.config.Exchange.Durable,
				c.config.Exchange.AutoDelete,
				c.config.Exchange.Internal,
				c.config.Exchange.NoWait,
				c.config.Exchange.Args,
			)
		} else {
			err = mqChan.ExchangeDeclare(
				c.config.Exchange.Name,
				c.config.Exchange.Kind,
				c.config.Exchange.Durable,
				c.config.Exchange.AutoDelete,
				c.config.Exchange.Internal,
				c.config.Exchange.NoWait,
				c.config.Exchange.Args,
			)
		}

		if err != nil {
			err = fmt.Errorf("RMQConsumer.Declare failed to declare exchange %s %w", c.config.Exchange.Name, err)
			return
		}

		if c.config.Queue.Passive {
			queue, err = mqChan.QueueDeclarePassive(
				c.config.Queue.Name,
				c.config.Queue.Durable,
				c.config.Queue.AutoDelete,
				c.config.Queue.Exclusive,
				c.config.Queue.NoWait,
				c.config.Queue.Args,
			)
		} else {
			queue, err = mqChan.QueueDeclare(
				c.config.Queue.Name,
				c.config.Queue.Durable,
				c.config.Queue.AutoDelete,
				c.config.Queue.Exclusive,
				c.config.Queue.NoWait,
				c.config.Queue.Args,
			)
		}
		if err != nil {
			err = fmt.Errorf("RMQConsumer.Declare failed to declare queue %s %w", c.config.Queue.Name, err)
			return
		}

		for _, b := range c.config.Bindings {
			err = mqChan.QueueBind(
				queue.Name,
				b.RoutingKey,
				b.ExchangeName,
				b.NoWait,
				b.Args,
			)
			if err != nil {
				err = fmt.Errorf(
					"RMQConsumer.Declare unable to bind queue '%s' to exchange '%s' via key '%s' %w",
					queue.Name,
					b.ExchangeName,
					b.RoutingKey,
					err,
				)
				return
			}
		}
	}()

	select {
	case <-ctx.Done():
		go func() {
			// Log our leaked goroutine's response whenever it finally finishes in case it has useful information.
			r := <-respChan
			c.config.Logf("RMQConsumer.Declare completed after it's context finished. It took %s. Err: %+v", time.Since(start), r.err)
		}()
		return nil, fmt.Errorf("RMQConsumer.Declare context ended before it finished %w", context.Cause(ctx))
	case r := <-respChan:
		// Set our consumer's queue name in case of an anonymous queue
		c.config.Queue.Name = r.queue.Name
		return mqChan, r.err
	}
}

// Consume will start consuming from the previously declared queue. Only closes the channel on errors.
func (c *RMQConsumer) Consume(ctx context.Context, mqChan *amqp.Channel) (_ <-chan amqp.Delivery, err error) {
	if c.config.AMQPTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.config.AMQPTimeout)
		defer cancel()
	}

	defer func() {
		if err != nil {
			mqChanErr := mqChan.Close()
			if mqChanErr != nil && !errors.Is(mqChanErr, amqp.ErrClosed) {
				c.config.Logf("RMQConsumer.Declare got an error while closing the amqp.Channel. err %+v", mqChanErr)
			}
		}
	}()

	type resp struct {
		deliveries <-chan amqp.Delivery
		err        error
	}
	respChan := make(chan resp, 1)
	start := time.Now()
	go func() {
		var r resp
		r.deliveries, r.err = mqChan.Consume(
			c.config.Queue.Name,
			c.config.Consume.Consumer,
			c.config.Consume.AutoAck,
			c.config.Consume.Exclusive,
			c.config.Consume.NoLocal,
			c.config.Consume.NoWait,
			c.config.Consume.Args,
		)
		respChan <- r
	}()

	select {
	case <-ctx.Done():
		go func() {
			// Log our leaked goroutine's response whenever it finally finishes in case it has useful information.
			r := <-respChan
			c.config.Logf("RMQConsumer.Consume completed after it's context finished. It took %s. Err: %+v", time.Since(start), r.err)
		}()
		return nil, fmt.Errorf("RMQConsumer.Consume context ended before it finished %w", context.Cause(ctx))
	case r := <-respChan:
		return r.deliveries, r.err
	}
}
