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
	Qos      ConsumerQos

	// AMQPTimeout sets a timeout on all AMQP requests.
	// Defaults to 30 seconds.
	AMQPTimeout time.Duration
	// Set Logf with your favorite logging library
	Logf func(msg string, args ...any)
	// Process*Backoff controls how frequently Process retries on errors. Defaults from 125 ms to 32 seconds.
	ProcessMinBackoff, ProcessMaxBackoff time.Duration
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

// ConsumerConsume are args for amqp.Channel.Consume
type ConsumerConsume struct {
	AutoAck   bool
	Consumer  string
	Exclusive bool
	NoLocal   bool
	NoWait    bool
	Args      amqp.Table
}

// ConsumerQos are args for amqp.Channel.Qos
type ConsumerQos struct {
	PrefetchCount int
	PrefetchSize  int
	Global        bool
}

type RMQConsumer struct {
	config ConsumerConfig
}

// NewConsumer creates an RMQConsumer to be used to pull messages from a RabbitMQ queue.
// A RMQConsumer contains all the information needed to repeatedly redeclare a RabbitMQ consumer in case of errors.
func NewConsumer(config ConsumerConfig) *RMQConsumer {
	if config.AMQPTimeout == 0 {
		config.AMQPTimeout = 30 * time.Second
	}
	if config.Logf == nil {
		config.Logf = func(msg string, args ...any) {}
	}
	if config.ProcessMinBackoff == 0 {
		config.ProcessMinBackoff = time.Second / 8
	}
	if config.ProcessMaxBackoff == 0 {
		config.ProcessMaxBackoff = 32 * time.Second
	}
	return &RMQConsumer{config: config}
}

// Declare will declare an exchange, queue, bindings in preparation for a future Consume call.
// Only closes the channel on errors.
func (c *RMQConsumer) Declare(ctx context.Context, rmqConn redial.RMQConnection) (_ *amqp.Channel, err error) {
	logPrefix := "RMQConsumer.Declare for queue %s"
	if c.config.AMQPTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.config.AMQPTimeout)
		defer cancel()
	}

	mqChan, err := rmqConn.Channel(ctx)
	if err != nil {
		return nil, fmt.Errorf(logPrefix+" failed to get a channel due to err %w", c.config.Queue.Name, err)
	}
	defer func() {
		if err != nil {
			mqChanErr := mqChan.Close()
			if mqChanErr != nil && !errors.Is(mqChanErr, amqp.ErrClosed) {
				c.config.Logf(logPrefix+" failed to close the amqp.Channel due to err %+v", c.config.Queue.Name, mqChanErr)
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
			err = fmt.Errorf(logPrefix+" failed to declare exchange %s due to %w", c.config.Queue.Name, c.config.Exchange.Name, err)
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
			err = fmt.Errorf(logPrefix+" failed to declare queue due to %w", c.config.Queue.Name, err)
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
					logPrefix+" unable to bind queue to exchange '%s' via key '%s' due to %w",
					queue.Name,
					b.ExchangeName,
					b.RoutingKey,
					err,
				)
				return
			}
		}

		if c.config.Qos != (ConsumerQos{}) {
			err = mqChan.Qos(c.config.Qos.PrefetchCount, c.config.Qos.PrefetchSize, c.config.Qos.Global)
			if err != nil {
				err = fmt.Errorf(logPrefix+" unable to set prefetch due to %w", queue.Name, err)
				return
			}
		}
	}()

	select {
	case <-ctx.Done():
		go func() {
			// Log our leaked goroutine's response whenever it finally finishes in case it has useful information.
			r := <-respChan
			c.config.Logf(logPrefix+" completed after it's context finished. It took %s. Err: %+v", c.config.Queue.Name, time.Since(start), r.err)
		}()
		return nil, fmt.Errorf(logPrefix+" unable to complete before context did due to %w", c.config.Queue.Name, context.Cause(ctx))
	case r := <-respChan:
		// Set our consumer's queue name in case of an anonymous queue which would have left c.Config.Queue.Name blank
		c.config.Queue.Name = r.queue.Name
		return mqChan, r.err
	}
}

// Consume will start consuming from the previously declared queue. Only closes mqChan on errors.
func (c *RMQConsumer) Consume(ctx context.Context, mqChan *amqp.Channel) (_ <-chan amqp.Delivery, err error) {
	logPrefix := "RMQConsumer.Consume for queue %s"
	if c.config.AMQPTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.config.AMQPTimeout)
		defer cancel()
	}

	defer func() {
		if err != nil {
			mqChanErr := mqChan.Close()
			if mqChanErr != nil && !errors.Is(mqChanErr, amqp.ErrClosed) {
				c.config.Logf(logPrefix+" failed to close the amqp.Channel due to err %+v", c.config.Queue.Name, mqChanErr)
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
			if r.err != nil {
				c.config.Logf(logPrefix+" completed after it's context finished. It took %s. Err: %+v", c.config.Queue.Name, time.Since(start), r.err)
			}
		}()
		return nil, fmt.Errorf(logPrefix+" context ended before it finished due to %w", c.config.Queue.Name, context.Cause(ctx))
	case r := <-respChan:
		return r.deliveries, r.err
	}
}

func calcDelay(min, max, current time.Duration) time.Duration {
	if current == 0 {
		return min
	} else if current < max {
		return current * 2
	} else {
		return max
	}
}

// Process uses the RMQConsumer config to repeatedly Declare and Consume from an AMQP queue, processing each message concurrently with deliveryProcessor.
// Because a goroutine is span up for each message, ConsumerQos must be set if this function is being used to provide an upper bound.
// A default prefetch count (and max goroutine count) of 2000 is used by Process if ConsumerQos isn't set.
// On any error Process will reconnect to AMQP, redeclare it's topology and resume consumption of messages.
// Blocks until it's context is finished, upon which it will return with an error describing why the context finished.
func (c *RMQConsumer) Process(ctx context.Context, rmqConn redial.RMQConnection, deliveryProcessor func(ctx context.Context, msg amqp.Delivery)) (err error) {
	logPrefix := "RMQConsumer.Process for queue %s"
	if c.config.Qos == (ConsumerQos{}) {
		c.config.Qos.PrefetchCount = 2000
	}
	var delay time.Duration
	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-time.After(delay):
		}

		mqChan, err := c.Declare(ctx, rmqConn)
		if err != nil {
			delay = calcDelay(c.config.ProcessMinBackoff, c.config.ProcessMaxBackoff, delay)
			c.config.Logf(logPrefix+" failed to Declare. Retrying in %s due to %v", c.config.Queue.Name, delay.String(), err)
			continue
		}

		msgChan, err := c.Consume(ctx, mqChan)
		if err != nil {
			delay = calcDelay(c.config.ProcessMinBackoff, c.config.ProcessMaxBackoff, delay)
			c.config.Logf(logPrefix+" failed to Consume. Retrying in %s due to %v", c.config.Queue.Name, delay.String(), err)
			continue
		}
		// Successfully redeclared our topology, so reset the backoff
		delay = 0

		c.processDeliveries(ctx, mqChan, msgChan, deliveryProcessor)
	}
}

func (c *RMQConsumer) processDeliveries(ctx context.Context, mqChan *amqp.Channel, msgChan <-chan amqp.Delivery, processor func(ctx context.Context, msg amqp.Delivery)) {
	logPrefix := "RMQConsumer.processDeliveries for queue %s"
	closeNotifier := mqChan.NotifyClose(make(chan *amqp.Error, 2))
	for {
		select {
		case <-ctx.Done():
			if err := mqChan.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
				c.config.Logf(logPrefix+" failed to Close it's AMQP channel due to %v", c.config.Queue.Name, err)
				// Typically we exit processDeliveries by waiting for the msgChan to close, but if we can't close the mqChan then abandon ship
				return
			}
		case err := <-closeNotifier:
			if err != nil {
				c.config.Logf(logPrefix+" got an AMQP Channel Close error %+v", c.config.Queue.Name, err)
			}
		case msg, ok := <-msgChan:
			if !ok {
				return
			}
			go processor(ctx, msg)
		}
	}
}