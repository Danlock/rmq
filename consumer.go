package rmq

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/danlock/rmq/internal"
	amqp "github.com/rabbitmq/amqp091-go"
)

// ConsumerConfig contains all the information needed to declare and consume from a queue.
type ConsumerConfig struct {
	Exchanges        []ConsumerExchange
	ExchangeBindings []ConsumerExchangeBinding
	QueueBindings    []ConsumerQueueBinding
	Queue            ConsumerQueue
	Consume          ConsumerConsume
	Qos              ConsumerQos

	// AMQPTimeout sets a timeout on all AMQP requests.
	// Defaults to 30 seconds.
	AMQPTimeout time.Duration
	// Log can be left nil, set with slog.Log or wrapped around your favorite logging library
	Log func(ctx context.Context, level slog.Level, msg string, args ...any)
	// *RetryInterval controls how frequently rmq.Consumer.Process retries on errors. Defaults from 0.125 seconds to 32 seconds.
	MinRetryInterval, MaxRetryInterval time.Duration
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
	Name       string // Can be empty since RabbitMQ will generate a unique name
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	NoWait     bool
	Passive    bool
	Args       amqp.Table
}

// ConsumerExchangeBinding are args for amqp.Channel.ExchangeBind
type ConsumerExchangeBinding struct {
	Destination string
	RoutingKey  string
	Source      string
	NoWait      bool
	Args        amqp.Table
}

// ConsumerQueueBinding are args for amqp.Channel.QueueBind
type ConsumerQueueBinding struct {
	ExchangeName string
	RoutingKey   string
	NoWait       bool
	Args         amqp.Table
}

// ConsumerConsume are args for amqp.Channel.Consume
type ConsumerConsume struct {
	AutoAck   bool // AutoAck should probably never be set to true in production
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

type Consumer struct {
	config ConsumerConfig
}

// NewConsumer takes in a ConsumerConfig that describes the AMQP topology used by a single queue,
// and returns a rmq.Consumer that can redeclare this topology on any errors during queue consumption.
// This enables robust reconnections even on flaky networks or downed RabbitMQ nodes.
func NewConsumer(config ConsumerConfig) *Consumer {
	if config.AMQPTimeout == 0 {
		config.AMQPTimeout = 30 * time.Second
	}
	internal.WrapLogFunc(&config.Log)

	if config.MinRetryInterval == 0 {
		config.MinRetryInterval = time.Second / 8
	}
	if config.MaxRetryInterval == 0 {
		config.MaxRetryInterval = 32 * time.Second
	}
	return &Consumer{config: config}
}

// Declare will declare an exchange, queue, bindings, etc in preparation for a future Consume call.
// Only closes the amqp.Channel on errors.
// If you only Declare without a future Consume call, remember to close the amqp.Channel.
func (c *Consumer) Declare(ctx context.Context, rmqConn *Connection) (_ *amqp.Channel, err error) {
	logPrefix := fmt.Sprintf("rmq.Consumer.Declare for queue %s", c.config.Queue.Name)
	if c.config.AMQPTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.config.AMQPTimeout)
		defer cancel()
	}

	mqChan, err := rmqConn.Channel(ctx)
	if err != nil {
		return nil, fmt.Errorf(logPrefix+" failed to get a channel due to err %w", err)
	}
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
			if err != nil {
				mqChanErr := mqChan.Close()
				if mqChanErr != nil && !errors.Is(mqChanErr, amqp.ErrClosed) {
					err = errors.Join(err, mqChanErr)
				}
			}
			respChan <- resp{queue, err}
		}()

		for _, e := range c.config.Exchanges {
			if e.Passive {
				err = mqChan.ExchangeDeclarePassive(
					e.Name,
					e.Kind,
					e.Durable,
					e.AutoDelete,
					e.Internal,
					e.NoWait,
					e.Args,
				)
			} else {
				err = mqChan.ExchangeDeclare(
					e.Name,
					e.Kind,
					e.Durable,
					e.AutoDelete,
					e.Internal,
					e.NoWait,
					e.Args,
				)
			}

			if err != nil {
				err = fmt.Errorf(logPrefix+" failed to declare exchange %s due to %w", e.Name, err)
				return
			}
		}

		for _, eb := range c.config.ExchangeBindings {
			err = mqChan.ExchangeBind(eb.Destination, eb.RoutingKey, eb.Source, eb.NoWait, eb.Args)
			if err != nil {
				err = fmt.Errorf(logPrefix+" failed to bind exchange %s to %s due to %w", eb.Destination, eb.Source, err)
				return
			}
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
			err = fmt.Errorf(logPrefix+" failed to declare queue due to %w", err)
			return
		}

		for _, b := range c.config.QueueBindings {
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
				err = fmt.Errorf(logPrefix+" unable to set prefetch due to %w", err)
				return
			}
		}
	}()

	select {
	case <-ctx.Done():
		go func() {
			// Log our leaked goroutine's response whenever it finally finishes in case it has useful information.
			r := <-respChan
			c.config.Log(ctx, slog.LevelWarn, logPrefix+" completed after it's context finished. It took %s. Err: %+v", time.Since(start), r.err)
		}()
		return nil, fmt.Errorf(logPrefix+" unable to complete before context did due to %w", context.Cause(ctx))
	case r := <-respChan:
		return mqChan, r.err
	}
}

// amqpChannelConsume will start consuming from the previously declared queue. Only closes mqChan on errors. Not public since amqp.Channel.ConsumeWithContext should replace most of it eventually.
func (c *Consumer) amqpChannelConsume(ctx context.Context, mqChan *amqp.Channel) (_ <-chan amqp.Delivery, err error) {
	logPrefix := fmt.Sprintf("rmq.Consumer.amqpChannelConsume for queue %s", c.config.Queue.Name)
	if c.config.AMQPTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.config.AMQPTimeout)
		defer cancel()
	}
	// TODO: https://github.com/rabbitmq/amqp091-go/pull/192 has merged a Channel.ConsumeWithContext, which may just cause this entire function to be deleted in the future.
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
		if r.err != nil {
			mqChanErr := mqChan.Close()
			if mqChanErr != nil && !errors.Is(mqChanErr, amqp.ErrClosed) {
				r.err = errors.Join(r.err, mqChanErr)
			}
		}
		respChan <- r
	}()

	select {
	case <-ctx.Done():
		go func() {
			// Log our leaked goroutine's response whenever it finally finishes in case it has useful information.
			r := <-respChan
			if r.err != nil {
				c.config.Log(ctx, slog.LevelWarn, logPrefix+" completed after it's context finished. It took %s. Err: %+v", time.Since(start), r.err)
			}
		}()
		return nil, fmt.Errorf(logPrefix+" context ended before it finished due to %w", context.Cause(ctx))
	case r := <-respChan:
		return r.deliveries, r.err
	}
}

// Consume uses the rmq.Consumer config to repeatedly Declare and amqp.Channel.Consume from an AMQP queue, forwarding deliveries to it's returned channel.
// On any error Consume will reconnect to AMQP, redeclare it's topology and resume consumption and forwarding of deliveries.
// Consume returns an unbuffered channel, and will block sending to it if it has no listeners.
// The returned channel is closed only after the context finishes.
func (c *Consumer) Consume(ctx context.Context, rmqConn *Connection) <-chan amqp.Delivery {
	outChan := make(chan amqp.Delivery)
	go func() {
		logPrefix := fmt.Sprintf("rmq.Consumer.Consume for queue %s", c.config.Queue.Name)
		var delay time.Duration
		var mqChan *amqp.Channel
		var err error
		for {
			select {
			case <-ctx.Done():
				close(outChan)
				return
			case <-time.After(delay):
			}

			mqChan, err = c.Declare(ctx, rmqConn)
			if err != nil {
				delay = internal.CalculateDelay(c.config.MinRetryInterval, c.config.MaxRetryInterval, delay)
				c.config.Log(ctx, slog.LevelError, logPrefix+" failed to Declare. Retrying in %s due to %v", delay.String(), err)
				continue
			}

			inChan, err := c.amqpChannelConsume(ctx, mqChan)
			if err != nil {
				delay = internal.CalculateDelay(c.config.MinRetryInterval, c.config.MaxRetryInterval, delay)
				c.config.Log(ctx, slog.LevelError, logPrefix+" failed to Consume. Retrying in %s due to %v", delay.String(), err)
				continue
			}
			// Successfully redeclared our topology, so reset the backoff
			delay = 0

			c.forwardDeliveries(ctx, mqChan, inChan, outChan)
		}
	}()
	return outChan
}

// forwardDeliveries forwards from inChan until it closes. If the context finishes it closes the amqp Channel so that the delivery channel will close eventually.
func (c *Consumer) forwardDeliveries(ctx context.Context, mqChan *amqp.Channel, inChan <-chan amqp.Delivery, outChan chan<- amqp.Delivery) {
	logPrefix := fmt.Sprintf("rmq.Consumer.forwardDeliveries for queue %s", c.config.Queue.Name)
	closeNotifier := mqChan.NotifyClose(make(chan *amqp.Error, 2))

	for {
		select {
		case <-ctx.Done():
			if err := mqChan.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
				c.config.Log(ctx, slog.LevelError, logPrefix+" failed to Close it's AMQP channel due to %v", err)
				// Typically we exit processDeliveries by waiting for inChan to close, but if we can't close even close the AMQP channel then abandon ship
				return
			}
		case err := <-closeNotifier:
			if err != nil {
				c.config.Log(ctx, slog.LevelError, logPrefix+" got an AMQP Channel Close error %+v", err)
			}
		case msg, ok := <-inChan:
			if !ok {
				return
			}
			// If the client never listens to outChan, this blocks forever
			// Other options include using select with a default and dropping the message if the client doesn't listen, dropping the message after a timeout,
			// or buffering messages and sending them again later. Of course the buffer could grow forever in that case without listeners.
			// The only thing blocked would be the rmq.Consumer.Process goroutine listening for reconnects and logging errors, which seem unneccessary without a listener anyway.
			// Alls well since we don't lock up the entire amqp.Connection like streadway/amqp with Notify* channels...
			outChan <- msg
		}
	}
}

// ConsumeConcurrently simply runs the provided deliveryProcessor on each delivery from Consume in a new goroutine.
// maxGoroutines limits the amounts of goroutines spawned and defaults to 2000.
// Qos.PrefetchCount can also limit goroutines spawned if deliveryProcessor properly Acks messages.
// Blocks until the context is finished and the Consume channel closes.
func (c *Consumer) ConsumeConcurrently(ctx context.Context, rmqConn *Connection, maxGoroutines uint64, deliveryProcessor func(ctx context.Context, msg amqp.Delivery)) {
	if maxGoroutines == 0 {
		maxGoroutines = 2000
	}
	semaphore := make(chan struct{}, maxGoroutines)
	for msg := range c.Consume(ctx, rmqConn) {
		semaphore <- struct{}{}
		go func(msg amqp.Delivery) { deliveryProcessor(ctx, msg); <-semaphore }(msg)
	}
}
