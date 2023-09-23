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

// ConsumerArgs contains information needed to declare and consume deliveries from a queue.
type ConsumerArgs struct {
	Args

	Queue         Queue
	QueueBindings []QueueBinding
	Consume       Consume
	Qos           Qos
}

// Queue contains args for amqp.Channel.QueueDeclare
type Queue struct {
	Name       string // If empty RabbitMQ will generate an unique name
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	NoWait     bool
	Passive    bool // Recommended for Consumer's to passively declare a queue previously declared by a Topology
	Args       amqp.Table
}

// QueueBinding contains args for amqp.Channel.QueueBind
type QueueBinding struct {
	QueueName    string // If empty, RabbitMQ will use the previously generated unique name
	ExchangeName string
	RoutingKey   string
	NoWait       bool
	Args         amqp.Table
}

// Consume contains args for amqp.Channel.Consume
type Consume struct {
	AutoAck   bool // AutoAck should not be set if you want to actually receive all your messages
	Consumer  string
	Exclusive bool
	NoLocal   bool
	NoWait    bool
	Args      amqp.Table
}

// Qos contains args for amqp.Channel.Qos
type Qos struct {
	PrefetchCount int // Recommended to be set, 2000 is a decent enough default but it heavily depends on your message size.
	PrefetchSize  int
	Global        bool
}

// Consumer enables reliable AMQP Queue consumption.
type Consumer struct {
	config ConsumerArgs
	conn   *Connection
}

// NewConsumer takes in a ConsumerArgs that describes the AMQP topology of a single queue,
// and returns a rmq.Consumer that can redeclare this topology on any errors during queue consumption.
// This enables robust reconnections even on unreliable networks.
func NewConsumer(rmqConn *Connection, config ConsumerArgs) *Consumer {
	config.setDefaults()
	return &Consumer{config: config, conn: rmqConn}
}

// safeDeclareAndConsume safely declares and consumes from an amqp.Queue
// Closes the amqp.Channel on errors.
func (c *Consumer) safeDeclareAndConsume(ctx context.Context) (_ *amqp.Channel, _ <-chan amqp.Delivery, err error) {
	logPrefix := fmt.Sprintf("rmq.Consumer.safeDeclareAndConsume for queue %s", c.config.Queue.Name)
	ctx, cancel := context.WithTimeout(ctx, c.config.AMQPTimeout)
	defer cancel()

	mqChan, err := c.conn.Channel(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf(logPrefix+" failed to get a channel due to err %w", err)
	}
	// Network calls that don't take a context can block indefintely.
	// Call them in a goroutine so we can timeout if necessary

	respChan := make(chan internal.ChanResp[<-chan amqp.Delivery], 1)
	shouldLog := make(chan struct{})
	start := time.Now()
	go func() {
		var r internal.ChanResp[<-chan amqp.Delivery]
		r.Val, r.Err = c.declareAndConsume(ctx, mqChan)
		if r.Err != nil {
			mqChanErr := mqChan.Close()
			if mqChanErr != nil && !errors.Is(mqChanErr, amqp.ErrClosed) {
				r.Err = errors.Join(r.Err, mqChanErr)
			}
		}

		select {
		case <-shouldLog:
			c.config.Log(ctx, slog.LevelWarn, logPrefix+" completed after it's context finished. It took %s. Err: %+v", time.Since(start), r.Err)
		default:
			respChan <- r
		}
	}()

	select {
	case <-ctx.Done():
		// Log our leaked goroutine's response whenever it finally finishes in case it has useful information.
		close(shouldLog)
		return nil, nil, fmt.Errorf(logPrefix+" unable to complete before context did due to %w", context.Cause(ctx))
	case r := <-respChan:
		return mqChan, r.Val, r.Err
	}
}

func (c *Consumer) declareAndConsume(ctx context.Context, mqChan *amqp.Channel) (_ <-chan amqp.Delivery, err error) {
	logPrefix := fmt.Sprintf("rmq.Consumer.declareAndConsume for queue (%s)", c.config.Queue.Name)

	if c.config.Qos != (Qos{}) {
		err = mqChan.Qos(c.config.Qos.PrefetchCount, c.config.Qos.PrefetchSize, c.config.Qos.Global)
		if err != nil {
			return nil, fmt.Errorf(logPrefix+" unable to set prefetch due to %w", err)
		} else if err = context.Cause(ctx); err != nil {
			return nil, fmt.Errorf(logPrefix+" failed to set Qos before context ended due to %w", err)
		}
	}

	queueDeclare := mqChan.QueueDeclare
	if c.config.Queue.Passive {
		queueDeclare = mqChan.QueueDeclarePassive
	}
	_, err = queueDeclare(
		c.config.Queue.Name,
		c.config.Queue.Durable,
		c.config.Queue.AutoDelete,
		c.config.Queue.Exclusive,
		c.config.Queue.NoWait,
		c.config.Queue.Args,
	)
	if err != nil {
		return nil, fmt.Errorf(logPrefix+" failed to declare queue due to %w", err)
	} else if err = context.Cause(ctx); err != nil {
		return nil, fmt.Errorf(logPrefix+" failed to declare queue before context ended due to %w", err)
	}

	for _, qb := range c.config.QueueBindings {
		err = mqChan.QueueBind(qb.QueueName, qb.RoutingKey, qb.ExchangeName, qb.NoWait, qb.Args)
		if err != nil {
			return nil, fmt.Errorf(logPrefix+" unable to bind queue (%s) to %s due to %w", qb.QueueName, qb.ExchangeName, err)
		} else if err = context.Cause(ctx); err != nil {
			return nil, fmt.Errorf(logPrefix+" failed to bind queues before context ended due to %w", err)
		}
	}

	// TODO: https://github.com/rabbitmq/amqp091-go/pull/192 has merged a Channel.ConsumeWithContext, which should be used here instead when we can
	deliveries, err := mqChan.Consume(
		c.config.Queue.Name,
		c.config.Consume.Consumer,
		c.config.Consume.AutoAck,
		c.config.Consume.Exclusive,
		c.config.Consume.NoLocal,
		c.config.Consume.NoWait,
		c.config.Consume.Args,
	)
	if err != nil {
		return nil, fmt.Errorf(logPrefix+" unable to consume due to %w", err)
	}

	return deliveries, nil
}

// Consume uses the rmq.Consumer config to declare and consume from an AMQP queue, forwarding deliveries to it's returned channel.
// On errors Consume reconnects to AMQP, redeclares and resumes consumption and forwarding of deliveries.
// Consume returns an unbuffered channel, and will block on sending to it if no ones listening.
// The returned channel is closed only after the context finishes and the amqp.Channel.Consume's Go channel delivers it's messages.
func (c *Consumer) Consume(ctx context.Context) <-chan amqp.Delivery {
	outChan := make(chan amqp.Delivery)
	go func() {
		logPrefix := fmt.Sprintf("rmq.Consumer.Consume for queue (%s)", c.config.Queue.Name)
		var delay time.Duration
		attempt := 0
		for {
			select {
			case <-ctx.Done():
				close(outChan)
				return
			case <-time.After(delay):
			}
			mqChan, inChan, err := c.safeDeclareAndConsume(ctx)
			if err != nil {
				delay = c.config.Delay(attempt)
				attempt++
				c.config.Log(ctx, slog.LevelError, logPrefix+" failed to safeDeclareAndConsume. Retrying in %s due to %v", delay.String(), err)
				continue
			}

			// Successfully redeclared our topology, so reset the backoff
			delay, attempt = 0, 0

			c.forwardDeliveries(ctx, mqChan, inChan, outChan)
		}
	}()
	return outChan
}

// forwardDeliveries forwards from inChan until it closes. If the context finishes it closes the amqp Channel so that the delivery channel will close after sending it's deliveries.
func (c *Consumer) forwardDeliveries(ctx context.Context, mqChan *amqp.Channel, inChan <-chan amqp.Delivery, outChan chan<- amqp.Delivery) {
	logPrefix := fmt.Sprintf("rmq.Consumer.forwardDeliveries for queue (%s)", c.config.Queue.Name)
	closeNotifier := mqChan.NotifyClose(make(chan *amqp.Error, 6))
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
			// The only thing blocked would be the rmq.Consumer.Consume goroutine listening for reconnects and logging errors, which seem unnecessary without a listener anyway.
			// Alls well since we don't lock up the entire amqp.Connection like streadway/amqp with Notify* channels...
			outChan <- msg
		}
	}
}

// ConsumeConcurrently simply runs the provided deliveryProcessor on each delivery from Consume in a new goroutine.
// maxGoroutines limits the amounts of goroutines spawned and defaults to 500.
// Qos.PrefetchCount can also limit goroutines spawned if deliveryProcessor properly Acks messages.
// Blocks until the context is finished and the Consume channel closes.
func (c *Consumer) ConsumeConcurrently(ctx context.Context, maxGoroutines uint64, deliveryProcessor func(ctx context.Context, msg amqp.Delivery)) {
	if maxGoroutines == 0 {
		maxGoroutines = 500
	}
	// We use a simple semaphore here and a new goroutine each time.
	// It may be more efficient to use a goroutine pool for small amounts of work, but a concerned caller can probably do it better themselves.
	semaphore := make(chan struct{}, maxGoroutines)
	deliverAndReleaseSemaphore := func(msg amqp.Delivery) { deliveryProcessor(ctx, msg); <-semaphore }
	for msg := range c.Consume(ctx) {
		semaphore <- struct{}{}
		go deliverAndReleaseSemaphore(msg)
	}
}
