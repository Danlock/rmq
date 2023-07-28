package rmq

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/danlock/rmq/internal"
	amqp "github.com/rabbitmq/amqp091-go"
)

// AMQPConnection abstracts an amqp091.Connection
type AMQPConnection interface {
	Channel() (*amqp.Channel, error)
	NotifyClose(receiver chan *amqp.Error) chan *amqp.Error
	LocalAddr() net.Addr
	Close() error
}

type ConnectConfig struct {
	// AMQPChannelTimeout will set a timeout on every Channel request.
	// If unset, the only thing stopping a Channel request from blocking indefinitely is the context passed into Channel.
	AMQPChannelTimeout time.Duration
	// *RedialInterval are used to implement backoff when dialing AMQP. Defaults to 0.125 seconds to 32 seconds.
	MinRedialInterval, MaxRedialInterval time.Duration
	// Set Logf with your favorite logging library
	Logf func(msg string, args ...any)
}

func ConnectWithAMQPConfig(ctx context.Context, conf ConnectConfig, amqpURL string, amqpConf amqp.Config) *RMQConnection {
	dialFn := func() (AMQPConnection, error) {
		return amqp.DialConfig(amqpURL, amqpConf)
	}
	return Connect(ctx, conf, dialFn)
}

// Connect returns a resilient, redialable AMQP connection.
// This connection will redial until it's context is canceled.
// Calling code should repeatedly call the Channel() function on any errors.
// That will trigger RMQConnection to either return a Channel or redial for a new connection.
func Connect(ctx context.Context, conf ConnectConfig, dialFn func() (AMQPConnection, error)) *RMQConnection {
	if dialFn == nil || ctx == nil {
		panic("Connect requires a ctx and a dialFn")
	}
	if conf.Logf == nil {
		// Nobody cares about RMQConnection's problems...
		conf.Logf = func(string, ...any) {}
	}
	if conf.MinRedialInterval == 0 {
		conf.MinRedialInterval = time.Second / 8
	}
	if conf.MaxRedialInterval == 0 {
		conf.MaxRedialInterval = 32 * time.Second
	}

	conn := RMQConnection{
		ctx:               ctx,
		dialFn:            dialFn,
		chanReqChan:       make(chan connReq[*amqp.Channel]),
		currentConReqChan: make(chan connReq[AMQPConnection]),

		config: conf,
	}

	go conn.redial()

	return &conn
}

func request[T any](connCtx, ctx context.Context, reqChan chan connReq[T]) (t T, _ error) {
	if ctx == nil {
		return t, fmt.Errorf("nil context")
	}
	respChan := make(chan connResp[T], 1)
	select {
	case <-connCtx.Done():
		return t, fmt.Errorf("RMQConnection context finished: %w", context.Cause(connCtx))
	case <-ctx.Done():
		return t, context.Cause(ctx)
	case reqChan <- connReq[T]{ctx: ctx, respChan: respChan}:
	}

	select {
	case <-connCtx.Done():
		return t, fmt.Errorf("RMQConnection context finished: %w", context.Cause(connCtx))
	case <-ctx.Done():
		return t, context.Cause(ctx)
	case resp := <-respChan:
		return resp.val, resp.err
	}
}

type connReq[T any] struct {
	ctx      context.Context
	respChan chan connResp[T]
}
type connResp[T any] struct {
	val T
	err error
}

// RMQConnection is a threadsafe, redialable wrapper around an AMQPConnection.
type RMQConnection struct {
	ctx               context.Context
	dialFn            func() (AMQPConnection, error)
	chanReqChan       chan connReq[*amqp.Channel]
	currentConReqChan chan connReq[AMQPConnection]

	config ConnectConfig
}

// Channel requests an AMQP channel from the current AMQP Connection if the context isn't finished.
// RabbitMQ can and will block on Channel requests, so it is recommended to either set ConnectConfig.AMQPChannelTimeout,
// or call this function with a appropiate context.
// On errors the RMQConnection will redial, and the caller is expected to call this again.
func (c *RMQConnection) Channel(ctx context.Context) (*amqp.Channel, error) {
	if c.config.AMQPChannelTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.config.AMQPChannelTimeout)
		defer cancel()
	}
	return request(c.ctx, ctx, c.chanReqChan)
}

// CurrentConnection requests the current AMQPConnection being used if the context isn't finished.
// It can be typecasted into an *amqp091.Connection.
// Use this if you want to create NotifyClose or NotifyBlocked channels for example.
func (c *RMQConnection) CurrentConnection(ctx context.Context) (AMQPConnection, error) {
	return request(c.ctx, ctx, c.currentConReqChan)
}

func (c *RMQConnection) redial() {
	var dialDelay time.Duration
	for {
		select {
		case <-c.ctx.Done():
			c.config.Logf("RMQConnection finishing due to context: %+v", context.Cause(c.ctx))
			return
		case <-time.After(dialDelay):
		}

		amqpConn, err := c.dialFn()
		if err != nil {
			// Configure our backoff according to parameters
			dialDelay = internal.CalculateDelay(c.config.MinRedialInterval, c.config.MaxRedialInterval, dialDelay)

			c.config.Logf("RMQConnection dialFn failed, retrying after %s. err: %+v", dialDelay.String(), err)
			continue
		}
		// After a successful dial, reset our dial delay
		dialDelay = 0

		c.listen(amqpConn)
	}
}

// listen listens and responds to Channel and Connection requests. It returns on any failure to prompt another redial.
func (c *RMQConnection) listen(amqpConn AMQPConnection) {
	notifyClose := amqpConn.NotifyClose(make(chan *amqp.Error, 1))
	for {
		select {
		case <-c.ctx.Done():
			// RMQConnection is shutting down, close the connection on our way out
			if err := amqpConn.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
				c.config.Logf("RMQConnection's AMQPConnection (%s) failed to close due to err: %+v", amqpConn.LocalAddr().String(), err)
			}
			return
		case err := <-notifyClose:
			c.config.Logf("RMQConnection's AMQPConnection (%s) recieved close notification err: %+v", amqpConn.LocalAddr().String(), err)
			return
		case connReq := <-c.currentConReqChan:
			connReq.respChan <- connResp[AMQPConnection]{val: amqpConn}
		case chanReq := <-c.chanReqChan:
			// Channel() desperately needs a context since RabbitMQ (or the network... or anything else) can and will block during Channel() requests
			// Leaking a blocked Channel call on timed out contexts is the best we can do from here
			respChan := make(chan connResp[*amqp.Channel], 1)
			go func() {
				var resp connResp[*amqp.Channel]
				resp.val, resp.err = amqpConn.Channel()
				respChan <- resp
			}()

			select {
			case <-c.ctx.Done():
				// RMQConnection is shutting down, close the connection on our way out
				if err := amqpConn.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
					c.config.Logf("RMQConnection's AMQPConnection (%s) failed to close due to err: %+v", amqpConn.LocalAddr().String(), err)
				}
				return
			case err := <-notifyClose:
				c.config.Logf("RMQConnection's AMQPConnection (%s) recieved close notification err: %+v", amqpConn.LocalAddr().String(), err)
				return
			case resp := <-respChan:
				chanReq.respChan <- resp
				if resp.err != nil {
					return
				}
			case <-chanReq.ctx.Done():
				chanReq.respChan <- connResp[*amqp.Channel]{err: context.Cause(chanReq.ctx)}
			}
		}
	}
}
