package rmq

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
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
	RemoteAddr() net.Addr
	Close() error
}

type ConnectConfig struct {
	// AMQPChannelTimeout will set a timeout on every Channel request.
	// If unset, please ensure calls to Channel contain a context with a timeout.
	AMQPChannelTimeout time.Duration
	// *RedialInterval are used to implement backoff when dialing AMQP. Defaults to 0.125 seconds to 32 seconds.
	MinRedialInterval, MaxRedialInterval time.Duration
	// Log can be left nil, set with slog.Log or wrapped around your favorite logging library
	Log func(ctx context.Context, level slog.Level, msg string, args ...any)
}

func ConnectWithAMQPConfig(ctx context.Context, conf ConnectConfig, amqpURL string, amqpConf amqp.Config) *RMQConnection {
	return Connect(ctx, conf, func() (AMQPConnection, error) {
		return amqp.DialConfig(amqpURL, amqpConf)
	})
}

// Connect returns a resilient, redialable AMQP connection that runs until it's context is canceled.
// Look at RMQPubilsher and RMQConsumer for practical examples of use,
// but essentially you grab an amqp.Channel, use it until theres an error, and then grab another amqp.Channel.
// Each Channel() call triggers RMQConnection to either return an amqp.Channel from it's CurrentConnection() or redial with the provided dialFn for a new AMQP Connection.
func Connect(ctx context.Context, conf ConnectConfig, dialFn func() (AMQPConnection, error)) *RMQConnection {
	if dialFn == nil || ctx == nil {
		panic("Connect requires a ctx and a dialFn")
	}
	internal.WrapLogFunc(&conf.Log)

	if conf.MinRedialInterval == 0 {
		conf.MinRedialInterval = time.Second / 8
	}
	if conf.MaxRedialInterval == 0 {
		conf.MaxRedialInterval = 32 * time.Second
	}

	conn := RMQConnection{
		ctx:               ctx,
		chanReqChan:       make(chan internal.ChanReq[*amqp.Channel]),
		currentConReqChan: make(chan internal.ChanReq[AMQPConnection]),

		config: conf,
	}

	go conn.redial(dialFn)

	return &conn
}

func request[T any](connCtx, ctx context.Context, reqChan chan internal.ChanReq[T]) (t T, _ error) {
	if ctx == nil {
		return t, fmt.Errorf("nil context")
	}
	respChan := make(chan internal.ChanResp[T], 1)
	select {
	case <-connCtx.Done():
		return t, fmt.Errorf("RMQConnection context finished: %w", context.Cause(connCtx))
	case <-ctx.Done():
		return t, context.Cause(ctx)
	case reqChan <- internal.ChanReq[T]{Ctx: ctx, RespChan: respChan}:
	}

	select {
	case <-connCtx.Done():
		return t, fmt.Errorf("RMQConnection context finished: %w", context.Cause(connCtx))
	case <-ctx.Done():
		return t, context.Cause(ctx)
	case resp := <-respChan:
		return resp.Val, resp.Err
	}
}

// RMQConnection is a threadsafe, redialable wrapper around an AMQPConnection.
type RMQConnection struct {
	ctx context.Context

	chanReqChan       chan internal.ChanReq[*amqp.Channel]
	currentConReqChan chan internal.ChanReq[AMQPConnection]

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

func (c *RMQConnection) redial(dialFn func() (AMQPConnection, error)) {
	var dialDelay time.Duration
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-time.After(dialDelay):
		}

		amqpConn, err := dialFn()
		if err != nil {
			// Configure our backoff according to parameters
			dialDelay = internal.CalculateDelay(c.config.MinRedialInterval, c.config.MaxRedialInterval, dialDelay)

			c.config.Log(c.ctx, slog.LevelError, "RMQConnection dialFn failed, retrying after %s. err: %+v", dialDelay.String(), err)
			continue
		}
		// After a successful dial, reset our dial delay
		dialDelay = 0

		c.listen(amqpConn)
	}
}

// listen listens and responds to Channel and Connection requests. It returns on any failure to prompt another redial.
func (c *RMQConnection) listen(amqpConn AMQPConnection) {
	logPrefix := fmt.Sprintf("RMQConnection's AMQPConnection (%s -> %s)", amqpConn.LocalAddr(), amqpConn.RemoteAddr())
	notifyClose := amqpConn.NotifyClose(make(chan *amqp.Error, 1))
	for {
		select {
		case <-c.ctx.Done():
			// RMQConnection is shutting down, close the connection on our way out
			if err := amqpConn.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
				c.config.Log(c.ctx, slog.LevelError, logPrefix+" failed to close due to err: %+v", err)
			}
			return
		case err := <-notifyClose:
			if err != nil {
				c.config.Log(c.ctx, slog.LevelError, logPrefix+" received close notification err: %+v", err)
			}
			return
		case connReq := <-c.currentConReqChan:
			connReq.RespChan <- internal.ChanResp[AMQPConnection]{Val: amqpConn}
		case chanReq := <-c.chanReqChan:
			// Channel() desperately needs a context since RabbitMQ (or the network... or anything else) can and will block during Channel() requests
			// Leaking a blocked Channel call on timed out contexts is the best we can do from here
			respChan := make(chan internal.ChanResp[*amqp.Channel], 1)
			go func() {
				var resp internal.ChanResp[*amqp.Channel]
				resp.Val, resp.Err = amqpConn.Channel()
				respChan <- resp
			}()

			select {
			case <-c.ctx.Done():
				// RMQConnection is shutting down, close the connection on our way out
				if err := amqpConn.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
					c.config.Log(c.ctx, slog.LevelError, logPrefix+"  failed to close due to err: %+v", err)
				}
				return
			case resp := <-respChan:
				chanReq.RespChan <- resp
				if resp.Err != nil {
					return
				}
			case <-chanReq.Ctx.Done():
				chanReq.RespChan <- internal.ChanResp[*amqp.Channel]{Err: context.Cause(chanReq.Ctx)}
			}
		}
	}
}
