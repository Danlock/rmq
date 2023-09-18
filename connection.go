package rmq

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
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
	CloseDeadline(time.Time) error
	IsClosed() bool
}

type ConnectConfig struct {
	// Topology will be declared each connection to mitigate downed RabbitMQ nodes. Recommended to set, but not required.
	Topology Topology
	// AMQPTimeout will set a timeout on AMQP operations. Defaults to 1 minute.
	AMQPTimeout time.Duration
	// *RedialInterval implements backoff on AMQP dials. Defaults to 0.125 seconds to 32 seconds.
	MinRedialInterval, MaxRedialInterval time.Duration
	// Log can be left nil, set with slog.Log or wrapped around your favorite logging library
	Log func(ctx context.Context, level slog.Level, msg string, args ...any)
}

func ConnectWithURL(ctx context.Context, conf ConnectConfig, amqpURL string) *Connection {
	return Connect(ctx, conf, func() (AMQPConnection, error) {
		return amqp.Dial(amqpURL)
	})
}

func ConnectWithAMQPConfig(ctx context.Context, conf ConnectConfig, amqpURL string, amqpConf amqp.Config) *Connection {
	return Connect(ctx, conf, func() (AMQPConnection, error) {
		return amqp.DialConfig(amqpURL, amqpConf)
	})
}

var setAMQP091Logger sync.Once

// Connect returns a resilient, redialable AMQP connection that runs until it's context is canceled.
// Look at rmq.Pubilsher and rmq.Consumer for practical examples of use,
// but the idea is to call Channel and use it until error. Then rinse and repeat.
// Each Channel() call triggers rmq.Connection to return an amqp.Channel from it's CurrentConnection() or redial with the provided dialFn for a new AMQP Connection.
func Connect(ctx context.Context, conf ConnectConfig, dialFn func() (AMQPConnection, error)) *Connection {
	if dialFn == nil || ctx == nil {
		panic("Connect requires a ctx and a dialFn")
	}
	// Thread safely set the amqp091 logger so it's included within danlock/rmq Logs.
	if conf.Log != nil {
		setAMQP091Logger.Do(func() {
			amqp.SetLogger(internal.AMQP091Logger{ctx, conf.Log})
		})
	}
	internal.WrapLogFunc(&conf.Log)

	if conf.AMQPTimeout == 0 {
		conf.AMQPTimeout = time.Minute
	}

	if conf.MinRedialInterval == 0 {
		conf.MinRedialInterval = time.Second / 8
	}
	if conf.MaxRedialInterval == 0 {
		conf.MaxRedialInterval = 32 * time.Second
	}

	conn := Connection{
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
		return t, fmt.Errorf("rmq.Connection context finished: %w", context.Cause(connCtx))
	case <-ctx.Done():
		return t, context.Cause(ctx)
	case reqChan <- internal.ChanReq[T]{Ctx: ctx, RespChan: respChan}:
	}

	select {
	case <-connCtx.Done():
		return t, fmt.Errorf("rmq.Connection context finished: %w", context.Cause(connCtx))
	case <-ctx.Done():
		return t, context.Cause(ctx)
	case resp := <-respChan:
		return resp.Val, resp.Err
	}
}

// Connection is a threadsafe, redialable wrapper around an amqp091.Connection
type Connection struct {
	ctx context.Context

	chanReqChan       chan internal.ChanReq[*amqp.Channel]
	currentConReqChan chan internal.ChanReq[AMQPConnection]

	config ConnectConfig
}

// Channel requests an AMQP channel from the current AMQP Connection.
// Recommended to set ConnectConfig.AMQPChannelTimeout or use context.WithTimeout.
// On errors the rmq.Connection will redial, and the caller is expected to call Channel() again for a new connection.
func (c *Connection) Channel(ctx context.Context) (*amqp.Channel, error) {
	ctx, cancel := context.WithTimeout(ctx, c.config.AMQPTimeout)
	defer cancel()
	return request(c.ctx, ctx, c.chanReqChan)
}

// CurrentConnection requests the current AMQPConnection being used by rmq.Connection.
// It can be typecasted into an *amqp091.Connection.
// Useful for making NotifyClose or NotifyBlocked channels for example.
// If the CurrentConnection is closed, this function will return amqp.ErrClosed
// until rmq.Connection dials successfully for another one.
func (c *Connection) CurrentConnection(ctx context.Context) (AMQPConnection, error) {
	ctx, cancel := context.WithTimeout(ctx, c.config.AMQPTimeout)
	defer cancel()
	conn, err := request(c.ctx, ctx, c.currentConReqChan)
	if err != nil {
		return conn, err
	}
	if conn.IsClosed() {
		return conn, amqp.ErrClosed
	}
	return conn, err
}

func (c *Connection) redial(dialFn func() (AMQPConnection, error)) {
	logPrefix := "rmq.Connection.redial"
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
			c.config.Log(c.ctx, slog.LevelError, logPrefix+" failed, retrying after %s. err: %+v", dialDelay.String(), err)
			continue
		}

		// Redeclare Topology if we have one. This has the bonus aspect of making sure the connection is actually usable, better than a Ping.
		if err := DeclareTopology(c.ctx, amqpConn, c.config.Topology); err != nil {
			dialDelay = internal.CalculateDelay(c.config.MinRedialInterval, c.config.MaxRedialInterval, dialDelay)
			c.config.Log(c.ctx, slog.LevelError, logPrefix+" DeclareTopology failed, retrying after %s. err: %+v", dialDelay.String(), err)
			continue
		}

		// After a successful dial and topology declare, reset our dial delay
		dialDelay = 0

		c.listen(amqpConn)
	}
}

// listen listens and responds to Channel and Connection requests. It returns on any failure to prompt another redial.
func (c *Connection) listen(amqpConn AMQPConnection) {
	logPrefix := fmt.Sprintf("rmq.Connection's AMQPConnection (%s -> %s)", amqpConn.LocalAddr(), amqpConn.RemoteAddr())
	notifyClose := amqpConn.NotifyClose(make(chan *amqp.Error, 1))
	for {
		select {
		case <-c.ctx.Done():
			if err := amqpConn.CloseDeadline(time.Now().Add(c.config.AMQPTimeout)); err != nil && !errors.Is(err, amqp.ErrClosed) {
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
			var resp internal.ChanResp[*amqp.Channel]
			resp.Val, resp.Err = c.safeChannel(chanReq.Ctx, amqpConn)
			chanReq.RespChan <- resp
			if resp.Err != nil {
				// redial on failed Channel requests
				return
			}
		}
	}
}

// safeChannel calls amqp.Connection.Channel with a timeout by launching it in a separate goroutine and waiting for the response.
// This is inefficient, results in a leaked goroutine on timeout, but is the best we can do until amqp091 adds a context to the function.
func (c *Connection) safeChannel(ctx context.Context, amqpConn AMQPConnection) (*amqp.Channel, error) {
	logPrefix := "rmq.Connection.safeChannel"
	respChan := make(chan internal.ChanResp[*amqp.Channel], 1)
	go func() {
		var resp internal.ChanResp[*amqp.Channel]
		resp.Val, resp.Err = amqpConn.Channel()
		respChan <- resp
	}()

	select {
	case <-c.ctx.Done(): // Close the current amqp.Connection when rmq.Connection is shutting down
		if err := amqpConn.CloseDeadline(time.Now().Add(c.config.AMQPTimeout)); err != nil && !errors.Is(err, amqp.ErrClosed) {
			c.config.Log(c.ctx, slog.LevelError, logPrefix+" failed to close connection due to err: %+v", err)
		}
		return nil, fmt.Errorf(logPrefix+" unable to complete before %w", context.Cause(c.ctx))
	case <-ctx.Done():
		return nil, fmt.Errorf(logPrefix+" unable to complete before %w", context.Cause(ctx))
	case resp := <-respChan:
		return resp.Val, resp.Err
	}
}
