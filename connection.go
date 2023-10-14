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

type ConnectArgs struct {
	Args
	// Topology will be declared each connection to mitigate downed RabbitMQ nodes. Recommended to set, but not required.
	Topology Topology
	// DisableAMQP091Logs ensures the Connection's logs will not include rabbitmq/amqp091-go log's.
	// Note only the first Connection created will include rabbitmq/amqp091-go logs, due to rabbitmq/amqp091-go using a single global logger.
	DisableAMQP091Logs bool
}

func ConnectWithURLs(ctx context.Context, conf ConnectArgs, amqpURLs ...string) *Connection {
	if len(amqpURLs) == 0 {
		panic("ConnectWithURLs needs amqpURLs!")
	}
	return Connect(ctx, conf, func() (AMQPConnection, error) {
		errs := make([]error, 0, len(amqpURLs))
		for _, amqpURL := range amqpURLs {
			amqpConn, err := amqp.Dial(amqpURL)
			if err == nil {
				return amqpConn, nil
			}
			errs = append(errs, err)
		}
		return nil, errors.Join(errs...)
	})
}

func ConnectWithAMQPConfig(ctx context.Context, conf ConnectArgs, amqpURL string, amqpConf amqp.Config) *Connection {
	return Connect(ctx, conf, func() (AMQPConnection, error) {
		return amqp.DialConfig(amqpURL, amqpConf)
	})
}

var setAMQP091Logger sync.Once

// Connect returns a resilient, redialable AMQP connection that runs until it's context is canceled.
// Each Channel() call triggers rmq.Connection to return an amqp.Channel from it's CurrentConnection() or redial with the provided dialFn for a new AMQP Connection.
// ConnectWith* functions provide a few simple dialFn's for ease of use. They can be a simple wrapper around an amqp.Dial or much more complicated.
// If you want to ensure the Connection is working, call MustChannel with a timeout.
func Connect(ctx context.Context, conf ConnectArgs, dialFn func() (AMQPConnection, error)) *Connection {
	if dialFn == nil || ctx == nil {
		panic("Connect requires a ctx and a dialFn")
	}
	// Thread safely set the amqp091 logger once so it's included within danlock/rmq Connection Logs.
	// We use a sync.Once to avoid races, but this also means just the first Connection logs these errors.
	// It could be useful to have every Connection log these but practically that would lead to unneccessary log spam.
	// If this behaviour causes an issue your only solution is to set DisableAMQP091Logs and call amqp.SetLogger yourself
	if conf.Log != nil && !conf.DisableAMQP091Logs {
		setAMQP091Logger.Do(func() {
			amqp.SetLogger(internal.AMQP091Logger{ctx, conf.Log})
		})
	}
	conf.setDefaults()

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
		return t, fmt.Errorf("rmq.Connection context timed out because %w", context.Cause(connCtx))
	case <-ctx.Done():
		return t, fmt.Errorf("request context timed out because %w", context.Cause(ctx))
	case reqChan <- internal.ChanReq[T]{Ctx: ctx, RespChan: respChan}:
	}

	select {
	case <-connCtx.Done():
		return t, fmt.Errorf("rmq.Connection context timed out because %w", context.Cause(connCtx))
	case <-ctx.Done():
		return t, fmt.Errorf("request context timed out because %w", context.Cause(ctx))
	case resp := <-respChan:
		return resp.Val, resp.Err
	}
}

// Connection is a threadsafe, redialable wrapper around an amqp091.Connection
type Connection struct {
	ctx context.Context

	chanReqChan       chan internal.ChanReq[*amqp.Channel]
	currentConReqChan chan internal.ChanReq[AMQPConnection]

	config ConnectArgs
}

// Channel requests an AMQP channel from the current AMQP Connection.
// On errors the rmq.Connection will redial, and the caller is expected to call Channel() again for a new connection.
func (c *Connection) Channel(ctx context.Context) (*amqp.Channel, error) {
	ctx, cancel := context.WithTimeout(ctx, c.config.AMQPTimeout)
	defer cancel()
	return request(c.ctx, ctx, c.chanReqChan)
}

// MustChannel calls Channel on an active Connection until it's context times out or it successfully gets a Channel.
// Recommended to use context.WithTimeout.
func (c *Connection) MustChannel(ctx context.Context) (*amqp.Channel, error) {
	logPrefix := "rmq.Connection.MustChannel"
	errs := make([]error, 0)
	for {
		select {
		case <-ctx.Done():
			errs = append(errs, fmt.Errorf(logPrefix+" timed out due to %w", context.Cause(ctx)))
			return nil, errors.Join(errs...)
		case <-c.ctx.Done():
			errs = append(errs, fmt.Errorf(logPrefix+" Connection timed out due to %w", context.Cause(c.ctx)))
			return nil, errors.Join(errs...)
		default:
		}

		mqChan, err := c.Channel(ctx)
		if err != nil {
			errs = append(errs, err)
		} else {
			return mqChan, nil
		}
	}
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
		return nil, err
	}
	if conn.IsClosed() {
		return nil, amqp.ErrClosed
	}
	return conn, nil
}

func (c *Connection) redial(dialFn func() (AMQPConnection, error)) {
	logPrefix := "rmq.Connection.redial"
	var dialDelay time.Duration
	attempt := 0
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-time.After(dialDelay):
		}

		amqpConn, err := dialFn()
		if err != nil {
			dialDelay = c.config.Delay(attempt)
			attempt++
			c.config.Log(c.ctx, slog.LevelError, logPrefix+" failed, retrying after %s. err: %+v", dialDelay.String(), err)
			continue
		}
		logPrefix = fmt.Sprintf("rmq.Connection.redial's AMQPConnection (%s -> %s)", amqpConn.LocalAddr(), amqpConn.RemoteAddr())

		// Redeclare Topology if we have one. This has the bonus aspect of making sure the connection is actually usable, better than a Ping.
		if err := DeclareTopology(c.ctx, amqpConn, c.config.Topology); err != nil {
			dialDelay = c.config.Delay(attempt)
			attempt++
			c.config.Log(c.ctx, slog.LevelError, logPrefix+" DeclareTopology failed, retrying after %s. err: %+v", dialDelay.String(), err)
			continue
		}

		// After a successful dial and topology declare, reset our attempts and delay
		dialDelay, attempt = 0, 0

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
		// If our contexts timed out, close successfully created channels within this goroutine.
		if resp.Err == nil && resp.Val != nil && (c.ctx.Err() != nil || ctx.Err() != nil) {
			if err := resp.Val.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
				c.config.Log(ctx, slog.LevelError, logPrefix+" failed to close channel due to err: %+v", err)
			}
		} else {
			respChan <- resp
		}
	}()

	select {
	case <-c.ctx.Done():
		return nil, fmt.Errorf(logPrefix+" unable to complete before Connection %w", context.Cause(c.ctx))
	case <-ctx.Done():
		return nil, fmt.Errorf(logPrefix+" unable to complete before %w", context.Cause(ctx))
	case resp := <-respChan:
		return resp.Val, resp.Err
	}
}
