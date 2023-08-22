package rmq

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/danlock/rmq/internal"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Publishing struct {
	amqp.Publishing
	Exchange   string
	RoutingKey string
	Mandatory  bool
	Immediate  bool

	// req is internal and private, which means it can't be set by callers.
	// This means it has the nice side effect of forcing callers to set struct fields when instantiating Publishing,
	// which makes adding fields to Publishing backwards compatible
	req internal.ChanReq[*amqp.DeferredConfirmation]
}

const pkgHeader = "github.com/danlock/rmq.RMQPublisher.PublishUntilAcked"

// makeID sets a field within the Publishing.Header so PublishUntilAcked can identify returned Publishing's.
func (p *Publishing) makeID() string {
	if p.Headers == nil {
		p.Headers = make(amqp.Table, 1)
	}
	id := fmt.Sprintf("%s|%p", time.Now().Format(time.RFC3339Nano), p)
	p.Headers[pkgHeader] = id
	return id
}

func (p *Publishing) publish(mqChan *amqp.Channel) {
	resp := internal.ChanResp[*amqp.DeferredConfirmation]{}
	resp.Val, resp.Err = mqChan.PublishWithDeferredConfirmWithContext(
		p.req.Ctx, p.Exchange, p.RoutingKey, p.Mandatory, p.Immediate, p.Publishing)
	p.req.RespChan <- resp
}

type PublisherConfig struct {
	// NotifyReturn will recieve amqp.Return's from any amqp.Channel this RMQPublisher sends on.
	// NotifyReturn will be closed once the RMQPublisher's context finishes.
	// NotifyReturn must be actively listened to in order to reliably receive amqp.Return's
	NotifyReturn chan<- amqp.Return

	// DontConfirm will not set the amqp.Channel in Confirm mode, and disallow PublishUntilConfirmed.
	DontConfirm bool

	// Set Logf with your favorite logging library
	Logf func(msg string, args ...any)
	// *RetryInterval controls how frequently RMQPublisher retries on errors. Defaults from 0.125 seconds to 32 seconds.
	MinRetryInterval, MaxRetryInterval time.Duration
}

type RMQPublisher struct {
	ctx                 context.Context
	config              PublisherConfig
	in                  chan *Publishing
	listeningForReturns chan bool
	requestReturns      chan internal.ChanReq[[]amqp.Return]
}

// NewPublisher creates a RMQPublisher that will publish messages to AMQP on a single amqp.Channel at a time.
// On any error such as Channel or Connection closes, it will get a new Channel, which redials AMQP if necessary.
func NewPublisher(ctx context.Context, rmqConn *RMQConnection, config PublisherConfig) *RMQPublisher {
	if ctx == nil || rmqConn == nil {
		panic("rmq.NewPublisher called with nil ctx or rmqConn")
	}

	if config.Logf == nil {
		config.Logf = func(msg string, args ...any) {}
	}
	if config.MinRetryInterval == 0 {
		config.MinRetryInterval = time.Second / 8
	}
	if config.MaxRetryInterval == 0 {
		config.MaxRetryInterval = 32 * time.Second
	}

	pub := &RMQPublisher{
		ctx:                 ctx,
		config:              config,
		in:                  make(chan *Publishing),
		listeningForReturns: make(chan bool),
		requestReturns:      make(chan internal.ChanReq[[]amqp.Return]),
	}

	returnChan := make(chan amqp.Return)
	go pub.storeReturns(returnChan)
	go pub.connect(rmqConn, returnChan)
	return pub
}

// connect grabs an amqp.Channel from RMQConnection. It does so repeatedly on any error until it's context finishes.
func (p *RMQPublisher) connect(rmqConn *RMQConnection, returnChan chan amqp.Return) {
	logPrefix := "RMQPublisher.connect"
	var delay time.Duration

	for {
		select {
		case <-p.ctx.Done():
			return
		case <-time.After(delay):
		}

		mqChan, err := rmqConn.Channel(p.ctx)
		if err != nil {
			delay = internal.CalculateDelay(p.config.MinRetryInterval, p.config.MaxRetryInterval, delay)
			p.config.Logf(logPrefix+" failed to get amqp.Channel. Retrying in %s due to err %+v", delay.String(), err)
			continue
		}

		if !p.config.DontConfirm {
			if err := mqChan.Confirm(false); err != nil {
				delay = internal.CalculateDelay(p.config.MinRetryInterval, p.config.MaxRetryInterval, delay)
				p.config.Logf(logPrefix+" failed to put amqp.Channel in confirm mode. Retrying in %s due to err %+v", delay.String(), err)
				continue
			}
		}

		// Successfully got a channel for publishing, reset delay
		delay = 0

		// Spin off a goroutine that echos this amqp.Channel's Return's until the amqp.Channel closes
		go func() {
			for r := range mqChan.NotifyReturn(make(chan amqp.Return)) {
				returnChan <- r
			}
		}()

		p.listen(mqChan)
	}
}

// listen sends publishes on a amqp.Channel until it's closed.
func (p *RMQPublisher) listen(mqChan *amqp.Channel) {
	logPrefix := "RMQPublisher.listen"
	notifyClose := mqChan.NotifyClose(make(chan *amqp.Error, 2))

	ctx, cancel := context.WithCancel(p.ctx)
	defer cancel()
	// Handle publishes in a separate goroutine so a slow publish won't lock up listen()
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case pub := <-p.in:
				pub.publish(mqChan)
			}
		}
	}()

	for {
		select {
		case <-p.ctx.Done():
			if err := mqChan.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
				p.config.Logf(logPrefix+" got an error while closing channel %v", err)
				return
			}
		case err, ok := <-notifyClose:
			if !ok {
				return
			} else if err != nil {
				p.config.Logf(logPrefix+" got an amqp.Channel close err %v", err)
			}
		}
	}
}

// storeReturns stores and provides amqp.Return's for PublishUntilAcked, and echos them on NotifyReturn if set.
func (p *RMQPublisher) storeReturns(returnChan <-chan amqp.Return) {
	returns := make([]amqp.Return, 0)

	listeners := 0
	for {
		select {
		case <-p.ctx.Done():
			// storeReturns shuts down only once it has no listeners
			if listeners == 0 {
				if p.config.NotifyReturn != nil {
					close(p.config.NotifyReturn)
				}
				return
			}
		case ret := <-returnChan:
			if listeners > 0 {
				returns = append(returns, ret)
			}
			if p.config.NotifyReturn != nil {
				// Try not to repeat streadway/amqp's mistake of deadlocking if a client isn't listening to their Notify* channel.
				// (https://github.com/rabbitmq/amqp091-go/issues/18)
				// Echo the return to the caller if they are listening. If they aren't listening, they don't get their amqp.Return.
				// Should have been listening...
				select {
				case p.config.NotifyReturn <- ret:
				default:
				}
			}
		case req := <-p.requestReturns:
			var resp internal.ChanResp[[]amqp.Return]
			if len(returns) > 0 {
				resp.Val = make([]amqp.Return, len(returns))
				copy(resp.Val, returns)
			}
			req.RespChan <- resp
		case listening := <-p.listeningForReturns:
			if listening {
				listeners++
			} else {
				listeners--

				if listeners == 0 {
					if p.ctx.Err() != nil {
						if p.config.NotifyReturn != nil {
							close(p.config.NotifyReturn)
						}
						return
					}
					// Whenever no ones looking, clear returns so it can't grow forever
					if len(returns) > 0 {
						returns = returns[0:0]
					}
				}
			}

		}
	}
}

// Publish send a Publishing on RMQPublisher's current amqp.Channel.
// Returns amqp.DefferedConfirmation's only if the RMQPublisher has Confirm set.
// If an error is returned, RMQPublisher will grab another amqp.Channel from RMQConnection, which itself will redial AMQP if necessary.
// This means simply retrying Publish on errors will send Publishing's even on flaky connections.
func (p *RMQPublisher) Publish(ctx context.Context, pub Publishing) (*amqp.DeferredConfirmation, error) {
	pub.req.Ctx = ctx
	pub.req.RespChan = make(chan internal.ChanResp[*amqp.DeferredConfirmation], 1)
	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("RMQPublisher.Publish context done before publish sent %w", context.Cause(ctx))
	case <-p.ctx.Done():
		return nil, fmt.Errorf("RMQPublisher context done before publish sent %w", context.Cause(p.ctx))
	case p.in <- &pub:
	}

	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("RMQPublisher.Publish context done before publish completed %w", context.Cause(ctx))
	case <-p.ctx.Done():
		return nil, fmt.Errorf("RMQPublisher context done before publish completed %w", context.Cause(p.ctx))
	case r := <-pub.req.RespChan:
		return r.Val, r.Err
	}
}

// PublishUntilConfirmed calls Publish and waits for Publishing to be confirmed.
// It republishes if a message isn't confirmed after ConfirmTimeout, or if Publish returns an error.
// Returns *amqp.DeferredConfirmation so the caller can check if it's Acked().
// Recommended to call with context.WithTimeout.
func (p *RMQPublisher) PublishUntilConfirmed(ctx context.Context, confirmTimeout time.Duration, pub Publishing) (*amqp.DeferredConfirmation, error) {
	logPrefix := "RMQPublisher.PublishUntilConfirmed"

	if p.config.DontConfirm {
		return nil, fmt.Errorf(logPrefix + " called on a RMQPublisher that's not in Confirm mode")
	}

	if confirmTimeout <= 0 {
		confirmTimeout = 15 * time.Second
	}

	var pubDelay time.Duration
	for {
		defConf, err := p.Publish(ctx, pub)
		if err != nil {
			pubDelay = internal.CalculateDelay(p.config.MinRetryInterval, p.config.MaxRetryInterval, pubDelay)
			p.config.Logf(logPrefix+" got a Publish error. Republishing due to %v", err)
			select {
			case <-ctx.Done():
				return defConf, fmt.Errorf(logPrefix+" context done before the publish was sent %w", context.Cause(ctx))
			case <-time.After(pubDelay):
				continue
			}
		}
		// reset the delay on success
		pubDelay = 0

		confirmTimeout := time.NewTimer(confirmTimeout)
		defer confirmTimeout.Stop()

		select {
		case <-confirmTimeout.C:
			p.config.Logf(logPrefix + " timed out waiting for confirm, republishing")
			continue
		case <-ctx.Done():
			return defConf, fmt.Errorf("RMQPublisher.PublishUntilConfirmed context done before the publish was confirmed %w", context.Cause(ctx))
		case <-defConf.Done():
			return defConf, nil
		}
	}
}

// defaultWasReturned checks if the Publishing is within the []amqp.Return by checking our preset ID from Publishing.makeID().
func (p *RMQPublisher) defaultWasReturned(pubID string) func([]amqp.Return, Publishing) bool {
	return func(returns []amqp.Return, _ Publishing) bool {
		return slices.ContainsFunc(returns, func(ret amqp.Return) bool {
			if ret.Headers == nil {
				return false
			}
			retID, ok := ret.Headers[pkgHeader].(string)
			if !ok {
				return false
			}
			return pubID == retID
		})
	}
}

var ErrPublishReturned = errors.New("RMQPublisher.PublishUntilAcked got a return")

type PublishUntilAckedConfig struct {
	// ConfirmTimeout defaults to 15 seconds
	ConfirmTimeout time.Duration
	// WasReturned correlates a Return and a Publishing so PublishUntilAcked won't republish it.
	// True if Publishing was returned, false if not.
	WasReturned func([]amqp.Return, Publishing) bool
}

// PublishUntilAcked is like PublishUntilConfirmed, but it republishes if nacked as well. User discretion is advised.
//
// Nacks can happen for a variety of reasons, ranging from user error (mistyped exchange) or RabbitMQ internal errors.
//
// PublishUntilAcked will republish a Mandatory Publishing with a nonexistent exchange forever (until the exchange exists), as one example.
// Only if the message is returned will PublishUntilAcked error out with ErrPublishReturned.
// PublishUntilAcked is intended for ensuring a Publishing with a known destination queue will get acked despite flaky connections or temporary RabbitMQ node failures.
// Recommended to call with context.WithTimeout.
//
// If WasReturned isn't set, an amqp.Publishing.Header field will be used to correlate amqp.Return's by default.
func (p *RMQPublisher) PublishUntilAcked(ctx context.Context, cfg PublishUntilAckedConfig, pub Publishing) error {
	logPrefix := "RMQPublisher.PublishUntilAcked"
	// make storeReturns() actually store returns
	select {
	case <-p.ctx.Done():
		return fmt.Errorf("RMQPublisher context done %w", context.Cause(p.ctx))
	case <-ctx.Done():
		return fmt.Errorf(logPrefix+" context done %w", context.Cause(p.ctx))
	case p.listeningForReturns <- true:
	}

	defer func() { p.listeningForReturns <- false }()

	if cfg.WasReturned == nil {
		cfg.WasReturned = p.defaultWasReturned(pub.makeID())
	}
	nacks := 0
	for {
		defConf, err := p.PublishUntilConfirmed(ctx, cfg.ConfirmTimeout, pub)
		if err != nil {
			return err
		}

		if defConf.Acked() {
			return nil
		}

		// Grab returns from the storeReturns() goroutine and check if our Publishing was returned.
		req := internal.ChanReq[[]amqp.Return]{Ctx: ctx, RespChan: make(chan internal.ChanResp[[]amqp.Return], 1)}
		p.requestReturns <- req
		resp := <-req.RespChan

		if cfg.WasReturned(resp.Val, pub) {
			return ErrPublishReturned
		}

		nacks++
		p.config.Logf("RMQPublisher.PublishUntilAcked resending Publishing that has been nacked %d time(s)...", nacks)
		// There isn't a delay here since PublishUntilConfirmed waiting for the confirm should effectively slow us down to what can be handled by the AMQP server.
	}
}
