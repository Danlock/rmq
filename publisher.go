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

type PublisherArgs struct {
	Args
	// NotifyReturn will receive amqp.Return's from any amqp.Channel this rmq.Publisher sends on.
	// Recommended to use a buffered channel. Closed after the publisher's context is done.
	NotifyReturn chan<- amqp.Return
	// LogReturns without their amqp.Return.Body using Args.Log when true
	LogReturns bool

	// DontConfirm means the Publisher's amqp.Channel won't be in Confirm mode. Methods except for Publish will throw an error.
	DontConfirm bool
}

type Publisher struct {
	ctx    context.Context
	config PublisherArgs
	in     chan *Publishing
}

// NewPublisher creates a rmq.Publisher that will publish messages to AMQP on a single amqp.Channel at a time.
// On error it reconnects via rmq.Connection. Shuts down when it's context is finished.
func NewPublisher(ctx context.Context, rmqConn *Connection, config PublisherArgs) *Publisher {
	if ctx == nil || rmqConn == nil {
		panic("rmq.NewPublisher called with nil ctx or rmqConn")
	}
	config.setDefaults()

	pub := &Publisher{
		ctx:    ctx,
		config: config,
		in:     make(chan *Publishing),
	}

	go pub.connect(rmqConn)
	return pub
}

// connect grabs an amqp.Channel from rmq.Connection. It does so repeatedly on any error until it's context finishes.
func (p *Publisher) connect(rmqConn *Connection) {
	logPrefix := "rmq.Publisher.connect"
	var delay time.Duration
	attempt := 0
	for {
		select {
		case <-p.ctx.Done():
			return
		case <-time.After(delay):
		}
		mqChan, err := rmqConn.Channel(p.ctx)
		if err != nil {
			delay = p.config.Delay(attempt)
			attempt++
			p.config.Log(p.ctx, slog.LevelError, logPrefix+" failed to get amqp.Channel. Retrying in %s due to err %+v", delay.String(), err)
			continue
		}
		if !p.config.DontConfirm {
			if err := mqChan.Confirm(false); err != nil {
				delay = p.config.Delay(attempt)
				attempt++
				p.config.Log(p.ctx, slog.LevelError, logPrefix+" failed to put amqp.Channel in confirm mode. Retrying in %s due to err %+v", delay.String(), err)
				continue
			}
		}

		// Successfully got a channel for publishing, reset delay
		delay, attempt = 0, 0
		p.handleReturns(mqChan)
		p.listen(mqChan)
	}
}

const dropReturnsAfter = 10 * time.Millisecond

// handleReturns echos the amqp.Channel's Return's until it closes
func (p *Publisher) handleReturns(mqChan *amqp.Channel) {
	logPrefix := "rmq.Publisher.handleReturns"
	if p.config.NotifyReturn == nil && !p.config.LogReturns {
		return
	}
	notifyReturns := mqChan.NotifyReturn(make(chan amqp.Return))
	go func() {
		dropTimer := time.NewTimer(0)
		<-dropTimer.C
		for r := range notifyReturns {
			if p.config.LogReturns {
				// A Body can be arbitrarily large and/or contain sensitve info. Don't log it by default.
				rBody := r.Body
				r.Body = nil
				p.config.Log(p.ctx, slog.LevelWarn, logPrefix+" got %+v", r)
				r.Body = rBody
			}
			if p.config.NotifyReturn == nil {
				continue
			}

			dropTimer.Reset(dropReturnsAfter)
			// Try not to repeat streadway/amqp's mistake of deadlocking if a client isn't listening to their Notify* channel.
			// (https://github.com/rabbitmq/amqp091-go/issues/18)
			// If they aren't listening to p.config.NotifyReturn, just drop the amqp.Return instead of deadlocking and leaking goroutines
			select {
			case p.config.NotifyReturn <- r:
				// Why is reusing a timer so bloody complicated... It's almost worth the timer leak just to reduce complexity
				if !dropTimer.Stop() {
					<-dropTimer.C
				}
			case <-dropTimer.C:
			}
		}
		// Close when the context is done, since we wont be sending anymore returns
		if p.config.NotifyReturn != nil && p.ctx.Err() != nil {
			close(p.config.NotifyReturn)
		}
	}()
}

// listen sends publishes on a amqp.Channel until it's closed.
func (p *Publisher) listen(mqChan *amqp.Channel) {
	logPrefix := "rmq.Publisher.listen"
	finishedPublishing := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(p.ctx)
	defer cancel()
	// Handle publishes in a separate goroutine so a slow publish won't lock up listen()
	go func() {
		for {
			select {
			case <-ctx.Done():
				close(finishedPublishing)
				return
			case pub := <-p.in:
				pub.publish(mqChan)
			}
		}
	}()

	notifyClose := mqChan.NotifyClose(make(chan *amqp.Error, 2))
	for {
		select {
		case <-p.ctx.Done():
			// Wait for publishing to finish since closing the channel in the middle of another channel request
			// tends to kill the entire connection with a "504 CHANNEL ERROR expected 'channel.open'"
			<-finishedPublishing
			if err := mqChan.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
				p.config.Log(p.ctx, slog.LevelError, logPrefix+" got an error while closing channel %v", err)
				return
			}
		case err, ok := <-notifyClose:
			if !ok {
				return
			} else if err != nil {
				p.config.Log(p.ctx, slog.LevelError, logPrefix+" got an amqp.Channel close err %v", err)
			}
		}
	}
}

type Publishing struct {
	amqp.Publishing
	Exchange   string
	RoutingKey string
	Mandatory  bool
	Immediate  bool

	// req is internal and private, which means it can't be set by callers.
	// This means it has the nice side effect of forcing callers to set struct fields when instantiating Publishing
	req internal.ChanReq[*amqp.DeferredConfirmation]
}

func (p *Publishing) publish(mqChan *amqp.Channel) {
	var resp internal.ChanResp[*amqp.DeferredConfirmation]
	resp.Val, resp.Err = mqChan.PublishWithDeferredConfirmWithContext(
		p.req.Ctx, p.Exchange, p.RoutingKey, p.Mandatory, p.Immediate, p.Publishing)
	p.req.RespChan <- resp
}

// Publish send a Publishing on rmq.Publisher's current amqp.Channel.
// Returns amqp.DefferedConfirmation's only if the rmq.Publisher has Confirm set.
// If an error is returned, rmq.Publisher will grab another amqp.Channel from rmq.Connection, which itself will redial AMQP if necessary.
// This means simply retrying Publish on errors will send Publishing's even on flaky connections.
func (p *Publisher) Publish(ctx context.Context, pub Publishing) (*amqp.DeferredConfirmation, error) {
	pub.req.Ctx = ctx
	pub.req.RespChan = make(chan internal.ChanResp[*amqp.DeferredConfirmation], 1)
	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("rmq.Publisher.Publish context done before publish sent %w", context.Cause(ctx))
	case <-p.ctx.Done():
		return nil, fmt.Errorf("rmq.Publisher context done before publish sent %w", context.Cause(p.ctx))
	case p.in <- &pub:
	}

	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("rmq.Publisher.Publish context done before publish completed %w", context.Cause(ctx))
	case <-p.ctx.Done():
		return nil, fmt.Errorf("rmq.Publisher context done before publish completed %w", context.Cause(p.ctx))
	case r := <-pub.req.RespChan:
		return r.Val, r.Err
	}
}

// PublishUntilConfirmed calls Publish and waits for Publishing to be confirmed.
// It republishes if a message isn't confirmed after confirmTimeout, or if Publish returns an error.
// Returns *amqp.DeferredConfirmation so the caller can check if it's Acked().
// confirmTimeout defaults to 1 minute. Recommended to call with context.WithTimeout.
func (p *Publisher) PublishUntilConfirmed(ctx context.Context, confirmTimeout time.Duration, pub Publishing) (*amqp.DeferredConfirmation, error) {
	logPrefix := "rmq.Publisher.PublishUntilConfirmed"

	if p.config.DontConfirm {
		return nil, fmt.Errorf(logPrefix + " called on a rmq.Publisher that's not in Confirm mode")
	}

	if confirmTimeout <= 0 {
		confirmTimeout = 15 * time.Second
	}

	var pubDelay time.Duration
	attempt := 0
	errs := make([]error, 0)

	for {
		defConf, err := p.Publish(ctx, pub)
		if err != nil {
			pubDelay = p.config.Delay(attempt)
			attempt++
			errs = append(errs, err)
			select {
			case <-ctx.Done():
				err = fmt.Errorf(logPrefix+" context done before the publish was sent %w", context.Cause(ctx))
				return defConf, errors.Join(append(errs, err)...)
			case <-time.After(pubDelay):
				continue
			}
		}
		attempt = 0

		confirmTimeout := time.NewTimer(confirmTimeout)
		defer confirmTimeout.Stop()

		select {
		case <-confirmTimeout.C:
			errs = append(errs, errors.New(logPrefix+" timed out waiting for confirm, republishing"))
			continue
		case <-ctx.Done():
			err = fmt.Errorf("rmq.Publisher.PublishUntilConfirmed context done before the publish was confirmed %w", context.Cause(ctx))
			return defConf, errors.Join(append(errs, err)...)
		case <-defConf.Done():
			return defConf, nil
		}
	}
}

// PublishUntilAcked is like PublishUntilConfirmed, but it also republishes nacks. User discretion is advised.
//
// Nacks can happen for a variety of reasons, ranging from user error (mistyped exchange) to RabbitMQ internal errors.
//
// PublishUntilAcked will republish a Mandatory Publishing with a nonexistent exchange forever (until the exchange exists), as one example.
// RabbitMQ acks Publishing's so monitor the NotifyReturn chan to ensure your Publishing's are being delivered.
//
// PublishUntilAcked is intended for ensuring a Publishing with a known destination queue will get acked despite flaky connections or temporary RabbitMQ node failures.
// Recommended to call with context.WithTimeout.
func (p *Publisher) PublishUntilAcked(ctx context.Context, confirmTimeout time.Duration, pub Publishing) error {
	logPrefix := "rmq.Publisher.PublishUntilAcked"
	nacks := 0
	for {
		defConf, err := p.PublishUntilConfirmed(ctx, confirmTimeout, pub)
		if err != nil {
			if nacks > 0 {
				return fmt.Errorf(logPrefix+" resent nacked Publishings %d time(s) and %w", nacks, err)
			}
			return err
		}

		if defConf.Acked() {
			return nil
		}

		nacks++
	}
}

// PublishBatchUntilAcked Publishes all of your Publishings at once, and then wait's for the DeferredConfirmation to be Acked,
// resending if it's been longer than confirmTimeout or if they've been nacked.
// confirmTimeout defaults to 1 minute. Recommended to call with context.WithTimeout.
func (p *Publisher) PublishBatchUntilAcked(ctx context.Context, confirmTimeout time.Duration, pubs ...Publishing) error {
	logPrefix := "rmq.Publisher.PublishBatchUntilConfirmed"

	if len(pubs) == 0 {
		return nil
	}
	if p.config.DontConfirm {
		return fmt.Errorf(logPrefix + " called on a rmq.Publisher that's not in Confirm mode")
	}

	if confirmTimeout == 0 {
		confirmTimeout = time.Minute
	}

	errs := make([]error, 0)
	pendingPubs := make([]*amqp.DeferredConfirmation, len(pubs))
	ackedPubs := make([]bool, len(pubs))

	for {
		select {
		case <-p.ctx.Done():
			err := fmt.Errorf(logPrefix+"'s Publisher timed out because %w", context.Cause(p.ctx))
			return errors.Join(append(errs, err)...)
		case <-ctx.Done():
			err := fmt.Errorf(logPrefix+" timed out because %w", context.Cause(ctx))
			return errors.Join(append(errs, err)...)
		default:
		}

		err := p.publishBatch(ctx, confirmTimeout, pubs, pendingPubs, ackedPubs, errs)
		if err == nil {
			return nil
		}
		clear(pendingPubs)
	}
}

// publishBatch publishes a slice of pubs once, waiting for them all to get acked.
// republishes on failure, returns after they've confirmed.
// blocks until context ends or confirmTimeout
func (p *Publisher) publishBatch(
	ctx context.Context,
	confirmTimeout time.Duration,
	pubs []Publishing,
	pendingPubs []*amqp.DeferredConfirmation,
	ackedPubs []bool,
	errs []error,
) (err error) {
	logPrefix := "rmq.Publisher.publishBatch"
	var delay time.Duration
	attempt := 0
	published := 0
	remaining := 0
	for _, acked := range ackedPubs {
		if !acked {
			remaining++
		}
	}

	for published != remaining {
		for i, pub := range pubs {
			// Skip if it's been previously acked or published
			if ackedPubs[i] || pendingPubs[i] != nil {
				continue
			}

			pendingPubs[i], err = p.Publish(ctx, pub)
			if err != nil {
				errs = append(errs, err)
				delay = p.config.Delay(attempt)
				attempt++
				select {
				case <-p.ctx.Done():
					return fmt.Errorf(logPrefix+"'s Publisher timed out because %w", context.Cause(p.ctx))
				case <-ctx.Done():
					return fmt.Errorf(logPrefix+" timed out because %w", context.Cause(ctx))
				case <-time.After(delay):
				}
			} else {
				published++
				attempt = 0
			}
		}
	}

	confirmTimer := time.After(confirmTimeout)
	confirmed := 0
	for confirmed != remaining {
		for i, pub := range pendingPubs {
			// Skip if it's been previously confirmed
			if ackedPubs[i] || pendingPubs[i] == nil {
				continue
			}

			select {
			case <-p.ctx.Done():
				return fmt.Errorf(logPrefix+"'s Publisher timed out because %w", context.Cause(p.ctx))
			case <-ctx.Done():
				return fmt.Errorf(logPrefix+" timed out because %w", context.Cause(ctx))
			case <-confirmTimer:
				return fmt.Errorf(logPrefix + " timed out waiting on confirms")
			case <-pub.Done():
				if pub.Acked() {
					ackedPubs[i] = true
				}
				confirmed++
				pendingPubs[i] = nil
			default:
			}
		}
	}

	return nil
}
