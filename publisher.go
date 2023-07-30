package rmq

import (
	"context"
	"fmt"
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

	req internal.ChanReq[*amqp.DeferredConfirmation]
}

func (pub *Publishing) publish(mqChan *amqp.Channel) {
	var resp internal.ChanResp[*amqp.DeferredConfirmation]
	resp.Val, resp.Err = mqChan.PublishWithDeferredConfirmWithContext(
		pub.req.Ctx, pub.Exchange, pub.RoutingKey, pub.Mandatory, pub.Immediate, pub.Publishing)
	pub.req.RespChan <- resp
}

type PublisherConfig struct {
	// amqp.Channel has several Notify* functions that listen for various errors/messages.
	// Set these channels to get notifications from the current amqp.Channel RMQPublisher is using.
	NotifyReturn chan<- amqp.Return
	NotifyFlow   chan<- bool

	// DontConfirm will not set the amqp.Channel in Confirm mode, and disallow PublishUntilConfirmed.
	DontConfirm bool

	// Set Logf with your favorite logging library
	Logf func(msg string, args ...any)
	// *RetryInterval controls how frequently RMQPublisher retries on errors. Defaults from 0.125 seconds to 32 seconds.
	MinRetryInterval, MaxRetryInterval time.Duration
}

type RMQPublisher struct {
	ctx    context.Context
	config PublisherConfig
	in     chan *Publishing
}

// NewPublisher creates a RMQPublisher that will publish messages to AMQP, redialing on errors.
func NewPublisher(ctx context.Context, rmqConn *RMQConnection, config PublisherConfig) *RMQPublisher {
	if ctx == nil {
		panic("rmq.NewPublisher called with nil ctx")
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
		ctx:    ctx,
		config: config,
		in:     make(chan *Publishing, 10),
	}
	go pub.connect(rmqConn)
	return pub
}

func (p *RMQPublisher) connect(rmqConn *RMQConnection) {
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

		// Spin off goroutines that echo this amqp.Channel's Notifications to our provided channels
		if p.config.NotifyFlow != nil {
			go func() {
				for f := range mqChan.NotifyFlow(make(chan bool, 2)) {
					p.config.NotifyFlow <- f
				}
			}()
		}
		if p.config.NotifyReturn != nil {
			go func() {
				for r := range mqChan.NotifyReturn(make(chan amqp.Return, 2)) {
					p.config.NotifyReturn <- r
				}
			}()
		}

		p.listen(mqChan)
	}
}

func (p *RMQPublisher) listen(mqChan *amqp.Channel) {
	logPrefix := "RMQPublisher.listen"
	notifyClose := mqChan.NotifyClose(make(chan *amqp.Error, 2))

	var closing bool
	for {
		select {
		case <-p.ctx.Done():
			// Close p.in so that listen will close after processing all Publishings
			if !closing {
				close(p.in)
				closing = true
			}
		case err := <-notifyClose:
			p.config.Logf(logPrefix+" amqp.Channel closed due to err %v, getting a new amqp.Channel", err)
			return
		case pub, ok := <-p.in:
			if !ok {
				return
			}
			go pub.publish(mqChan)
		}
	}
}

// Publish sends a Publishing to be published on RMQPublisher's current amqp.Channel.
// Returns an amqp.DeferredConfirmation only if the RMQPublisher is in confirm mode.
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

// PublishUntilConfirmed calls Publish and waits for it's AMQP confirm. It will republish if AMQP nacks, or takes longer than confirmTimeout.
// Use context.WithTimeout so this function doesn't retry forever. Should only be used when you know the Exchange and RoutingKey will have a queue waiting,
// otherwise this will simply retry forever on each Nack.
func (p *RMQPublisher) PublishUntilConfirmed(ctx context.Context, confirmTimeout time.Duration, retryOnPublishErr bool, pub Publishing) error {
	logPrefix := "RMQPublisher.PublishUntilConfirmed"
	if p.config.DontConfirm {
		return fmt.Errorf(logPrefix + " called on a RMQPublisher that's not in Confirm mode")
	}

	var pubDelay time.Duration
	for {
		defConf, err := p.Publish(ctx, pub)
		if err != nil {
			if retryOnPublishErr {
				pubDelay = internal.CalculateDelay(p.config.MinRetryInterval, p.config.MaxRetryInterval, pubDelay)
				p.config.Logf(logPrefix+" got a Publish error %v, republishing after %s", err, pubDelay.String())
				select {
				case <-ctx.Done():
					return fmt.Errorf(logPrefix+" context done before the publish was sent %w", context.Cause(ctx))
				case <-time.After(pubDelay):
					continue
				}
			} else {
				return err
			}
		}

		pubDelay = 0

		select {
		case <-ctx.Done():
			return fmt.Errorf(logPrefix+" context done before the publish was confirmed %w", context.Cause(ctx))
		case <-time.After(confirmTimeout):
			continue
		case <-defConf.Done():
			if defConf.Acked() {
				return nil
			}
		}
	}
}
