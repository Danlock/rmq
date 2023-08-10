package rmq

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
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
}

type PublisherConfig struct {
	// NotifyReturn will recieve amqp.Return's from any amqp.Channel this RMQPublisher sends on.
	NotifyReturn chan<- amqp.Return

	// DontConfirm will not set the amqp.Channel in Confirm mode, and disallow PublishUntilConfirmed.
	DontConfirm bool

	// Set Logf with your favorite logging library
	Logf func(msg string, args ...any)
	// *RetryInterval controls how frequently RMQPublisher retries on errors. Defaults from 0.125 seconds to 32 seconds.
	MinRetryInterval, MaxRetryInterval time.Duration
	// MaxConcurrentPublishes fails Publish calls until the concurrent publishes fall below this number.
	// This limits goroutines spawned by RMQPublisher, and combined with PublishUntilConfirmed could provide backpressure.
	MaxConcurrentPublishes int64
}

type PublishingResponse struct {
	*amqp.DeferredConfirmation
	Pub *Publishing
}

type publishings struct {
	pubs []Publishing
	req  internal.ChanReq[[]PublishingResponse]
}

func (p *publishings) publish(mqChan *amqp.Channel, wg *sync.WaitGroup) {
	defer wg.Done()
	resp := internal.ChanResp[[]PublishingResponse]{Val: make([]PublishingResponse, len(p.pubs))}
	for i, pub := range p.pubs {
		resp.Val[i].Pub = &p.pubs[i]
		resp.Val[i].DeferredConfirmation, resp.Err = mqChan.PublishWithDeferredConfirmWithContext(
			p.req.Ctx, pub.Exchange, pub.RoutingKey, pub.Mandatory, pub.Immediate, pub.Publishing)
		if resp.Err != nil {
			resp.Val = resp.Val[:i]
			break
		}
	}
	p.req.RespChan <- resp
}

type RMQPublisher struct {
	ctx            context.Context
	config         PublisherConfig
	in             chan publishings
	concurrentPubs atomic.Int64
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
		in:     make(chan publishings),
	}
	go pub.connect(rmqConn)
	return pub
}

// connect grabs an amqp.Channel from RMQConnection. It does so repeatedly on any error until it's context finishes.
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

		// Spin off a goroutine that echos this amqp.Channel's Return until it closes
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

// listen sends publishes on a amqp.Channel until it's closed.
func (p *RMQPublisher) listen(mqChan *amqp.Channel) {
	logPrefix := "RMQPublisher.listen"
	notifyClose := mqChan.NotifyClose(make(chan *amqp.Error, 2))
	var pubGroup sync.WaitGroup
	for {
		select {
		case <-p.ctx.Done():
			// Wait for publishes on the current channel to complete before closing it.
			pubGroup.Wait()
			if err := mqChan.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
				p.config.Logf(logPrefix+" got an error while closing channel %v", err)
				return
			}
		case err, ok := <-notifyClose:
			if !ok {
				return
			} else if err != nil {
				p.config.Logf(logPrefix+" during %d publishes got an amqp.Channel close err %v", p.concurrentPubs.Load(), err)
			}
		case pubs := <-p.in:
			// Publish in a separate goroutine to prevent a blocking amqp.Publish preventing us from receiving a close error
			pubGroup.Add(1)
			go pubs.publish(mqChan, &pubGroup)
		}
	}
}

var ErrTooManyPublishes = fmt.Errorf("Rejecting publish due to RMQPublisher.MaxConcurrentPublishes, retry later or raise RMQPublisher.MaxConcurrentPublishes")

// Publish sends Publishings on RMQPublisher's current amqp.Channel.
// Returns amqp.DefferedConfirmation's only if the RMQPublisher has Confirm set.
// Publishings are sent until error. PublishingResponses contains successful publishes only.
func (p *RMQPublisher) Publish(ctx context.Context, pubs ...Publishing) ([]PublishingResponse, error) {
	if p.config.MaxConcurrentPublishes > 0 {
		concurrentPubs := p.concurrentPubs.Add(1)
		defer p.concurrentPubs.Add(-1)
		if concurrentPubs > p.config.MaxConcurrentPublishes {
			return nil, ErrTooManyPublishes
		}
	}

	publishes := publishings{pubs: pubs}
	publishes.req.Ctx = ctx
	publishes.req.RespChan = make(chan internal.ChanResp[[]PublishingResponse], 1)
	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("RMQPublisher.Publish context done before publish sent %w", context.Cause(ctx))
	case <-p.ctx.Done():
		return nil, fmt.Errorf("RMQPublisher context done before publish sent %w", context.Cause(p.ctx))
	case p.in <- publishes:
	}

	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("RMQPublisher.Publish context done before publish completed %w", context.Cause(ctx))
	case <-p.ctx.Done():
		return nil, fmt.Errorf("RMQPublisher context done before publish completed %w", context.Cause(p.ctx))
	case r := <-publishes.req.RespChan:
		return r.Val, r.Err
	}
}

type PublishUntilConfirmedConfig struct {
	// ConfirmTimeout is how long PublishUntilConfirmed waits before republishing. Defaults to 1 minute.
	ConfirmTimeout time.Duration
	// RetryOnPublishErr controls whether PublishUntilConfirmed retries when a publish fails
	RetryOnPublishErr bool
	// RetryOnNack controls whether PublishUntilConfirmed retries on a nack. User discretion is advised.
	// If a Publishing is sent to an nonexisting exchange or routing key with no queue while this is set,
	// PublishUntilConfirmed will republish repeatedly until the queue/exchange is created.
	RetryOnNack bool
}

// PublishUntilConfirmed calls Publish and waits for the Publishings to be confirmed.
// It republishes if a message isn't confirmed after ConfirmTimeout.
// TODO: On error, returns the pubs that were not acked.
func (p *RMQPublisher) PublishUntilConfirmed(ctx context.Context, cfg PublishUntilConfirmedConfig, pubs ...Publishing) ([]Publishing, error) {
	logPrefix := "RMQPublisher.PublishUntilConfirmed"
	if p.config.DontConfirm {
		return pubs, fmt.Errorf(logPrefix + " called on a RMQPublisher that's not in Confirm mode")
	}
	if cfg.ConfirmTimeout == 0 {
		cfg.ConfirmTimeout = time.Minute
	}

	var pubDelay time.Duration
	for {
		pendConfirms, err := p.Publish(ctx, pubs...)
		if err != nil {
			if cfg.RetryOnPublishErr {
				pubDelay = internal.CalculateDelay(p.config.MinRetryInterval, p.config.MaxRetryInterval, pubDelay)
				p.config.Logf(logPrefix+" got a Publish error. Republishing after %s due to %v", pubDelay.String(), err)
				select {
				case <-ctx.Done():
					return pubs, fmt.Errorf(logPrefix+" context done before the publish was sent %w", context.Cause(ctx))
				case <-time.After(pubDelay):
					continue
				}
			} else {
				return err
			}
		}
		// Succesfully published, so reset the delay
		pubDelay = 0

		pubs, err = p.handleConfirms(ctx, cfg, pubs, pendConfirms)
		if err != nil {
			return pubs, err
		}
		if len(pubs) == 0 {
			return nil, nil
		}
	}
}

// handleConfirms loops over the []PublishingResponse of a Publish call, checking if they have been acked every 5ms.
func (p *RMQPublisher) handleConfirms(ctx context.Context, cfg PublishUntilConfirmedConfig, pubs []Publishing, pendConfirms []PublishingResponse) (unackedPubs []Publishing, err error) {
	unackedPubs = pubs
	defer func() {
		// Delete acked Publishing's out of pubs and return that slice instead
		i := 0
		for _, pub := range unackedPubs {
			// This technically disallows a client from sending an empty Publishing with PublishUntilConfirmed.
			// If that is a valid use case for you, use Publish instead.
			// Unsure why RabbitMQ even accepts a message without an Exchange and RoutingKey.
			if pub.Exchange != "" || pub.RoutingKey != "" {
				unackedPubs[i] = pub
				i++
			}
		}
		unackedPubs = unackedPubs[:i]
	}()

	confirmTimeoutChan := time.After(cfg.ConfirmTimeout)
	confirms := 0
	for {
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("RMQPublisher.PublishUntilConfirmed context done before the publish was confirmed %w", context.Cause(ctx))
		case <-confirmTimeoutChan:
			return pubs, nil
		case <-time.After(5 * time.Millisecond):
		}

		for i, defConf := range pendConfirms {
			if defConf.Pub == nil {
				continue
			}

			select {
			case <-defConf.Done():
				if defConf.Acked() || !cfg.RetryOnNack {
					*defConf.Pub = Publishing{}
				}
				// Delete the current confirm so we don't check it again
				confirms++
				pendConfirms[i] = PublishingResponse{}
			default:
			}
		}
		// Exit once all the PublishingResponse's have confirmed
		if confirms == len(pubs) {
			return pubs, nil
		}
	}
}
