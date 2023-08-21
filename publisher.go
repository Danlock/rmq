package rmq

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
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

const pkgHeader = "github.com/danlock/rmq.RMQPublisher.PublishUntilAcked"

// setID sets a field within the Publishing.Header within PublishUntilAcked for identifying and ignoring returned Publishing's.
func (p *Publishing) setID(prefix string, index int) {
	if p.Headers == nil {
		p.Headers = make(amqp.Table, 1)
	}
	p.Headers[pkgHeader] = prefix + strconv.Itoa(index)
}

type PublisherConfig struct {
	// NotifyReturn will recieve []amqp.Return's from any amqp.Channel this RMQPublisher sends on.
	NotifyReturn chan<- []amqp.Return

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
	ctx                 context.Context
	config              PublisherConfig
	concurrentPubs      atomic.Int64
	in                  chan publishings
	listeningForReturns chan bool
	requestReturns      chan internal.ChanReq[[]amqp.Return]
}

// NewPublisher creates a RMQPublisher that will publish messages to AMQP on a single amqp.Channel at a time.
// On any error such as Channel or Connection closes, it will get a new Channel, which redials AMQP if necessary.
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
		ctx:                 ctx,
		config:              config,
		in:                  make(chan publishings),
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

		// Spin off a goroutine that echos this amqp.Channel's Returns until it closes
		go func() {
			for r := range mqChan.NotifyReturn(make(chan amqp.Return, 2)) {
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
// If the AMQP Channel or Connection closes mid Publish, not all messages will be sent.
func (p *RMQPublisher) Publish(ctx context.Context, pubs ...Publishing) ([]PublishingResponse, error) {
	if len(pubs) == 0 {
		return nil, nil
	}

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

// PublishUntilConfirmed calls Publish and waits for the Publishings to be confirmed.
// It republishes if a message isn't confirmed after ConfirmTimeout.
// Returns []PublishingResponse which includes the amqp.DefferredConfirmation for the caller to check Acked().
// Recommended to call with context.WithTimeout.
func (p *RMQPublisher) PublishUntilConfirmed(ctx context.Context, confirmTimeout time.Duration, pubs ...Publishing) ([]PublishingResponse, error) {
	logPrefix := "RMQPublisher.PublishUntilConfirmed"
	if len(pubs) == 0 {
		return nil, nil
	}
	if p.config.DontConfirm {
		return nil, fmt.Errorf(logPrefix + " called on a RMQPublisher that's not in Confirm mode")
	}

	allConfirmed := make([]PublishingResponse, 0, len(pubs))
	if confirmTimeout <= 0 {
		confirmTimeout = 5 * time.Minute
	}

	var pubDelay time.Duration
	unconfirmedPubs := pubs
	for {
		pubStart := time.Now()
		pendConfirms, err := p.Publish(ctx, unconfirmedPubs...)
		if err != nil {
			pubDelay = internal.CalculateDelay(p.config.MinRetryInterval, p.config.MaxRetryInterval, pubDelay)
			p.config.Logf(logPrefix+" got a Publish error. Republishing after %s due to %v", pubDelay.String(), err)
			select {
			case <-ctx.Done():
				return allConfirmed, fmt.Errorf(logPrefix+" context done before the publish was sent %w", context.Cause(ctx))
			case <-time.After(pubDelay):
				continue
			}
		}
		// Succesfully published, so reset the delay
		pubDelay = 0

		confirmed, unconfirmed, err := p.handleConfirms(ctx, confirmTimeout, pendConfirms)
		allConfirmed = append(allConfirmed, confirmed...)
		// finish once everything is confirmed, or if we got an unexpected error
		if (err != nil && !errors.Is(err, ErrConfirmTimeout)) || len(unconfirmed) == 0 {
			return allConfirmed, err
		}

		p.config.Logf(logPrefix+" timed out waiting on confirms after %s. Republishing %d unconfirmed Publishing's...", time.Since(pubStart), len(unconfirmed))

		// Make a new pubs slice and copy the Publishing's over so we don't mess with any confirmed PublishingResponse.Pub pointers
		if &unconfirmedPubs[0] == &pubs[0] {
			unconfirmedPubs = make([]Publishing, len(unconfirmed))
			for i, r := range unconfirmed {
				unconfirmedPubs[i] = *r.Pub
			}
		} else {
			for i := 0; i < len(unconfirmedPubs); i++ {
				for _, resp := range confirmed {
					if &unconfirmedPubs[i] == resp.Pub {
						unconfirmedPubs = slices.Delete(unconfirmedPubs, i, i+1)
					}
				}
			}
		}
	}
}

var ErrConfirmTimeout = errors.New("RMQPublisher.PublishUntilConfirmed timed out waiting for confirms, republishing")

// handleConfirms loops over the []PublishingResponse of a Publish call, checking if they have been acked every 5ms.
func (p *RMQPublisher) handleConfirms(ctx context.Context, confirmTimeout time.Duration, unconfirmed []PublishingResponse) (confirmed, _ []PublishingResponse, err error) {
	confirmed = make([]PublishingResponse, 0, len(unconfirmed))
	confirmTimeoutChan := time.After(confirmTimeout)

	for {
		select {
		case <-ctx.Done():
			return confirmed, unconfirmed, fmt.Errorf("RMQPublisher.PublishUntilConfirmed context done before the publish was confirmed %w", context.Cause(ctx))
		case <-confirmTimeoutChan:
			return confirmed, unconfirmed, ErrConfirmTimeout
		case <-time.After(5 * time.Millisecond):
		}

		unconfirmed = slices.DeleteFunc(unconfirmed, func(pr PublishingResponse) bool {
			select {
			case <-pr.Done():
				confirmed = append(confirmed, pr)
				return true
			default:
				return false
			}
		})

		// Exit once all the PublishingResponse's have confirmed
		if len(unconfirmed) == 0 {
			return confirmed, unconfirmed, nil
		}
	}
}

// storeReturns stores and provides returned indexes for PublishUntilAcked, and echos them on NotifyReturn if set.
func (p *RMQPublisher) storeReturns(returnChan <-chan amqp.Return) {
	returns := make([]amqp.Return, 0)
	returnsToEcho := make([]amqp.Return, 0)

	listeners := 0
	for {
		select {
		case <-p.ctx.Done():
			// storeReturns shuts down once it has no listeners and the RMQPublisher context has cancelled.
			if listeners == 0 {
				return
			}
		case ret := <-returnChan:
			if listeners > 0 {
				returns = append(returns, ret)
			}
			if p.config.NotifyReturn != nil {
				// Try not to repeat streadway/amqp's mistake of deadlocking if a client isn't listening to their Notify* channel.
				// Return our stored returns to the caller if they're listening, if not try again next return.
				// returnsToEcho could grow forever if the client never listens to the NotifyReturn chan they provided us, but that is acceptable for misbehaving callers.
				returnsToEcho = append(returnsToEcho, ret)
				select {
				case p.config.NotifyReturn <- returnsToEcho:
					returnsToEcho = make([]amqp.Return, 0)
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
						return
					}

					if len(returns) > 0 {
						returns = returns[0:0]
					}
				}
			}

		}
	}
}

type PublishUntilAckedConfig struct {
	// ConfirmTimeout defaults to 5 minutes
	ConfirmTimeout time.Duration
	// CorrelateReturn is used to correlate a Return and a Publishing so PublishUntilAcked won't republish them..
	// Must return an index within []Publishing that the amqp.Return correlates to or a -1 if it doesn't correlate to any.
	// Also must not mutate []Publishing.
	CorrelateReturn func(amqp.Return, []Publishing) int
}

// correlateReturn tries to grab a PublishUntilAcked header id out of an AMQP return, if it even exists.
func (p *RMQPublisher) correlateReturn(prefix string) func(amqp.Return, []Publishing) int {
	return func(ret amqp.Return, pubs []Publishing) int {
		if ret.Headers == nil {
			return -1
		}
		retID, ok := ret.Headers[pkgHeader].(string)
		if !ok {
			return -1
		}
		retIndex, err := strconv.Atoi(strings.TrimPrefix(retID, prefix))
		if err != nil {
			return -1
		}
		return retIndex
	}
}

// PublishUntilAcked is like PublishUntilConfirmed, but it republishes nacked confirms. User discretion is advised.
//
// Nacks can happen for a variety of reasons, ranging from user error (mistyped exchange) or RabbitMQ internal errors.
//
// PublishUntilAcked will republish a Mandatory Publishing with a nonexistent exchange forever (until the exchange exists), as one example.
// PublishUntilAcked is intended for ensuring a Publishing with a known destination queue will get acked despite flaky connections or temporary RabbitMQ node failures.
// Recommended to call with context.WithTimeout.
//
// If CorrelateReturn isn't set, an amqp.Publishing.Header field will be used to correlate amqp.Return's by default.
// On error, it will return []PublishingResponse containing Publishing's acked before the error occurred.
func (p *RMQPublisher) PublishUntilAcked(ctx context.Context, cfg PublishUntilAckedConfig, pubs ...Publishing) ([]PublishingResponse, error) {
	if len(pubs) == 0 {
		return nil, nil
	}

	select {
	case <-p.ctx.Done():
		return nil, ctx.Err()
	case p.listeningForReturns <- true:
	}

	defer func() { p.listeningForReturns <- false }()

	if cfg.CorrelateReturn == nil {
		// unique "enough" prefix for callers without a CorrelateReturn
		idPrefix := fmt.Sprint(time.Now().Format(time.RFC3339Nano), "|", &pubs, "|")
		cfg.CorrelateReturn = p.correlateReturn(idPrefix)
		for i, p := range pubs {
			p.setID(idPrefix, i)
		}
	}

	pubsPointerMap := make(map[*Publishing]int, len(pubs))
	for i := range pubs {
		pubsPointerMap[&pubs[i]] = i
	}
	ackedResps := make([]PublishingResponse, 0, len(pubs))
	nackedPubs := pubs
	for {
		pubResps, err := p.PublishUntilConfirmed(ctx, cfg.ConfirmTimeout, nackedPubs...)

		nackedResps := slices.DeleteFunc(pubResps, func(pr PublishingResponse) bool {
			acked := pr.Acked()
			if acked {
				ackedResps = append(ackedResps, pr)
			}
			return acked
		})

		if err != nil || len(nackedResps) == 0 {
			return ackedResps, err
		}

		// Get returns from the storeReturns goroutine and run cfg.CorrelateReturns on them, then delete them out of nackedResps.
		req := internal.ChanReq[[]amqp.Return]{Ctx: ctx, RespChan: make(chan internal.ChanResp[[]amqp.Return], 1)}
		p.requestReturns <- req
		resp := <-req.RespChan

		var returnedIndexes map[int]struct{}
		for _, ret := range resp.Val {
			retIndex := cfg.CorrelateReturn(ret, pubs)
			if retIndex >= 0 && retIndex < len(pubs) {
				returnedIndexes[retIndex] = struct{}{}
			}
		}

		nackedResps = slices.DeleteFunc(nackedResps, func(pr PublishingResponse) bool {
			_, ok := returnedIndexes[pubsPointerMap[pr.Pub]]
			return ok
		})

		if len(nackedResps) == 0 {
			return ackedResps, err
		}

		p.config.Logf("RMQPublisher.PublishUntilAcked resending %d nacked Publishing's...", len(nackedResps))

		// Resend remaining nacks in a new slice.
		// TODO: this could be done more efficiently. Mutating pubs would require changing the default p.correlateReturns as well.
		nackedPubs = make([]Publishing, len(nackedResps))
		for i, resp := range nackedResps {
			nackedPubs[i] = *resp.Pub
			pubsPointerMap[&nackedPubs[i]] = pubsPointerMap[resp.Pub]
		}
	}
}
