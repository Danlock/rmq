package rmq

import (
	"context"
	"errors"
	"fmt"
	"maps"
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

	pubsToConfirm := len(pubs)
	var pubDelay time.Duration
	for {
		pendConfirms, err := p.Publish(ctx, pubs...)
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

		confirmed, unconfirmed, err := p.handleConfirms(ctx, confirmTimeout, pubs, pendConfirms)
		allConfirmed = append(allConfirmed, confirmed...)
		// finish once everything is confirmed, or if we got an unexpected error
		if (err != nil && !errors.Is(err, ErrConfirmTimeout)) || len(allConfirmed) == pubsToConfirm {
			return allConfirmed, err
		}

		p.config.Logf(logPrefix+" timed out waiting on %d confirms. Republishing...", len(unconfirmed))

		// Make a new pubs slice and copy the Publishing's over so we don't mess with any confirmed PublishingResponse.Pub pointers
		pubs = make([]Publishing, len(unconfirmed))
		for i, r := range unconfirmed {
			pubs[i] = *r.Pub
		}
	}
}

var ErrConfirmTimeout = errors.New("RMQPublisher.PublishUntilConfirmed timed out waiting for confirms, republishing")

// handleConfirms loops over the []PublishingResponse of a Publish call, checking if they have been acked every 5ms.
func (p *RMQPublisher) handleConfirms(ctx context.Context, confirmTimeout time.Duration, pubs []Publishing, unconfirmed []PublishingResponse) (confirmed, _ []PublishingResponse, err error) {
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

type PublishUntilAckedConfig struct {

	// ConfirmTimeout defaults to 5 minutes
	ConfirmTimeout time.Duration
	// CorrelateReturn is used to correlate a Return and a Publishing so PublishUntilAcked can ignore their nacks.
	// Must return an index within []Publishing that the amqp.Return correlates to or a -1.
	CorrelateReturn func(amqp.Return, []Publishing) int
	ReturnChan      <-chan amqp.Return
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

// PublishUntilAcked is like PublishUntilConfirmed, but it will republish nacked confirms. User discretion is advised.
// Nacks can happen for a variety of reasons, ranging from user error (mistyped exchange) or RabbitMQ internal errors.
// PublishUntilAcked will republish a Mandatory Publishing with an nonexisting exchange forever (or until the exchange exists), as one example.
// PublishUntilAcked is intended for ensuring a Publishing with a known destination queue will get acked despite flaky connections or RabbitMQ node failures.
// Recommended to call with context.WithTimeout.
// If ReturnChan is set, CorrelateReturn is used to ignore nacks from returned Publishing's.
// If CorrelateReturn isn't set, an amqp.Publishing.Header field will be used by default.
// On error, it will return []PublishingResponse containing Publishing's acked before the error occurred.
func (p *RMQPublisher) PublishUntilAcked(ctx context.Context, cfg PublishUntilAckedConfig, pubs ...Publishing) ([]PublishingResponse, error) {
	ignoringNackedReturns := cfg.ReturnChan != nil
	// If we have a return chan, set a header field within each publishing so we can correlate a return with it's Publishing,
	// unless the caller provided their own method of identifying Returns.
	if ignoringNackedReturns && cfg.CorrelateReturn == nil {
		idPrefix := time.Now().Format(time.RFC3339Nano) + "|"
		cfg.CorrelateReturn = p.correlateReturn(idPrefix)
		for i, p := range pubs {
			p.setID(idPrefix, i)
		}
	}

	var returnIndexMu sync.RWMutex
	returnedIndexes := make(map[int]struct{})
	if ignoringNackedReturns {
		returnCtx, returnCtxCancel := context.WithCancel(ctx)
		defer returnCtxCancel()
		go func() {
			for {
				select {
				case <-returnCtx.Done():
					return
				case ret := <-cfg.ReturnChan:
					retIndex := cfg.CorrelateReturn(ret, pubs)
					if retIndex >= 0 && retIndex < len(pubs) {
						returnIndexMu.Lock()
						returnedIndexes[retIndex] = struct{}{}
						returnIndexMu.Unlock()
					}
				}
			}
		}()
	}

	pubsPointerMap := make(map[*Publishing]int, len(pubs))
	for i := range pubs {
		pubsPointerMap[&pubs[i]] = i
	}
	nackedPubs := pubs
	ackedResps := make([]PublishingResponse, 0, len(pubs))
	for {
		pubResps, err := p.PublishUntilConfirmed(ctx, cfg.ConfirmTimeout, nackedPubs...)

		nackedPubResps := make([]PublishingResponse, 0)
		pubResps = slices.DeleteFunc(pubResps, func(pr PublishingResponse) bool {
			acked := pr.Acked()
			if !acked {
				nackedPubResps = append(nackedPubResps, pr)
			}
			return !acked
		})
		ackedResps = append(ackedResps, pubResps...)

		if err != nil || len(nackedPubs) == 0 {
			return ackedResps, err
		}
		// If possible, disregard nacks that are from returned messages since they will just get sent over and over
		if ignoringNackedReturns {
			returnIndexMu.RLock()
			for retIndex := range returnedIndexes {
				nackedPubResps = slices.DeleteFunc(nackedPubResps, func(pr PublishingResponse) bool {
					return retIndex == pubsPointerMap[pr.Pub]
				})
			}
			returnIndexMu.RUnlock()
		}

		if len(nackedPubResps) == 0 {
			return ackedResps, err
		}

		// prevent pubPointerMap from growing forever by removing dead pointers from last iteration's nackedPubs
		if len(pubsPointerMap) > len(pubs) {
			maps.DeleteFunc(pubsPointerMap, func(p *Publishing, i int) bool {
				return p != &pubs[i]
			})
		}

		// Prepare to resend remaining nacks
		nackedPubs = make([]Publishing, len(nackedPubResps))
		for i, resp := range nackedPubResps {
			nackedPubs[i] = *resp.Pub
			pubsPointerMap[&nackedPubs[i]] = pubsPointerMap[resp.Pub]
		}
	}
}
