package rmq

import (
	"context"
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
	// ConfirmChan will receive the amqp.DeferredConfirmation for this message once it is published, enabling the sender to publish confirms.
	// Only used when PublisherConfig.Confirm is true.
	ConfirmChan chan *amqp.DeferredConfirmation
}

type PublisherConfig struct {
	// amqp.Channel has several Notify* functions that listen for various errors/messages.
	// Set these channels to get notifications from the current amqp.Channel RMQPublisher is using.
	NotifyReturn chan<- amqp.Return
	NotifyFlow   chan<- bool

	// Confirm will set the amqp.Channel in confirm mode
	Confirm bool

	// Set Logf with your favorite logging library
	Logf func(msg string, args ...any)
	// *RetryInterval controls how frequently RMQPublisher retries on errors. Defaults from 0.125 seconds to 32 seconds.
	MinRetryInterval, MaxRetryInterval time.Duration
	// PublishInterval controls how frequently RMQPublisher publishes. Defaults to 0.1 seconds.
	PublishInterval time.Duration
}

type amqpPublishResult struct {
	err         error
	confirmChan chan *amqp.DeferredConfirmation
}

type RMQPublisher struct {
	config  PublisherConfig
	in      chan Publishing
	pubErrs chan error
}

// NewPublisher creates a RMQPublisher that will publish messages to AMQP, redialing on errors.
func NewPublisher(ctx context.Context, rmqConn RMQConnection, config PublisherConfig) *RMQPublisher {
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
	if config.PublishInterval == 0 {
		config.PublishInterval = time.Second / 10
	}

	pub := &RMQPublisher{
		config:  config,
		in:      make(chan Publishing, 10),
		pubErrs: make(chan error, 2),
	}
	go pub.listen(ctx, rmqConn)
	return pub
}

func (p *RMQPublisher) listen(ctx context.Context, rmqConn RMQConnection) {
	logPrefix := "RMQPublisher.listen"

	var delay time.Duration
	pubBuffer := make([]Publishing, 0, 100)
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}

		mqChan, err := rmqConn.Channel(ctx)
		if err != nil {
			delay = internal.CalculateDelay(p.config.MinRetryInterval, p.config.MaxRetryInterval, delay)
			p.config.Logf(logPrefix+" failed to get amqp.Channel. Retrying in %s due to err %+v", delay.String(), err)
			continue
		}

		if p.config.Confirm {
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

		p.process(ctx, mqChan, &pubBuffer)
	}
}

func (p *RMQPublisher) process(ctx context.Context, mqChan *amqp.Channel, pubBuffer *[]Publishing) {
	logPrefix := "RMQPublisher.process"
	publishTimer := time.NewTicker(p.config.PublishInterval)
	defer publishTimer.Stop()
	notifyClose := mqChan.NotifyClose(make(chan *amqp.Error, 2))
	for {
		select {
		case <-ctx.Done():
			// Context is closing, send all the messages in our buffer
			for _, pub := range *pubBuffer {
				pub := pub
				go p.publish(ctx, mqChan, pub)
			}
			return
		case err, ok := <-notifyClose:
			if ok {
				p.config.Logf(logPrefix+" amqp.Channel closed. Retrying due to err %v", err)
			}
		case pub := <-p.in:
			*pubBuffer = append(*pubBuffer, pub)
		case <-publishTimer.C:
			for _, pub := range *pubBuffer {
				pub := pub
				go p.publish(ctx, mqChan, pub)
			}
		case err := <-p.pubErrs:
			if err != nil {
				p.config.Logf(logPrefix+" failed to publish message due to err %v", err)
			}
		}
	}
}

func (p *RMQPublisher) publish(ctx context.Context, mqChan *amqp.Channel, pub Publishing) {
	defConf, err := mqChan.PublishWithDeferredConfirmWithContext(ctx, pub.Exchange, pub.RoutingKey, pub.Mandatory, pub.Immediate, pub.Publishing)
	if pub.ConfirmChan != nil {
		pub.ConfirmChan <- defConf
	}
	p.pubErrs <- err
}
