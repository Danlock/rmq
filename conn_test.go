package rmq

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type MockAMQPConnection struct {
	ChannelFn       func() (*amqp.Channel, error)
	NotifyCloseChan chan *amqp.Error
}

func (m *MockAMQPConnection) Channel() (*amqp.Channel, error) {
	if m.ChannelFn != nil {
		return m.ChannelFn()
	}
	return nil, fmt.Errorf("sike")
}
func (m *MockAMQPConnection) NotifyClose(receiver chan *amqp.Error) chan *amqp.Error {
	if m.NotifyCloseChan != nil {
		return m.NotifyCloseChan
	}
	return receiver
}
func (m *MockAMQPConnection) LocalAddr() net.Addr {
	return &net.UnixAddr{"MockAMQPConnection", "unix"}
}
func (m *MockAMQPConnection) RemoteAddr() net.Addr {
	return &net.UnixAddr{"MockAMQPConnection", "unix"}
}
func (m *MockAMQPConnection) CloseDeadline(time.Time) error {
	return nil
}
func (m *MockAMQPConnection) IsClosed() bool {
	return false
}

func TestConnect(t *testing.T) {
	errs := make(chan any, 1)
	go func() {
		defer func() {
			errs <- recover()
		}()
		_ = Connect(nil, ConnectArgs{}, nil)
	}()
	result := <-errs
	if result == nil {
		t.Fatalf("Connect should panic when missing required arguments")
	}
	rmqConn := Connect(context.Background(), ConnectArgs{}, func() (AMQPConnection, error) { return nil, fmt.Errorf("sike") })
	if rmqConn == nil {
		t.Fatalf("Connect failed to return a rmq.Connection")
	}
}

func TestConnection_CurrentConnection(t *testing.T) {
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	tests := []struct {
		name            string
		connCtx, reqCtx context.Context
		connDialFn      func() (AMQPConnection, error)
		wantErr         error
	}{
		{
			"success",
			context.Background(), context.Background(),
			func() (AMQPConnection, error) { return &MockAMQPConnection{}, nil },
			nil,
		},
		{
			"failed due to request context canceled",
			context.Background(), canceledCtx,
			func() (AMQPConnection, error) { return &MockAMQPConnection{}, nil },
			context.Canceled,
		},
		{
			"failed due to connection context canceled",
			canceledCtx, context.Background(),
			func() (AMQPConnection, error) { return &MockAMQPConnection{}, nil },
			context.Canceled,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rmqConn := Connect(tt.connCtx, ConnectArgs{}, tt.connDialFn)
			got, err := rmqConn.CurrentConnection(tt.reqCtx)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("rmq.Connection.CurrentConnection() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err == nil && got == nil {
				t.Errorf("rmq.Connection.CurrentConnection() should have returned an AMQPConnection")
			}
		})
	}
}

func TestConnection_Channel(t *testing.T) {
	closeChan := make(chan *amqp.Error, 5)
	badErr := fmt.Errorf("shucks")
	// close the connection, redialer should grab a new one in time
	closeChan <- amqp.ErrClosed
	goodMockAMQP := &MockAMQPConnection{
		ChannelFn:       func() (*amqp.Channel, error) { return nil, nil },
		NotifyCloseChan: closeChan}
	badMockAMQP := &MockAMQPConnection{
		ChannelFn: func() (*amqp.Channel, error) { return nil, badErr }}
	slowMockAMQP := &MockAMQPConnection{
		ChannelFn: func() (*amqp.Channel, error) {
			time.Sleep(time.Second / 4)
			return nil, nil
		}}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	baseCfg := Args{Log: slog.Log}
	connConf := ConnectArgs{
		Args: baseCfg,
	}
	goodRMQConn := Connect(ctx, connConf, func() (AMQPConnection, error) {
		return goodMockAMQP, nil
	})
	badRMQConn := Connect(ctx, connConf, func() (AMQPConnection, error) {
		return badMockAMQP, nil
	})
	slowRMQConn := Connect(ctx, connConf, func() (AMQPConnection, error) {
		return slowMockAMQP, nil
	})
	slowConfig := ConnectArgs{Args: Args{Log: slog.Log, AMQPTimeout: 50 * time.Millisecond}}
	slowUsingTimeoutRMQConn := Connect(ctx, slowConfig, func() (AMQPConnection, error) {
		return slowMockAMQP, nil
	})

	shortCtx, shortCancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer shortCancel()
	tests := []struct {
		name    string
		ctx     context.Context
		rmqConn *Connection
		wantErr error
	}{
		{
			"success",
			ctx,
			goodRMQConn,
			nil,
		}, {
			"failed",
			ctx,
			badRMQConn,
			badErr,
		}, {
			"ctx finished before slow channel",
			shortCtx,
			slowRMQConn,
			context.DeadlineExceeded,
		}, {
			"AMQPChannelTimeout before slow channel",
			ctx,
			slowUsingTimeoutRMQConn,
			context.DeadlineExceeded,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(tt.ctx, time.Second)
			defer cancel()
			_, err := tt.rmqConn.Channel(ctx)
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("rmq.Connection.Channel() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}

	connConf = ConnectArgs{
		Args: Args{
			Log:   connConf.Log,
			Delay: func(attempt int) time.Duration { return time.Duration(attempt) * time.Millisecond },
		},
	}

	flakyCount := 0
	dialErr := fmt.Errorf("dial fail")
	flakyRMQConn := Connect(ctx, connConf, func() (AMQPConnection, error) {
		flakyCount++
		if flakyCount == 5 {
			return badMockAMQP, nil
		} else if flakyCount > 6 {
			return goodMockAMQP, nil
		}
		return nil, dialErr
	})

	_, err := flakyRMQConn.Channel(ctx)
	if !errors.Is(err, badErr) {
		t.Fatalf("rmq.Connection.Channel() error = %v, wantErr %v", err, badErr)
	}
	_, err = flakyRMQConn.Channel(ctx)
	if err != nil {
		t.Fatalf("rmq.Connection.Channel() error = %v", err)
	}

	midChanCtx, midChanCancel := context.WithCancel(context.Background())

	midChanMockAMQP := &MockAMQPConnection{
		ChannelFn: func() (*amqp.Channel, error) {
			midChanCancel()
			time.Sleep(time.Second / 4)
			return nil, nil
		}}
	midChanRMQ := Connect(midChanCtx, connConf, func() (AMQPConnection, error) {
		return midChanMockAMQP, nil
	})
	if _, err = midChanRMQ.Channel(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("rmq.Connection.Channel() error = %v, wantErr %v", err, context.Canceled)
	}
}
