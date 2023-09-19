package internal

import (
	"context"
	"fmt"
	"time"

	"log/slog"
)

// ChanReq and ChanResp are used to send and receive resources over a channel.
// The Ctx is sent so that the listener can use it for a timeout if necessary.
// RespChan should be buffered to at least 1 to not block the listening goroutine.
type ChanReq[T any] struct {
	Ctx      context.Context
	RespChan chan ChanResp[T]
}
type ChanResp[T any] struct {
	Val T
	Err error
}

var FibonacciDurations = [...]time.Duration{
	0, time.Second, time.Second, 2 * time.Second, 3 * time.Second, 5 * time.Second,
	8 * time.Second, 13 * time.Second, 21 * time.Second, 34 * time.Second,
}

// slog.Log's function signature. Useful for context aware logging and simpler to wrap than an interface like slog.Handler
type SlogLog = func(context.Context, slog.Level, string, ...any)

// WrapLogFunc runs fmt.Sprintf on the msg, args parameters so the end user can use slog.Log or any other logging library more interchangeably.
// The slog.Log func signature is an improvement over the usual func(string, any...).
// The end user can take advantage of context for log tracing, slog.Level to ignore warnings, and we only depend on the stdlib.
// This does mean calldepth loggers will need a +1 however.
func WrapLogFunc(logFunc *SlogLog) {
	if logFunc == nil {
		panic("WrapLogFunc called with nil")
	} else if *logFunc == nil {
		*logFunc = func(context.Context, slog.Level, string, ...any) {}
	} else {
		userLog := *logFunc
		*logFunc = func(ctx context.Context, level slog.Level, msg string, args ...any) {
			userLog(ctx, level, fmt.Sprintf(msg, args...))
		}
	}
}

// AMQP091Logger wraps the amqp091 Logger interface with a little boilerplate.
type AMQP091Logger struct {
	Ctx context.Context
	Log SlogLog
}

func (l AMQP091Logger) Printf(format string, v ...interface{}) {
	l.Log(l.Ctx, slog.LevelError, "rabbitmq/amqp091-go: "+fmt.Sprintf(format, v...))
}
