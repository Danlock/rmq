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

func CalculateDelay(min, max, current time.Duration) time.Duration {
	if current <= 0 {
		return min
	} else if current < max {
		return current * 2
	} else {
		return max
	}
}

// WrapLogFunc runs fmt.Sprintf on the msg, args parameters so the end user can use slog.Log or any other logging library more interchangeably.
// The slog.Log func signature is convenient for providing an easily wrappable log func, and is better than the usual func(string, any...).
// The end user can take advantage of context for log tracing, slog.Level to ignore warnings, and we only depend on the stdlib.
// This does mean calldepth loggers will need a +1 however.
func WrapLogFunc(logFunc *func(ctx context.Context, level slog.Level, msg string, args ...any)) {
	if logFunc == nil {
		panic("WrapLogFunc called with nil")
	} else if *logFunc == nil {
		*logFunc = func(ctx context.Context, level slog.Level, msg string, args ...any) {}
	} else {
		userLog := *logFunc
		*logFunc = func(ctx context.Context, level slog.Level, msg string, args ...any) {
			userLog(ctx, level, fmt.Sprintf(msg, args...))
		}
	}
}
