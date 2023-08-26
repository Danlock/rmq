package internal

import (
	"context"
	"fmt"
	"time"

	"log/slog"
)

type ChanReq[T any] struct {
	Ctx      context.Context
	RespChan chan ChanResp[T]
}
type ChanResp[T any] struct {
	Val T
	Err error
}

func CalculateDelay(min, max, current time.Duration) time.Duration {
	if current == 0 {
		return min
	} else if current < max {
		return current * 2
	} else {
		return max
	}
}

// WrapLogFunc runs fmt.Sprintf on the msg, args parameters so the end user can use slog.Log or any other logging library more interchangably
// danlock/rmq won't send the args parameter to the user provided Log func, but the end user can take advantage of slog.Level to ignore warnings, and we don't need to add any dependencies.
// This does mean calldepth loggers will need a +1 however.
func WrapLogFunc(logFunc *func(ctx context.Context, level slog.Level, msg string, args ...any)) {
	if logFunc == nil {
		panic("WrapLogFunc takes a pointer")
	} else if *logFunc == nil {
		*logFunc = func(ctx context.Context, level slog.Level, msg string, args ...any) {}
	} else {
		userLog := *logFunc
		*logFunc = func(ctx context.Context, level slog.Level, msg string, args ...any) {
			userLog(ctx, level, fmt.Sprintf(msg, args...))
		}
	}
}
