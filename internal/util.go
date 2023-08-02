package internal

import (
	"context"
	"time"
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

type testingT interface {
	Helper()
	Logf(format string, args ...any)
}

func LogTillCtx(t testingT, ctx context.Context) func(string, ...any) {
	return func(s string, a ...any) {
		if ctx.Err() == nil {
			t.Helper()
			t.Logf(time.Now().Format(time.RFC3339Nano)+": "+s, a...)
		}
	}
}
