// +build !appengine

package goon

import (
	"time"

	"appengine"
	"appengine_internal"
)

type TimeoutContext struct {
	appengine.Context
	d time.Duration
}

func (g *Goon) timeout(t time.Duration) appengine.Context {
	return &TimeoutContext{
		Context: appengine.Timeout(g.context, t),
		d:       t,
	}
}

func (tc *TimeoutContext) Call(service, method string, in, out appengine_internal.ProtoMessage, opts *appengine_internal.CallOptions) error {
	timeoutChan := time.After(tc.d)
	responseChan := make(chan error, 1) // buffer so goroutine can't hang'
	go func() {
		responseChan <- tc.Context.Call(service, method, in, out, opts)
	}()
	select {
	case <-timeoutChan:
		return TimeoutError{service, method}
	case err := <-responseChan:
		return err
	}
}
