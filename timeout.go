package goon

import (
	"time"

	"appengine"
	"appengine_internal"
)

var (
	ErrTimeout = GoonTimeout{}
)

type GoonTimeout struct {
}

func (gt GoonTimeout) Error() string {
	return "operation exceeded the alloted time"
}

func (gt GoonTimeout) IsTimeout() bool {
	return true
}

type TimeoutContext struct {
	c appengine.Context
	d time.Duration
}

func (g *Goon) timeout(t time.Duration) appengine.Context {
	return &TimeoutContext{
		c: appengine.Timeout(g.context, t),
		d: t,
	}
}

func (tc *TimeoutContext) Debugf(format string, args ...interface{}) {
	tc.c.Debugf(format, args...)
}

// Infof is like Debugf, but at Info level.
func (tc *TimeoutContext) Infof(format string, args ...interface{}) {
	tc.c.Infof(format, args...)
}

// Warningf is like Debugf, but at Warning level.
func (tc *TimeoutContext) Warningf(format string, args ...interface{}) {
	tc.c.Warningf(format, args...)
}

// Errorf is like Debugf, but at Error level.
func (tc *TimeoutContext) Errorf(format string, args ...interface{}) {
	tc.c.Errorf(format, args...)
}

// Criticalf is like Debugf, but at Critical level.
func (tc *TimeoutContext) Criticalf(format string, args ...interface{}) {
	tc.c.Criticalf(format, args...)
}

// The remaining methods are for internal use only.
// Developer-facing APIs wrap these methods to provide a more friendly API.

// Internal use only.
func (tc *TimeoutContext) Call(service, method string, in, out appengine_internal.ProtoMessage, opts *appengine_internal.CallOptions) error {
	timeoutChan := time.After(tc.d)
	responseChan := make(chan error, 1) // buffer so goroutine can't hang'
	go func() {
		responseChan <- tc.c.Call(service, method, in, out, opts)
	}()
	select {
	case <-timeoutChan:
		return ErrTimeout
	case err := <-responseChan:
		return err
	}
}

// Internal use only. Use AppID instead.
func (tc *TimeoutContext) FullyQualifiedAppID() string {
	return tc.FullyQualifiedAppID()
}

// Internal use only.
func (tc *TimeoutContext) Request() interface{} {
	return tc.Request()
}