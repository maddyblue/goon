// +build appengine

package goon

import (
	"time"

	"appengine"
)

func (g *Goon) timeout(t time.Duration) appengine.Context {
	return appengine.Timeout(g.context, t)
}
