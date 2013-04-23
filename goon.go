/*
 * Copyright (c) 2012 Matt Jibson <matt.jibson@gmail.com>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

package goon

import (
	"appengine"
	"appengine/datastore"
	"appengine/memcache"
	"errors"
	"fmt"
	"net/http"
	"reflect"
)

var (
	// LogErrors issues appengine.Context.Errorf on any error.
	LogErrors bool = true
)

// Goon holds the app engine context and request memory cache.
type Goon struct {
	context       appengine.Context
	cache         map[string]interface{}
	inTransaction bool
	toSet         map[string]interface{}
	toDelete      []string
}

func memkey(k *datastore.Key) string {
	return k.String()
}

func NewGoon(r *http.Request) *Goon {
	return FromContext(appengine.NewContext(r))
}

func FromContext(c appengine.Context) *Goon {
	return &Goon{
		context: c,
		cache:   make(map[string]interface{}),
	}
}

func (g *Goon) error(err error) {
	if LogErrors {
		g.context.Errorf(err.Error())
	}
}

func (g *Goon) Key(src interface{}) (*datastore.Key, error) {
	return g.getStructKey(src)
}

// RunInTransaction runs f in a transaction. It calls f with a transaction
// context tg that f should use for all App Engine operations. Neither cache nor
// memcache are used or set during a transaction.
//
// Otherwise similar to appengine/datastore.RunInTransaction:
// https://developers.google.com/appengine/docs/go/datastore/reference#RunInTransaction
func (g *Goon) RunInTransaction(f func(tg *Goon) error, opts *datastore.TransactionOptions) error {
	var ng *Goon
	err := datastore.RunInTransaction(g.context, func(tc appengine.Context) error {
		ng = &Goon{
			context:       tc,
			inTransaction: true,
			toSet:         make(map[string]interface{}),
		}
		return f(ng)
	}, opts)

	if err == nil {
		for k, v := range ng.toSet {
			g.cache[k] = v
		}

		for _, k := range ng.toDelete {
			delete(g.cache, k)
		}
	} else {
		g.error(err)
	}

	return err
}

// Put stores src.
// If src has an incomplete key, it is updated.
func (g *Goon) Put(src interface{}) error {
	return g.PutMulti([]interface{}{src})
}

// PutMany is a wrapper around PutMulti.
func (g *Goon) PutMany(srcs ...interface{}) error {
	return g.PutMulti(srcs)
}

const putMultiLimit = 500

// PutMulti stores a sequence of entities.
// Any entity with an incomplete key will be updated.
func (g *Goon) PutMulti(srcs []interface{}) error {
	var memkeys []string
	keys := make([]*datastore.Key, len(srcs))
	for i, src := range srcs {
		key, err := g.getStructKey(src)
		if err != nil {
			return err
		}
		if !key.Incomplete() {
			memkeys = append(memkeys, memkey(key))
		}
		keys[i] = key
	}

	// Memcache needs to be updated after the datastore to prevent a common race condition
	defer memcache.DeleteMulti(g.context, memkeys)

	for i := 0; i <= len(srcs)/putMultiLimit; i++ {
		lo := i * putMultiLimit
		hi := (i + 1) * putMultiLimit
		if hi > len(srcs) {
			hi = len(srcs)
		}
		rkeys, err := datastore.PutMulti(g.context, keys[lo:hi], srcs[lo:hi])
		if err != nil {
			g.error(err)
			return err
		}

		for i, src := range srcs[lo:hi] {
			if keys[lo+i].Incomplete() {
				setStructKey(src, rkeys[i])
			}
			if g.inTransaction {
				g.toSet[memkey(rkeys[i])] = src
			}
		}
	}

	if !g.inTransaction {
		g.putMemoryMulti(srcs)
	}

	return nil
}

func (g *Goon) putMemoryMulti(srcs []interface{}) {
	for _, src := range srcs {
		g.putMemory(src)
	}
}

func (g *Goon) putMemory(src interface{}) {
	key, _ := g.getStructKey(src)
	g.cache[memkey(key)] = src
}

func (g *Goon) putMemcache(srcs []interface{}) error {
	items := make([]*memcache.Item, len(srcs))

	for i, src := range srcs {
		gob, err := toGob(src)
		if err != nil {
			g.error(err)
			return err
		}
		key, err := g.getStructKey(src)

		items[i] = &memcache.Item{
			Key:   memkey(key),
			Value: gob,
		}
	}

	err := memcache.SetMulti(g.context, items)

	if err != nil {
		g.error(err)
		return err
	}

	g.putMemoryMulti(srcs)
	return nil
}

// Get fetches an entity of kind src by key.
// If there is no such key, datastore.ErrNoSuchEntity is returned.
func (g *Goon) Get(dst interface{}) error {
	dsts := []interface{}{dst}
	if err := g.GetMulti(dsts); err != nil {
		// Look for an embedded error if it's multi
		if me, ok := err.(appengine.MultiError); ok {
			for i, merr := range me {
				if i == 0 {
					return merr
				}
			}
		}
		// Not multi, normal error
		return err
	}
	return nil
}

// Get fetches a sequency of Entities, whose keys must already be valid.
// Entities with no correspending key have their NotFound field set to true.
func (g *Goon) GetMulti(dst interface{}) error {
	v := reflect.Indirect(reflect.ValueOf(dst))
	if v.Kind() != reflect.Slice {
		return errors.New("goon: dst must be a slice or pointer-to-slice")
	}
	l := v.Len()

	var dskeys []*datastore.Key
	var dsdst []interface{}
	var gmdst interface{}
	var dixs []int

	if !g.inTransaction {
		var memkeys []string
		var mixs []int

		for i := 0; i < l; i++ {
			vi := v.Index(i)
			key, err := g.getStructKey(vi.Interface())
			if err != nil {
				g.error(err)
				return err
			}
			m := memkey(key)
			if s, present := g.cache[m]; present {
				vi.Set(reflect.ValueOf(s))
			} else {
				memkeys = append(memkeys, m)
				mixs = append(mixs, i)
			}
		}

		memvalues, err := memcache.GetMulti(g.context, memkeys)
		if err != nil {
			g.error(errors.New(fmt.Sprintf("goon: ignored memcache error: %v", err.Error())))
			// ignore memcache errors
			//return err
		}

		for i, m := range memkeys {
			d := v.Index(mixs[i]).Interface()
			if s, present := memvalues[m]; present {
				err := fromGob(d, s.Value)
				if err != nil {
					g.error(err)
					return err
				}

				g.putMemory(d)
			} else {
				key, err := g.getStructKey(d)
				if err != nil {
					g.error(err)
					return err
				}
				dskeys = append(dskeys, key)
				dsdst = append(dsdst, d)
				dixs = append(dixs, mixs[i])
			}
		}
		gmdst = dsdst
	} else {
		dskeys = make([]*datastore.Key, l)
		dixs = make([]int, l)
		gmdst = dst

		for i := 0; i < l; i++ {
			vi := v.Index(i)
			key, err := g.getStructKey(vi.Interface())
			if err != nil {
				g.error(err)
				return err
			}
			dskeys[i] = key
			dixs[i] = i
		}
	}

	gmerr := datastore.GetMulti(g.context, dskeys, gmdst)
	var ret error
	var multiErr appengine.MultiError
	if gmerr != nil {
		merr, ok := gmerr.(appengine.MultiError)
		if !ok {
			g.error(gmerr)
			return gmerr
		}
		multiErr = make(appengine.MultiError, l)
		for i, idx := range dixs {
			multiErr[idx] = merr[i]
		}
		ret = multiErr
	}

	if len(dskeys) > 0 && !g.inTransaction {
		if err := g.putMemcache(dsdst); err != nil {
			g.error(err)
			return err
		}
	}

	return ret
}

// Delete deletes the entity for the given key.
func (g *Goon) Delete(key *datastore.Key) error {
	keys := []*datastore.Key{key}
	return g.DeleteMulti(keys)
}

// DeleteMulti is a batch version of Delete.
func (g *Goon) DeleteMulti(keys []*datastore.Key) error {
	memkeys := make([]string, len(keys))
	for i, k := range keys {
		mk := memkey(k)
		memkeys[i] = mk

		if g.inTransaction {
			g.toDelete = append(g.toDelete, mk)
		} else {
			delete(g.cache, mk)
		}
	}

	// Memcache needs to be updated after the datastore to prevent a common race condition
	defer memcache.DeleteMulti(g.context, memkeys)

	return datastore.DeleteMulti(g.context, keys)
}

// NotFound returns true if err is an appengine.MultiError and err[idx] is an datastore.ErrNoSuchEntity.
func NotFound(err error, idx int) bool {
	if merr, ok := err.(appengine.MultiError); ok {
		return len(merr) <= idx && merr[idx] == datastore.ErrNoSuchEntity
	}
	return false
}
