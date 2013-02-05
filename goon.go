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
	"bytes"
	"encoding/gob"
	"errors"
	"net/http"
	"reflect"
)

// Goon holds the app engine context and request memory cache.
type Goon struct {
	context       appengine.Context
	cache         map[string]*Entity
	inTransaction bool
	toSet         map[string]*Entity
	toDelete      []string
}

func memkey(k *datastore.Key) string {
	return k.String()
}

func NewGoon(r *http.Request) *Goon {
	return ContextGoon(appengine.NewContext(r))
}

func ContextGoon(c appengine.Context) *Goon {
	return &Goon{
		context: c,
		cache:   make(map[string]*Entity),
	}
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
			toSet:         make(map[string]*Entity),
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
	}

	return err
}

// Put stores Entity e.
// If e has an incomplete key, it is updated.
func (g *Goon) Put(e *Entity) error {
	return g.PutMulti([]*Entity{e})
}

// PutMulti stores a sequence of Entities.
// Any entity with an incomplete key will be updated.
func (g *Goon) PutMulti(es []*Entity) error {
	var err error

	var memkeys []string
	keys := make([]*datastore.Key, len(es))
	src := make([]interface{}, len(es))

	for i, e := range es {
		if !e.Key.Incomplete() {
			memkeys = append(memkeys, e.memkey())
		}

		keys[i] = e.Key
		src[i] = e.Src
	}

	memcache.DeleteMulti(g.context, memkeys)

	keys, err = datastore.PutMulti(g.context, keys, src)

	if err != nil {
		return err
	}

	for i, e := range es {
		es[i].setKey(keys[i])

		if g.inTransaction {
			g.toSet[e.memkey()] = e
		}
	}

	if !g.inTransaction {
		g.putMemoryMulti(es)
	}

	return nil
}

func (g *Goon) putMemoryMulti(es []*Entity) {
	for _, e := range es {
		g.putMemory(e)
	}
}

func (g *Goon) putMemory(e *Entity) {
	g.cache[e.memkey()] = e
}

func (g *Goon) putMemcache(es []*Entity) error {
	items := make([]*memcache.Item, len(es))

	for i, e := range es {
		gob, err := e.gob()
		if err != nil {
			return err
		}

		items[i] = &memcache.Item{
			Key:   e.memkey(),
			Value: gob,
		}
	}

	err := memcache.SetMulti(g.context, items)

	if err != nil {
		return err
	}

	g.putMemoryMulti(es)
	return nil
}

// structKind returns the reflect.Kind name of src if it is a struct, else nil.
func structKind(src interface{}) (string, error) {
	v := reflect.ValueOf(src)
	v = reflect.Indirect(v)
	t := v.Type()
	k := t.Kind()

	if k == reflect.Struct {
		return t.Name(), nil
	}
	return "", errors.New("goon: src has invalid type")
}

// GetById fetches an entity of kind src by id.
// Refer to appengine/datastore.NewKey regarding key specification.
func (g *Goon) GetById(src interface{}, stringID string, intID int64, parent *datastore.Key) (*Entity, error) {
	k, err := structKind(src)
	if err != nil {
		return nil, err
	}
	key := datastore.NewKey(g.context, k, stringID, intID, parent)
	return g.Get(src, key)
}

// Get fetches an entity of kind src by key.
func (g *Goon) Get(src interface{}, key *datastore.Key) (*Entity, error) {
	e := NewEntity(key, src)
	es := []*Entity{e}
	err := g.GetMulti(es)
	if err != nil {
		return nil, err
	}
	return es[0], nil
}

// Get fetches a sequency of Entities, whose keys must already be valid.
// Entities with no correspending key have their NotFound field set to true.
func (g *Goon) GetMulti(es []*Entity) error {
	var dskeys []*datastore.Key
	var dst []interface{}
	var dixs []int

	if !g.inTransaction {
		var memkeys []string
		var mixs []int

		for i, e := range es {
			m := e.memkey()
			if s, present := g.cache[m]; present {
				es[i] = s
			} else {
				memkeys = append(memkeys, m)
				mixs = append(mixs, i)
			}
		}

		memvalues, err := memcache.GetMulti(g.context, memkeys)
		if err != nil {
			return err
		}

		for i, m := range memkeys {
			e := es[mixs[i]]
			if s, present := memvalues[m]; present {
				err := fromGob(e, s.Value)
				if err != nil {
					return err
				}

				g.putMemory(e)
			} else {
				dskeys = append(dskeys, e.Key)
				dst = append(dst, e.Src)
				dixs = append(dixs, mixs[i])
			}
		}
	} else {
		dskeys = make([]*datastore.Key, len(es))
		dst = make([]interface{}, len(es))
		dixs = make([]int, len(es))

		for i, e := range es {
			dskeys[i] = e.Key
			dst[i] = e.Src
			dixs[i] = i
		}
	}

	var merr appengine.MultiError
	err := datastore.GetMulti(g.context, dskeys, dst)
	if err != nil {
		switch err.(type) {
		case appengine.MultiError:
			merr = err.(appengine.MultiError)
		default:
			return err
		}
	}
	var mes []*Entity

	for i, idx := range dixs {
		e := es[idx]
		if merr != nil && merr[i] != nil {
			e.NotFound = true
		}
		mes = append(mes, e)
	}

	if len(mes) > 0 && !g.inTransaction {
		err = g.putMemcache(mes)
		if err != nil {
			return err
		}
	}

	multiErr, any := make(appengine.MultiError, len(es)), false
	for i, e := range es {
		if e.NotFound {
			multiErr[i] = datastore.ErrNoSuchEntity
			any = true
		}
	}

	if any {
		return multiErr
	}

	return nil
}

func fromGob(e *Entity, b []byte) error {
	var buf bytes.Buffer
	_, _ = buf.Write(b)
	gob.Register(e.Src)
	dec := gob.NewDecoder(&buf)
	t := Entity{}
	err := dec.Decode(&t)
	if err != nil {
		return err
	}

	e.NotFound = t.NotFound
	ev := reflect.Indirect(reflect.ValueOf(e.Src))

	v := reflect.Indirect(reflect.ValueOf(t.Src))
	for i := 0; i < v.NumField(); i++ {
		f := v.Field(i)
		if f.CanSet() {
			ev.Field(i).Set(f)
		}
	}

	return nil
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

	memcache.DeleteMulti(g.context, memkeys)

	return datastore.DeleteMulti(g.context, keys)
}
