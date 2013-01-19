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

// Package goon provides an autocaching interface to the app engine datastore
// similar to the python NDB package.
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

type Goon struct {
	context appengine.Context
	cache   map[string]*Entity
}

func memkey(k *datastore.Key) string {
	return k.String()
}

func NewGoon(r *http.Request) *Goon {
	return &Goon{
		context: appengine.NewContext(r),
		cache:   make(map[string]*Entity),
	}
}

func (g *Goon) Put(e *Entity) error {
	return g.PutMulti([]*Entity{e})
}

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

	err = memcache.DeleteMulti(g.context, memkeys)

	if err != nil {
		return err
	}

	keys, err = datastore.PutMulti(g.context, keys, src)

	if err != nil {
		return err
	}

	for i := range es {
		es[i].setKey(keys[i])
	}

	g.putMemoryMulti(es)

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

func (g *Goon) Get(src interface{}, stringID string, intID int64, parent *datastore.Key) (*Entity, error) {
	k, err := structKind(src)
	if err != nil {
		return nil, err
	}
	key := datastore.NewKey(g.context, k, stringID, intID, parent)
	e := NewEntity(key, src)
	err = g.GetMulti([]*Entity{e})
	if err != nil {
		return nil, err
	}
	return e, nil
}

func (g *Goon) GetMulti(es []*Entity) error {
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

	var dskeys []*datastore.Key
	var dst []interface{}
	var dixs []int

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

	var merr appengine.MultiError
	err = datastore.GetMulti(g.context, dskeys, dst)
	if err != nil {
		merr = err.(appengine.MultiError)
	}
	var mes []*Entity

	for i, idx := range dixs {
		e := es[idx]
		if merr != nil && merr[i] != nil {
			e.NotFound = true
		}
		mes = append(mes, e)
	}

	err = g.putMemcache(mes)
	if err != nil {
		return err
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
	return dec.Decode(e)
}
