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
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"fmt"
)

var (
	// LogErrors issues appengine.Context.Errorf on any error.
	LogErrors bool = true
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
	return FromContext(appengine.NewContext(r))
}

func FromContext(c appengine.Context) *Goon {
	return &Goon{
		context: c,
		cache:   make(map[string]*Entity),
	}
}

func (g *Goon) error(err error) {
	if LogErrors {
		g.context.Errorf(err.Error())
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
	} else {
		g.error(err)
	}

	return err
}

// Put stores Entity e.
// If e has an incomplete key, it is updated.
func (g *Goon) Put(e *Entity) error {
	return g.PutMulti([]*Entity{e})
}

// PutMany is a wrapper around PutMulti.
func (g *Goon) PutMany(es ...*Entity) error {
	return g.PutMulti(es)
}

const putMultiLimit = 500

// PutMulti stores a sequence of Entities.
// Any entity with an incomplete key will be updated.
func (g *Goon) PutMulti(es []*Entity) error {
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

	// Memcache needs to be updated after the datastore to prevent a common race condition
	defer memcache.DeleteMulti(g.context, memkeys)

	for i := 0; i <= len(src)/putMultiLimit; i++ {
		lo := i * putMultiLimit
		hi := (i + 1) * putMultiLimit
		if hi > len(src) {
			hi = len(src)
		}
		rkeys, err := datastore.PutMulti(g.context, keys[lo:hi], src[lo:hi])
		if err != nil {
			g.error(err)
			return err
		}

		for i, e := range es[lo:hi] {
			es[lo+i].setKey(rkeys[i])

			if g.inTransaction {
				g.toSet[e.memkey()] = e
			}
		}
	}

	if !g.inTransaction {
		g.putMemoryMulti(es)
	}

	// Before returning, update the structs to have correct key info
	for _, e := range es {
		if e.Src != nil {
			setStructKey(e.Src, e.Key)
		}
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
			g.error(err)
			return err
		}

		items[i] = &memcache.Item{
			Key:   e.memkey(),
			Value: gob,
		}
	}

	err := memcache.SetMulti(g.context, items)

	if err != nil {
		g.error(err)
		return err
	}

	g.putMemoryMulti(es)
	return nil
}

// Kind returns the Kind name of src, which must be a struct.
// If src has a field named _goon with a tag "kind", that is used.
// Otherwise, reflection is used to determine the type name of src.
// In the case of an error, the empty string is returned.
//
// For example, to overwrite the default "Group" kind name:
//   type Group struct {
//     _goon interface{} `kind:"something_else"`
//     Name  string
//   }
func Kind(src interface{}) string {
	v := reflect.ValueOf(src)
	v = reflect.Indirect(v)
	t := v.Type()
	k := t.Kind()

	if k == reflect.Struct {
		if f, present := t.FieldByName("_goon"); present {
			name := f.Tag.Get("kind")
			if name != "" {
				return name
			}
		}

		return t.Name()
	}
	return ""
}

// GetById fetches an entity of kind src by id.
// Refer to appengine/datastore.NewKey regarding key specification.
func (g *Goon) GetById(src interface{}, stringID string, intID int64, parent *datastore.Key) (*Entity, error) {
	return g.GetByIdKind(src, Kind(src), stringID, intID, parent)
}

// GetByIdKind fetches an entity of specified kind by id.
// Refer to appengine/datastore.NewKey regarding key specification.
func (g *Goon) GetByIdKind(src interface{}, kind, stringID string, intID int64, parent *datastore.Key) (*Entity, error) {
	key := datastore.NewKey(g.context, kind, stringID, intID, parent)
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
	for _, e := range es {
		t := reflect.TypeOf(e.Src)
		pt := reflect.Indirect(reflect.ValueOf(e.Src)).Type()
		if t.Kind() != reflect.Ptr || pt.Kind() != reflect.Struct {
			return errors.New("goon: expected *struct (ptr to struct), got struct")
		}
	}

	// Before returning, update the structs to have correct key info
	defer func() {
		for _, e := range es {
			if e.Src != nil {
				setStructKey(e.Src, e.Key)
			}
		}
	}()

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
			g.error(errors.New(fmt.Sprintf("ignored memcache error: %v", err.Error())))
			// ignore memcache errors
			//return err
		}

		for i, m := range memkeys {
			e := es[mixs[i]]
			if s, present := memvalues[m]; present {
				err := fromGob(e, s.Value)
				if err != nil {
					g.error(err)
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
			g.error(err)
			return err
		}
	}
	var mes []*Entity

	multiErr, any := make(appengine.MultiError, len(es)), false
	for i, idx := range dixs {
		e := es[idx]
		if merr != nil {
			if merr[i] == datastore.ErrNoSuchEntity {
				e.NotFound = true
			} else {
				multiErr[i] = merr[i]
				any = true
			}
		}
		mes = append(mes, e)
	}

	if len(mes) > 0 && !g.inTransaction {
		err = g.putMemcache(mes)
		if err != nil {
			return err
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

func setStructKey(src interface{}, key *datastore.Key) error {
	v := reflect.Indirect(reflect.ValueOf(src))
	t := v.Type()
	k := t.Kind()

	if k != reflect.Struct {
		return errors.New(fmt.Sprintf("goon: Expected struct, got instead: %v", k))
	}

	for i := 0; i < v.NumField(); i++ {
		tf := t.Field(i)
		vf := v.Field(i)
		
		if !vf.CanSet() {
			continue
		}

		tag := tf.Tag.Get("goon")
		if tag != "" {
			tagValues := strings.Split(tag, ",")
			for _, tagValue := range tagValues {
				if tagValue == "id" {
					if vf.Kind() == reflect.Int64 {
						vf.SetInt(key.IntID())
					} else if vf.Kind() == reflect.String {
						vf.SetString(key.StringID())
					}
				} else if tagValue == "parent" {
					if vf.Type() == reflect.TypeOf(&datastore.Key{}) {
						vf.Set(reflect.ValueOf(key.Parent()))
					}
				}
			}
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

	// Memcache needs to be updated after the datastore to prevent a common race condition
	defer memcache.DeleteMulti(g.context, memkeys)

	return datastore.DeleteMulti(g.context, keys)
}
