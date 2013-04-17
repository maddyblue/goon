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
	"appengine/datastore"
	"reflect"
)

// Count returns the number of results for the query.
func (g *Goon) Count(q *datastore.Query) (int, error) {
	return q.Count(g.context)
}

// GetAll returns the entities in the given query. Returned entities (but not
// the query itself) are cached in local memory (but not memcache).
//
// Returns nil if q is a keys-only query or dst is not a slice.
//
// Refer to appengine/datastore.Query.Count:
// https://developers.google.com/appengine/docs/go/datastore/reference#Query.GetAll
//
// Example usage:
//   g := goon.NewGoon(r)
//   q := datastore.NewQuery("Group")
//   var gg []Group // (or []*Group)
//   es, err := g.GetAll(q, &gg)
func (g *Goon) GetAll(q *datastore.Query, dst interface{}) ([]*Entity, error) {
	keys, err := q.GetAll(g.context, dst)
	if err != nil {
		g.error(err)
		return nil, err
	}

	keysOnly := dst == nil

	var v reflect.Value
	var t reflect.Type
	if !keysOnly {
		v = reflect.Indirect(reflect.ValueOf(dst))
		t = v.Type()

		// try to detect a keys-only query
		if t.Kind() != reflect.Slice || v.Len() != len(keys) {
			keysOnly = true
		}
	}

	es := make([]*Entity, len(keys))

	for i, k := range keys {
		var e *Entity
		if keysOnly {
			e = NewEntity(k, nil)
		} else {
			e = NewEntity(k, v.Index(i).Interface())
		}
		es[i] = e

		// Do not pollute the cache with empty results from keys-only queries
		if !g.inTransaction && !keysOnly {
			g.cache[e.memkey()] = e
		}
	}

	// Before returning, update the structs to have correct key info
	if !keysOnly {
		for _, e := range es {
			if e.Src != nil {
				setStructKey(e.Src, e.Key)
			}
		}
	}

	return es, nil
}

func (g *Goon) Run(q *datastore.Query) *Iterator {
	return &Iterator{
		g: g,
		i: q.Run(g.context),
	}
}

// Iterator is the result of running a query.
type Iterator struct {
	g *Goon
	i *datastore.Iterator
}

// Cursor returns a cursor for the iterator's current location.
func (t *Iterator) Cursor() (datastore.Cursor, error) {
	return t.i.Cursor()
}

// Next returns the entity of the next result. When there are no more results,
// datastore.Done is returned as the error. If dst is null (for a keys-only
// query), nil is returned as the entity.
//
// If the query is not keys only and dst is non-nil, it also loads the entity
// stored for that key into the struct pointer dst, with the same semantics
// and possible errors as for the Get function. This is returned as an Entity
// and cached in memory.
//
// If the query is keys only, dst must be passed as nil. Otherwise the cache
// will be populated with empty entities since there is no way to detect the
// case of a keys-only query.
//
// Refer to appengine/datastore.Iterator.Next:
// https://developers.google.com/appengine/docs/go/datastore/reference#Iterator.Next
func (t *Iterator) Next(dst interface{}) (*Entity, error) {
	k, err := t.i.Next(dst)
	if err != nil {
		return nil, err
	}

	if dst == nil {
		return nil, nil
	}

	e := NewEntity(k, dst)

	if !t.g.inTransaction {
		t.g.cache[e.memkey()] = e
	}

	// Before returning, update the struct to have correct key info
	setStructKey(e.Src, e.Key)

	return e, nil
}
