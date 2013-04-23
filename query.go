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

// GetAll runs the query in the given context and returns all keys that match
// that query, as well as appending the values to dst.
//
// If q is not a "keys-only" query, GetAll sets the goon key fields of dst and
// caches returned data in memory.
//
// See: https://developers.google.com/appengine/docs/go/datastore/reference#Query.GetAll
func (g *Goon) GetAll(q *datastore.Query, dst interface{}) ([]*datastore.Key, error) {
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

	if keysOnly {
		return keys, err
	}

	for i, k := range keys {
		e := v.Index(i).Interface()
		setStructKey(&e, k)

		if !g.inTransaction {
			g.cache[memkey(k)] = &e
		}
	}

	return keys, err
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
func (t *Iterator) Next(dst interface{}) (*datastore.Key, error) {
	k, err := t.i.Next(dst)
	if err != nil {
		return nil, err
	}

	if !t.g.inTransaction && dst != nil {
		t.g.cache[memkey(k)] = dst
	}

	// Before returning, update the struct to have correct key info
	if dst != nil {
		setStructKey(dst, k)
	}

	return k, err
}
