/*
 * Copyright (c) 2012 The Goon Authors
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
	"fmt"
	"reflect"

	"google.golang.org/appengine/v2/datastore"
)

// Count returns the number of results for the query.
func (g *Goon) Count(q *datastore.Query) (int, error) {
	return q.Count(g.Context)
}

// GetAll runs the query and returns all the keys that match the query, as well
// as appending the values to dst, setting the goon key fields of dst, and
// caching the returned data in local memory.
//
// For "keys-only" queries dst can be nil, however if it is not, then GetAll
// appends zero value structs to dst, only setting the goon key fields.
//
// No data is cached with projection or "keys-only" queries.
//
// See: https://developers.google.com/appengine/docs/go/datastore/reference#Query.GetAll
func (g *Goon) GetAll(q *datastore.Query, dst interface{}) ([]*datastore.Key, error) {
	v := reflect.ValueOf(dst)
	dstV := reflect.Indirect(v)
	vLenBefore := 0
	if dst != nil {
		if v.Kind() != reflect.Ptr {
			return nil, fmt.Errorf("goon: Expected dst to be a pointer to a slice or nil, got instead: %v", v.Kind())
		}
		v = v.Elem()
		if v.Kind() != reflect.Slice {
			return nil, fmt.Errorf("goon: Expected dst to be a pointer to a slice or nil, got instead: %v", v.Kind())
		}
		vLenBefore = v.Len()
	}

	var propLists []datastore.PropertyList
	keys, err := q.GetAll(g.Context, &propLists)
	if err != nil {
		g.error(err)
		return keys, err
	}
	if dst == nil || len(keys) == 0 {
		return keys, err
	}

	projection := false
	if len(propLists) > 0 {
		for i := range propLists {
			if len(propLists[i]) > 0 {
				projection = isIndexValue(&propLists[i][0])
				break
			}
		}
	}
	keysOnly := (len(propLists) != len(keys))
	updateCache := !g.inTransaction && !keysOnly && !projection

	elemType := v.Type().Elem()
	elemTypeIsPtr := false
	if elemType.Kind() == reflect.Ptr {
		elemType = elemType.Elem()
		elemTypeIsPtr = true
	}
	if elemType.Kind() != reflect.Struct {
		return keys, fmt.Errorf("goon: Expected struct, got instead: %v", elemType.Kind())
	}

	initMem := false
	finalLen := vLenBefore + len(keys)
	if v.Cap() < finalLen {
		// If the slice doesn't have enough capacity for the final length,
		// then create a new slice with the exact capacity needed,
		// with all elements zero-value initialized already.
		newSlice := reflect.MakeSlice(v.Type(), finalLen, finalLen)
		if copied := reflect.Copy(newSlice, v); copied != vLenBefore {
			return keys, fmt.Errorf("goon: Wanted to copy %v elements to dst but managed %v", vLenBefore, copied)
		}
		v = newSlice
	} else {
		// If the slice already has enough capacity ..
		if elemTypeIsPtr {
			// .. then just change the length if it's a slice of pointers,
			// because we will overwrite all the pointers anyway.
			v.SetLen(finalLen)
		} else {
			// .. we need to initialize the memory of every non-pointer element.
			initMem = true
		}
	}

	var toCache []*cacheItem
	if updateCache {
		toCache = make([]*cacheItem, 0, len(keys))
	}
	var rerr error

	for i, k := range keys {
		if elemTypeIsPtr {
			ev := reflect.New(elemType)
			v.Index(vLenBefore + i).Set(ev)
		} else if initMem {
			ev := reflect.New(elemType).Elem()
			v = reflect.Append(v, ev)
		}
		vi := v.Index(vLenBefore + i)

		var e interface{}
		if vi.Kind() == reflect.Ptr {
			e = vi.Interface()
		} else {
			e = vi.Addr().Interface()
		}

		if !keysOnly {
			if err := deserializeProperties(e, propLists[i]); err != nil {
				if errFieldMismatch(err) {
					// If we're not configured to ignore, set rerr to err,
					// but proceed with deserializing other entities
					if !IgnoreFieldMismatch {
						rerr = err
					}
				} else {
					return nil, err
				}
			}
		}

		if err := g.setStructKey(e, k); err != nil {
			return nil, err
		}

		if updateCache {
			// Serialize the properties
			data, err := serializeProperties(propLists[i], true)
			if err != nil {
				g.error(err)
				return nil, err
			}
			// Prepare the properties for caching
			toCache = append(toCache, &cacheItem{key: cacheKey(k), value: data})
		}
	}

	if len(toCache) > 0 {
		g.cache.SetMulti(toCache)
	}

	// Set dst to the slice we created
	dstV.Set(v)

	return keys, rerr
}

// Run runs the query.
func (g *Goon) Run(q *datastore.Query) *Iterator {
	return &Iterator{
		g: g,
		i: q.Run(g.Context),
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
// datastore.Done is returned as the error.
//
// If the query is not keys only and dst is non-nil, it also loads the entity
// stored for that key into the struct pointer dst, with the same semantics
// and possible errors as for the Get function.
//
// This result is cached in memory, unless it's a projection query.
//
// If the query is keys only and dst is non-nil, dst will be given the right id.
//
// Refer to appengine/datastore.Iterator.Next:
// https://developers.google.com/appengine/docs/go/datastore/reference#Iterator.Next
func (t *Iterator) Next(dst interface{}) (*datastore.Key, error) {
	var props datastore.PropertyList
	k, err := t.i.Next(&props)
	if err != nil {
		return k, err
	}
	var rerr error
	if dst != nil {
		projection := false
		if len(props) > 0 {
			projection = isIndexValue(&props[0])
		}
		keysOnly := (props == nil)
		updateCache := !t.g.inTransaction && !keysOnly && !projection
		if !keysOnly {
			if err := deserializeProperties(dst, props); err != nil {
				if errFieldMismatch(err) {
					// If we're not configured to ignore, set rerr to err,
					// but proceed with work
					if !IgnoreFieldMismatch {
						rerr = err
					}
				} else {
					return k, err
				}
			}
		}
		if err := t.g.setStructKey(dst, k); err != nil {
			return k, err
		}
		if updateCache {
			data, err := serializeProperties(props, true)
			if err != nil {
				return k, err
			}
			t.g.cache.Set(&cacheItem{key: cacheKey(k), value: data})
		}
	}
	return k, rerr
}
