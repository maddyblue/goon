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
	"bytes"
	"reflect"
	"testing"
	"unsafe"
)

func TestCacheBasics(t *testing.T) {
	items := []*cacheItem{}
	items = append(items, &cacheItem{key: "foo", value: []byte{1, 2, 3}})
	items = append(items, &cacheItem{key: "bar", value: []byte{4, 5, 6}})

	keys := make([]string, 0, len(items))
	for _, item := range items {
		keys = append(keys, item.key)
	}

	c := newCache(defaultCacheLimit)

	for i := range items {
		if v := c.Get(items[i].key); v != nil {
			t.Fatalf("Expected nil for items[%d] but got %v", i, v)
		}

		c.Set(items[i])

		if v := c.Get(items[i].key); !bytes.Equal(v, items[i].value) {
			t.Fatalf("Invalid bytes for items[%d]! %x vs %x", i, v, items[i].value)
		}

		c.Delete(items[i].key)

		if v := c.Get(items[i].key); v != nil {
			t.Fatalf("Expected nil for items[%d] but got %v", i, v)
		}

		if c.size != 0 {
			t.Fatalf("Expected size to be zero, but got %v", c.size)
		}
	}

	if vs := c.GetMulti(keys); !reflect.DeepEqual(vs, [][]byte{nil, nil}) {
		t.Fatalf("Expected nils but got %+v", vs)
	}

	c.SetMulti(items)

	if vs := c.GetMulti(keys); !reflect.DeepEqual(vs, [][]byte{items[0].value, items[1].value}) {
		t.Fatalf("Invalid bytes for items! %+v", vs)
	}

	c.DeleteMulti(keys)

	if vs := c.GetMulti(keys); !reflect.DeepEqual(vs, [][]byte{nil, nil}) {
		t.Fatalf("Expected nils but got %+v", vs)
	}

	if c.size != 0 {
		t.Fatalf("Expected size to be zero, but got %v", c.size)
	}

	c.Set(items[0])
	c.Flush()
	if v := c.Get(items[0].key); v != nil {
		t.Fatalf("Expected nil after flush but got %v", v)
	}

	c.Set(items[0])
	c.Set(&cacheItem{key: items[0].key, value: []byte{7, 7, 7}})
	if v := c.Get(items[0].key); !bytes.Equal(v, []byte{7, 7, 7}) {
		t.Fatalf("Invalid bytes for value change! Got %x", v)
	}
}

func TestCacheKeyLeak(t *testing.T) {
	ak, bk := string([]byte{'f', 'o', 'o'}), string([]byte{'f', 'o', 'o'})
	av, bv := []byte{1, 2, 3}, []byte{4, 5, 6}

	c := newCache(defaultCacheLimit)

	// Set the original value
	c.Set(&cacheItem{key: ak, value: av})
	if v := c.Get(ak); !bytes.Equal(v, av) {
		t.Fatalf("Invalid bytes! %v", v)
	}

	// Rewrite it with a different value, and also a different key but same key contents
	c.Set(&cacheItem{key: bk, value: bv})
	if v := c.Get(bk); !bytes.Equal(v, bv) {
		t.Fatalf("Invalid bytes! %v", v)
	}

	// Modify the new key contents without changing the pointer
	*(*byte)(unsafe.Pointer(*(*uintptr)(unsafe.Pointer(&bk)))) = 'g'
	if bk != "goo" {
		t.Fatalf("Expected key to be 'goo' but it's %v", bk)
	}

	// Make sure that we can no longer retrieve the value with the new key,
	// as that will only be possible via pointer equality, which means that
	// the cache is still holding on to the new key, which doubles key storage
	if v := c.Get(bk); v != nil {
		t.Fatalf("Cache is leaking memory by keeping around the new key pointer! %v", v)
	}
	// Also make sure that we can retrieve the correct new value
	// by using an unrelated key pointer that just matches the key contents
	if v := c.Get("foo"); !bytes.Equal(v, bv) {
		t.Fatalf("Invalid bytes! %v", v)
	}

	// Inspect the internals of the cache too, which contains only a single entry with ak as key
	keyAddr := *(*uintptr)(unsafe.Pointer(&ak))
	for key, elem := range c.elements {
		if ka := *(*uintptr)(unsafe.Pointer(&key)); ka != keyAddr {
			t.Fatalf("map key has wrong pointer! %x vs %x", ka, keyAddr)
		}
		if ka := *(*uintptr)(unsafe.Pointer(&(elem.Value.(*cacheItem)).key)); ka != keyAddr {
			t.Fatalf("element key has wrong pointer! %x vs %x", ka, keyAddr)
		}
	}
}

func TestCacheLimit(t *testing.T) {
	c := newCache(defaultCacheLimit)

	items := []*cacheItem{}
	items = append(items, &cacheItem{key: "foo", value: []byte{1, 2, 3}})
	items = append(items, &cacheItem{key: "bar", value: []byte{4, 5, 6}})

	keys := make([]string, 0, len(items))
	for _, item := range items {
		keys = append(keys, item.key)
	}

	if vs := c.GetMulti(keys); !reflect.DeepEqual(vs, [][]byte{nil, nil}) {
		t.Fatalf("Expected nils but got %+v", vs)
	}

	c.SetMulti(items)

	if vs := c.GetMulti(keys); !reflect.DeepEqual(vs, [][]byte{items[0].value, items[1].value}) {
		t.Fatalf("Invalid bytes for items! %+v", vs)
	}

	c.setLimit(0)

	if vs := c.GetMulti(keys); !reflect.DeepEqual(vs, [][]byte{nil, nil}) {
		t.Fatalf("Expected nils but got %+v", vs)
	}

	if c.size != 0 {
		t.Fatalf("Expected size to be zero, but got %v", c.size)
	}

	c.setLimit(cachedValueOverhead + len(items[1].key) + cap(items[1].value))

	c.SetMulti(items)

	if vs := c.GetMulti(keys); !reflect.DeepEqual(vs, [][]byte{nil, items[1].value}) {
		t.Fatalf("Invalid bytes for items! %+v", vs)
	}
}
