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
	"container/list"
	"reflect"
	"sync"
)

var cachedValueOverhead int

func init() {
	// Calculate the platform dependant overhead size for keeping a value in cache
	var elem list.Element
	var ci cacheItem
	cachedValueOverhead += int(reflect.TypeOf(elem).Size())   // list.Element in cache.accessed
	cachedValueOverhead += int(reflect.TypeOf(&elem).Size())  // *list.Element in cache.elements
	cachedValueOverhead += int(reflect.TypeOf(ci.key).Size()) // string in cache.elements as key
	cachedValueOverhead += int(reflect.TypeOf(ci).Size())     // cacheItem pointed to by list.Element.Value
	// In addition to the above overhead, the total cache value size must include
	// the length of the key string in bytes and the cap of the value []byte
}

type cacheItem struct {
	key   string
	value []byte
}

type cache struct {
	lock     sync.Mutex
	elements map[string]*list.Element // access via key
	accessed list.List                // most recently accessed in front
	size     int                      // Total size of all the values in the cache
	limit    int                      // Maximum size allowed
}

const defaultCacheLimit = 16 << 20 // 16 MiB

func newCache(limit int) *cache {
	return &cache{elements: map[string]*list.Element{}, limit: limit}
}

func (c *cache) setLimit(limit int) {
	c.lock.Lock()
	c.limit = limit
	c.meetLimitUnderLock()
	c.lock.Unlock()
}

// meetLimit must be called under cache.lock
func (c *cache) meetLimitUnderLock() {
	for c.size > c.limit {
		e := c.accessed.Back()
		if e == nil {
			break
		}
		c.deleteExistingUnderLock(e)
	}
}

// setUnderLock must be called under cache.lock
func (c *cache) setUnderLock(item *cacheItem) {
	// Check if there's already an entry for this key
	if e, ok := c.elements[item.key]; ok {
		// There already exists a value for this key, so update it
		ci := e.Value.(*cacheItem)
		c.size += cap(item.value) - cap(ci.value)
		// Make sure that item.key is the same pointer as the map key,
		// as this will ensure faster map lookup via pointer equality.
		// Not doing so would also lead to double memory usage, as the two key
		// pointers would be pointing to the same contents in different places.
		item.key = ci.key
		e.Value = item
		c.accessed.MoveToFront(e)
	} else {
		// Brand new key, so add it
		c.size += cachedValueOverhead + len(item.key) + cap(item.value)
		c.elements[item.key] = c.accessed.PushFront(item)
	}
}

// Set takes ownership of item
func (c *cache) Set(item *cacheItem) {
	c.lock.Lock()
	c.setUnderLock(item)
	c.meetLimitUnderLock()
	c.lock.Unlock()
}

// SetMulti takes ownership of item
func (c *cache) SetMulti(items []*cacheItem) {
	c.lock.Lock()
	for _, item := range items {
		c.setUnderLock(item)
	}
	c.meetLimitUnderLock()
	c.lock.Unlock()
}

// deleteExistingUnderLock must be called under cache.lock
// The specified element must be non-nil and be guaranteed to exist in the cache
func (c *cache) deleteExistingUnderLock(e *list.Element) {
	ci := e.Value.(*cacheItem)
	c.size -= cachedValueOverhead + len(ci.key) + cap(ci.value)
	delete(c.elements, ci.key)
	c.accessed.Remove(e)
}

// deleteUnderLock must be called under cache.lock
func (c *cache) deleteUnderLock(key string) {
	if e, ok := c.elements[key]; ok {
		c.deleteExistingUnderLock(e)
	}
}

func (c *cache) Delete(key string) {
	c.lock.Lock()
	c.deleteUnderLock(key)
	c.lock.Unlock()
}

func (c *cache) DeleteMulti(keys []string) {
	c.lock.Lock()
	for _, key := range keys {
		c.deleteUnderLock(key)
	}
	c.lock.Unlock()
}

// getUnderLock must be called under cache.lock
func (c *cache) getUnderLock(key string) []byte {
	if e, ok := c.elements[key]; ok {
		c.accessed.MoveToFront(e)
		return (e.Value.(*cacheItem)).value
	}
	return nil
}

// The cache retains ownership of the []byte, so consider it immutable
func (c *cache) Get(key string) []byte {
	c.lock.Lock()
	result := c.getUnderLock(key)
	c.lock.Unlock()
	return result
}

// The cache retains ownership of the []byte, so consider it immutable
func (c *cache) GetMulti(keys []string) [][]byte {
	c.lock.Lock()
	result := make([][]byte, 0, len(keys))
	for _, key := range keys {
		result = append(result, c.getUnderLock(key))
	}
	c.lock.Unlock()
	return result
}

func (c *cache) Flush() {
	c.lock.Lock()
	c.size = 0
	c.elements = map[string]*list.Element{}
	c.accessed.Init()
	c.lock.Unlock()
}
