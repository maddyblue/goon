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
	"context"
	"encoding/ascii85"
	"fmt"
	"net/http"
	"path/filepath"
	"reflect"
	"runtime"
	"sync"
	"time"

	"golang.org/x/crypto/blake2b"
	"google.golang.org/appengine"
	"google.golang.org/appengine/datastore"
	"google.golang.org/appengine/log"
	"google.golang.org/appengine/memcache"
)

var (
	// LogErrors issues appengine.Context.Errorf on any error.
	LogErrors = true
	// LogTimeoutErrors issues appengine.Context.Warningf on memcache timeout errors.
	LogTimeoutErrors = false

	// MemcachePutTimeoutThreshold is the number of bytes after which the large
	// timeout setting is added to the small timeout setting. This repeats.
	// Which means that with a 20 KiB threshold and a 100 KiB memcache payload,
	// the final timeout is MemcachePutTimeoutSmall + 5*MemcachePutTimeoutLarge
	MemcachePutTimeoutThreshold = 20 * 1024 // 20 KiB
	// MemcachePutTimeoutSmall is the minimum time to wait during memcache
	// Put operations before aborting them and using the datastore.
	MemcachePutTimeoutSmall = 5 * time.Millisecond
	// MemcachePutTimeoutLarge is the amount of extra time to wait for larger
	// memcache Put requests. See also MemcachePutTimeoutThreshold.
	MemcachePutTimeoutLarge = 1 * time.Millisecond
	// MemcacheGetTimeout is the amount of time to wait for all memcache Get
	// requests, per key fetched. Because we can't really know how big entities
	// we are requesting, this setting should be for the maximum size entity.
	// The final timeout is limited to the number of maximum sized entities
	// an RPC result can contain, so the timeout won't grow insanely large
	// if you're fetching a ton of small entities.
	MemcacheGetTimeout = 31250 * time.Microsecond // 31.25 milliseconds

	// IgnoreFieldMismatch decides whether *datastore.ErrFieldMismatch errors
	// should be silently ignored. This allows you to easily remove fields from structs.
	IgnoreFieldMismatch = true
)

var (
	// Determines if memcache.PutMulti errors are returned by goon.
	// Currently only meant for use during goon development testing.
	propagateMemcachePutError = false
)

// Goon holds the app engine context and the request memory cache.
type Goon struct {
	Context       context.Context
	cache         *cache
	inTransaction bool
	txnCacheLock  sync.Mutex // protects toDelete / toDeleteMC
	toDelete      map[string]struct{}
	toDeleteMC    map[string]struct{}
	// KindNameResolver is used to determine what Kind to give an Entity.
	// Defaults to DefaultKindName
	KindNameResolver KindNameResolver
}

// MemcacheKey returns the string form of the provided datastore key.
var MemcacheKey = func(k *datastore.Key) string {
	return k.Encode()
}

// Versioning, so that incompatible changes to the cache system won't cause problems
var cacheKeyPrefix = fmt.Sprintf("g%X:", serializationFormatVersion)

// cacheKey returns the fully legal string key used for cache systems
func cacheKey(k *datastore.Key) string {
	// By default we just use the prefix + MemcacheKey result
	key := cacheKeyPrefix + MemcacheKey(k)
	// However if the resulting key length exceeds the maximum allowed ..
	if len(key) > memcacheMaxKeySize {
		// .. then we need to shorten it while still staying unique.
		// We pass the key through the BLAKE2b hash function,
		// which is cryptographically secure but also very fast.
		// We request an output of 24 bytes (192-bit) for speed reasons.
		h, err := blake2b.New(24, nil)
		if err != nil {
			panic(fmt.Sprintf("Unexpected error initializing blake2b: %v", err))
		}
		h.Write([]byte(key))
		hash := h.Sum(make([]byte, 0, 24))
		// After hashing, we encode the results with ascii85.
		// Ascii85 works in 4 byte chunks, generating 5 letters for each.
		// Which means we turn our 24 byte hash into a 30 letter string.
		//
		// We want letters instead of using the hash directly for debug reasons.
		// As it will be easier for people to observe & manage keys manually.
		//
		// We aim the length of 30, because 32 is the maximum length
		// where the Go std map does key lookups directly without hashing.
		encoded := make([]byte, 30, 30)
		ascii85.Encode(encoded, hash)
		key = string(encoded)
	}
	return key
}

// Returns the duration that should be used for a memcache.GetMulti timeout.
func memcacheGetTimeout(keyCount int) time.Duration {
	// Takes the number of keys given to memcache.GetMulti,
	// clamps that to the maximum number of max sized items,
	// and multiplies it with the per-max-sized-item timeout.
	if keyCount > memcacheMaxRPCSize/memcacheMaxItemSize {
		keyCount = memcacheMaxRPCSize / memcacheMaxItemSize
	}
	return time.Duration(keyCount) * MemcacheGetTimeout
}

// Returns the duration that should be used for a memcache.PutMulti timeout.
func memcachePutTimeout(payloadSize int) time.Duration {
	// Takes the payload size given to memcache.PutMulti,
	// and returns a dynamic duration based on that size.
	return MemcachePutTimeoutSmall + time.Duration(payloadSize/MemcachePutTimeoutThreshold)*MemcachePutTimeoutLarge
}

// Memcache limits
// These are based on the constants in SDK/google/appengine/api/memcache/memcache_stub.py
// Also documented in https://cloud.google.com/appengine/docs/standard/go/memcache/
const (
	// The Google provided overhead is 73 bytes, which seems wrong in practice.
	// Testing has shown that Put/PutMulti will succeed with even a lower overhead.
	// However by 'succeed' I mean not erroring. The value never gets stored.
	// The actual overhead seems to be 76 at which point the items will
	// start being stored in memcache and show up in the statistics.
	memcacheOverhead     = 76
	memcacheMaxKeySize   = 250
	memcacheMaxItemSize  = 1 << 20 // 1 MiB
	memcacheMaxValueSize = memcacheMaxItemSize - memcacheMaxKeySize - memcacheOverhead
	memcacheMaxRPCSize   = 32 << 20 // 32 MiB
)

// Datastore limits
const (
	datastoreGetMultiMaxItems    = 1000
	datastorePutMultiMaxItems    = 500
	datastoreDeleteMultiMaxItems = 500

	// The maximum GetMulti result RPC size was determined experimentally on 2019-05-20
	datastoreGetMultiMaxRPCSize = 50 << 20 // 50 MiB
)

// NewGoon creates a new Goon object from the given request.
func NewGoon(r *http.Request) *Goon {
	return FromContext(appengine.NewContext(r))
}

// FromContext creates a new Goon object from the given appengine Context.
// Useful with profiling packages like appstats.
func FromContext(c context.Context) *Goon {
	return &Goon{
		Context:          c,
		cache:            newCache(defaultCacheLimit),
		KindNameResolver: DefaultKindName,
	}
}

func (g *Goon) error(err error) {
	if !LogErrors {
		return
	}
	_, filename, line, ok := runtime.Caller(1)
	if ok {
		log.Errorf(g.Context, "goon - %s:%d - %v", filepath.Base(filename), line, err)
	} else {
		log.Errorf(g.Context, "goon - %v", err)
	}
}

func (g *Goon) timeoutError(err error) {
	if LogTimeoutErrors {
		log.Warningf(g.Context, "goon memcache timeout: %v", err)
	}
}

func (g *Goon) memcacheDeleteError(err error) {
	if err == nil || !LogErrors {
		return
	}
	if me, ok := err.(appengine.MultiError); ok {
		for i := range me {
			if me[i] != nil && me[i] != memcache.ErrCacheMiss {
				err = me[i]
				goto reportError
			}
		}
		return
	}
reportError:
	_, filename, line, ok := runtime.Caller(1)
	if ok {
		log.Errorf(g.Context, "goon - %s:%d - memcache.DeleteMulti failed: %v - the goon cache may be out of sync now!", filepath.Base(filename), line, err)
	} else {
		log.Errorf(g.Context, "goon - memcache.DeleteMulti failed: %v - the goon cache may be out of sync now!", err)
	}
}

func (g *Goon) extractKeys(src interface{}, putRequest bool) ([]*datastore.Key, error) {
	v := reflect.Indirect(reflect.ValueOf(src))
	if v.Kind() != reflect.Slice {
		return nil, fmt.Errorf("goon: value must be a slice or pointer-to-slice")
	}
	l := v.Len()

	keys := make([]*datastore.Key, l)
	for i := 0; i < l; i++ {
		vi := v.Index(i)
		key, hasStringId, err := g.getStructKey(vi.Interface())
		if err != nil {
			return nil, err
		}
		if !putRequest && key.Incomplete() {
			return nil, fmt.Errorf("goon: cannot find a key for struct - %v", vi.Interface())
		} else if putRequest && key.Incomplete() && hasStringId {
			return nil, fmt.Errorf("goon: empty string id on put")
		}
		keys[i] = key
	}
	return keys, nil
}

// Key is the same as KeyError, except nil is returned on error or if the key
// is incomplete.
func (g *Goon) Key(src interface{}) *datastore.Key {
	if k, err := g.KeyError(src); err == nil {
		return k
	}
	return nil
}

// Kind returns src's datastore Kind or "" on error.
func (g *Goon) Kind(src interface{}) string {
	if k, err := g.KeyError(src); err == nil {
		return k.Kind()
	}
	return ""
}

// KeyError returns the key of src based on its properties.
func (g *Goon) KeyError(src interface{}) (*datastore.Key, error) {
	key, _, err := g.getStructKey(src)
	return key, err
}

// RunInTransaction runs f in a transaction. It calls f with a transaction
// context tg that f should use for all App Engine operations. Neither cache nor
// memcache are used or set during a transaction.
//
// Otherwise similar to appengine/datastore.RunInTransaction:
// https://developers.google.com/appengine/docs/go/datastore/reference#RunInTransaction
func (g *Goon) RunInTransaction(f func(tg *Goon) error, opts *datastore.TransactionOptions) error {
	var ng *Goon
	err := datastore.RunInTransaction(g.Context, func(tc context.Context) error {
		ng = &Goon{
			Context:          tc,
			inTransaction:    true,
			toDelete:         make(map[string]struct{}),
			toDeleteMC:       make(map[string]struct{}),
			KindNameResolver: g.KindNameResolver,
		}
		return f(ng)
	}, opts)

	if err == nil {
		ng.txnCacheLock.Lock()
		defer ng.txnCacheLock.Unlock()
		if len(ng.toDeleteMC) > 0 {
			var memkeys []string
			for k := range ng.toDeleteMC {
				memkeys = append(memkeys, k)
			}
			g.memcacheDeleteError(memcache.DeleteMulti(g.Context, memkeys))
		}
		for k := range ng.toDelete {
			g.cache.Delete(k)
		}
	} else {
		g.error(err)
	}

	return err
}

// Put saves the entity src into the datastore based on src's key k. If k
// is an incomplete key, the returned key will be a unique key generated by
// the datastore.
func (g *Goon) Put(src interface{}) (*datastore.Key, error) {
	v := reflect.ValueOf(src)
	if v.Kind() != reflect.Ptr {
		return nil, fmt.Errorf("goon: expected pointer to a struct, got %#v", src)
	}
	ks, err := g.PutMulti([]interface{}{src})
	if err != nil {
		if me, ok := err.(appengine.MultiError); ok {
			return nil, me[0]
		}
		return nil, err
	}
	return ks[0], nil
}

// PutMulti is a batch version of Put.
//
// src must be a *[]S, *[]*S, *[]I, []S, []*S, or []I, for some struct type S,
// or some interface type I. If *[]I or []I, each element must be a struct pointer.
func (g *Goon) PutMulti(src interface{}) ([]*datastore.Key, error) {
	keys, err := g.extractKeys(src, true) // allow incomplete keys on a Put request
	if err != nil {
		return nil, err
	}

	v := reflect.Indirect(reflect.ValueOf(src))
	mu := new(sync.Mutex)
	multiErr, any := make(appengine.MultiError, len(keys)), false
	goroutines := (len(keys)-1)/datastorePutMultiMaxItems + 1
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func(i int) {
			defer wg.Done()
			lo := i * datastorePutMultiMaxItems
			hi := (i + 1) * datastorePutMultiMaxItems
			if hi > len(keys) {
				hi = len(keys)
			}
			rkeys, pmerr := datastore.PutMulti(g.Context, keys[lo:hi], v.Slice(lo, hi).Interface())
			if pmerr != nil {
				mu.Lock()
				any = true // this flag tells PutMulti to return multiErr later
				mu.Unlock()
				merr, ok := pmerr.(appengine.MultiError)
				if !ok {
					g.error(pmerr)
					for j := lo; j < hi; j++ {
						multiErr[j] = pmerr
					}
					return
				}
				copy(multiErr[lo:hi], merr)
			}

			for i, key := range keys[lo:hi] {
				if multiErr[lo+i] != nil {
					continue // there was an error writing this value, go to next
				}
				vi := v.Index(lo + i).Interface()
				if key.Incomplete() {
					g.setStructKey(vi, rkeys[i])
					keys[lo+i] = rkeys[i]
				}
			}
		}(i)
	}
	wg.Wait()

	// Caches need to be updated after the datastore to prevent a common race condition,
	// where a concurrent request will fetch the not-yet-updated data from the datastore
	// and populate the caches with it.
	cachekeys := make([]string, 0, len(keys))
	for _, key := range keys {
		if !key.Incomplete() {
			cachekeys = append(cachekeys, cacheKey(key))
		}
	}
	if g.inTransaction {
		g.txnCacheLock.Lock()
		for _, ck := range cachekeys {
			g.toDelete[ck] = struct{}{}
			g.toDeleteMC[ck] = struct{}{}
		}
		g.txnCacheLock.Unlock()
	} else {
		g.cache.DeleteMulti(cachekeys)
		g.memcacheDeleteError(memcache.DeleteMulti(g.Context, cachekeys))
	}

	if any {
		return keys, realError(multiErr)
	}
	return keys, nil
}

// FlushLocalCache clears the local memory cache.
func (g *Goon) FlushLocalCache() {
	g.cache.Flush()
}

type memcacheTask struct {
	items []*memcache.Item
	size  int
}

// NB! putMemcache is expected to treat cacheItem as immutable!
func (g *Goon) putMemcache(citems []*cacheItem) error {
	// Go over all the cache items and generate memcache tasks from them,
	// by splitting them up based on payload size
	items := make([]*memcache.Item, len(citems))
	tasks := make([]memcacheTask, 0, 1) // In most cases there's just a single task
	lastSplit := 0
	payloadSize := 0
	for i, citem := range citems {
		items[i] = &memcache.Item{
			Key:   citem.key,
			Value: citem.value,
		}
		itemSize := memcacheOverhead + len(citem.key) + len(citem.value)
		if payloadSize+itemSize > memcacheMaxRPCSize {
			tasks = append(tasks, memcacheTask{items: items[lastSplit:i], size: payloadSize})
			lastSplit = i
			payloadSize = 0
		}
		payloadSize += itemSize
	}
	tasks = append(tasks, memcacheTask{items: items[lastSplit:len(citems)], size: payloadSize})
	// Execute all the tasks with goroutines
	count := len(tasks)
	errc := make(chan error, count)
	for i := 0; i < count; i++ {
		go func(idx int) {
			tc, cf := context.WithTimeout(g.Context, memcachePutTimeout(tasks[idx].size))
			errc <- memcache.SetMulti(tc, tasks[idx].items)
			cf()
		}(i)
	}
	// Wait for all goroutines to finish and log any errors.
	// Also return a non-deterministic error if there are any.
	var rerr error
	for i := 0; i < count; i++ {
		err := <-errc
		if err != nil {
			if appengine.IsTimeoutError(err) {
				g.timeoutError(err)
			} else {
				g.error(err)
			}
			rerr = err
		}
	}
	return rerr
}

// Get loads the entity based on dst's key into dst
// If there is no such entity for the key, Get returns
// datastore.ErrNoSuchEntity.
func (g *Goon) Get(dst interface{}) error {
	v := reflect.ValueOf(dst)
	if v.Kind() != reflect.Ptr {
		return fmt.Errorf("goon: expected pointer to a struct, got %#v", dst)
	}
	if !v.CanSet() {
		v = v.Elem()
	}
	dsts := []interface{}{dst}
	if err := g.GetMulti(dsts); err != nil {
		// Look for an embedded error if it's multi
		if me, ok := err.(appengine.MultiError); ok {
			return me[0]
		}
		// Not multi, normal error
		return err
	}
	v.Set(reflect.Indirect(reflect.ValueOf(dsts[0])))
	return nil
}

// GetMulti is a batch version of Get.
//
// dst must be a *[]S, *[]*S, *[]I, []S, []*S, or []I, for some struct type S,
// or some interface type I. If *[]I or []I, each element must be a struct pointer.
func (g *Goon) GetMulti(dst interface{}) error {
	keys, err := g.extractKeys(dst, false) // don't allow incomplete keys on a Get request
	if err != nil {
		return err
	}

	v := reflect.Indirect(reflect.ValueOf(dst))

	multiErr, anyErr := make(appengine.MultiError, len(keys)), false
	var extraErr error

	if g.inTransaction {
		// todo: support getMultiLimit in transactions
		if err := datastore.GetMulti(g.Context, keys, v.Interface()); err != nil {
			if merr, ok := err.(appengine.MultiError); ok {
				for i := 0; i < len(keys); i++ {
					if merr[i] != nil && (!IgnoreFieldMismatch || !errFieldMismatch(merr[i])) {
						anyErr = true // this flag tells GetMulti to return multiErr later
						multiErr[i] = merr[i]
					}
				}
			} else {
				g.error(err)
				anyErr = true // this flag tells GetMulti to return multiErr later
				for i := 0; i < len(keys); i++ {
					multiErr[i] = err
				}
			}
			if anyErr {
				return realError(multiErr)
			}
		}
		return nil
	}

	var dskeys []*datastore.Key
	var dsdst []interface{}
	var dixs []int // dskeys[5] === keys[dixs[5]]

	var mckeys []string
	var mixs []int // mckeys[3] =~= keys[mixs[3]]

	lckeys := make([]string, 0, len(keys))
	for _, key := range keys {
		// NB! Current implementation has optimizations in place
		// that expect memcache & local cache keys to match.
		lckeys = append(lckeys, cacheKey(key))
	}

	lcvalues := g.cache.GetMulti(lckeys)

	for i, key := range keys {
		vi := v.Index(i)
		if vi.Kind() == reflect.Struct {
			vi = vi.Addr()
		}
		d := vi.Interface()

		if data := lcvalues[i]; data != nil {
			// Attempt to deserialize the cached value into the struct
			err := deserializeStruct(d, data)
			if err != nil && (!IgnoreFieldMismatch || !errFieldMismatch(err)) {
				if err == datastore.ErrNoSuchEntity || errFieldMismatch(err) {
					anyErr = true // this flag tells GetMulti to return multiErr later
					multiErr[i] = err
				} else {
					g.error(err)
					return err
				}
			}
		} else {
			mckeys = append(mckeys, lckeys[i])
			mixs = append(mixs, i)
			dskeys = append(dskeys, key)
			dsdst = append(dsdst, d)
			dixs = append(dixs, i)
		}
	}

	if len(mckeys) == 0 {
		if anyErr {
			return realError(multiErr)
		}
		return nil
	}

	// memcache.GetMulti is limited to memcacheMaxRPCSize for the data returned.
	// Thus if the returned data is bigger than memcacheMaxRPCSize - memcacheMaxItemSize
	// then we do another memcache.GetMulti on the missing keys.
	memvalues := make(map[string]*memcache.Item, len(mckeys))
	mcKeysSet := make(map[string]struct{}, len(mckeys))
	for _, mk := range mckeys {
		mcKeysSet[mk] = struct{}{}
	}
	for {
		nextmckeys := make([]string, 0, len(mcKeysSet))
		for mk := range mcKeysSet {
			nextmckeys = append(nextmckeys, mk)
		}
		tc, cf := context.WithTimeout(g.Context, memcacheGetTimeout(len(nextmckeys)))
		mvs, err := memcache.GetMulti(tc, nextmckeys)
		cf()
		// timing out or another error from memcache isn't something to fail over, but do log it
		if appengine.IsTimeoutError(err) {
			g.timeoutError(err)
			break
		} else if err != nil {
			g.error(err)
			break
		}
		payloadSize := 0
		for k, v := range mvs {
			memvalues[k] = v
			payloadSize += memcacheOverhead + len(v.Key) + len(v.Value)
			delete(mcKeysSet, k)
		}
		if len(mcKeysSet) == 0 || payloadSize < memcacheMaxRPCSize-memcacheMaxItemSize {
			break
		}
	}

	if len(memvalues) > 0 {
		// since memcache fetch was successful, reset the datastore fetch list and repopulate it
		dskeys = dskeys[:0]
		dsdst = dsdst[:0]
		dixs = dixs[:0]
		// we only want to check the returned map if there weren't any errors
		// unlike the datastore, memcache will return a smaller map with no error if some of the keys were missed

		for i, m := range mckeys {
			d := v.Index(mixs[i]).Interface()
			if v.Index(mixs[i]).Kind() == reflect.Struct {
				d = v.Index(mixs[i]).Addr().Interface()
			}
			if s, present := memvalues[m]; present {
				// Mirror any memcache entries in local cache
				g.cache.Set(&cacheItem{key: m, value: s.Value})
				// Attempt to deserialize the cached value into the struct
				err := deserializeStruct(d, s.Value)
				if err != nil && (!IgnoreFieldMismatch || !errFieldMismatch(err)) {
					if err == datastore.ErrNoSuchEntity || errFieldMismatch(err) {
						anyErr = true // this flag tells GetMulti to return multiErr later
						multiErr[mixs[i]] = err
					} else {
						g.error(err)
						return err
					}
				}
			} else {
				dskeys = append(dskeys, keys[mixs[i]])
				dsdst = append(dsdst, d)
				dixs = append(dixs, mixs[i])
			}
		}
		if len(dskeys) == 0 {
			if anyErr {
				return realError(multiErr)
			}
			return nil
		}
	}

	mu := new(sync.Mutex)
	goroutines := (len(dskeys)-1)/datastoreGetMultiMaxItems + 1
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func(i int) {
			defer wg.Done()
			lo := i * datastoreGetMultiMaxItems
			hi := (i + 1) * datastoreGetMultiMaxItems
			if hi > len(dskeys) {
				hi = len(dskeys)
			}
			toCache := make([]*cacheItem, 0, hi-lo)
			propLists := make([]datastore.PropertyList, hi-lo)
			handleProp := func(i, idx int, exists bool) {
				// Serialize the properties
				data, err := serializeProperties(propLists[i], exists)
				if err != nil {
					g.error(err)
					multiErr[idx] = err
					return
				}
				// Prepare the properties for caching
				toCache = append(toCache, &cacheItem{key: lckeys[idx], value: data})
				// Deserialize the properties into a struct
				if exists {
					err = deserializeProperties(dsdst[lo+i], propLists[i])
					if err != nil && (!IgnoreFieldMismatch || !errFieldMismatch(err)) {
						multiErr[idx] = err
					}
				}
			}
			gmerr := datastore.GetMulti(g.Context, dskeys[lo:hi], propLists)
			if gmerr != nil {
				mu.Lock()
				anyErr = true // this flag tells GetMulti to return multiErr later
				mu.Unlock()
				merr, ok := gmerr.(appengine.MultiError)
				if !ok {
					g.error(gmerr)
					for j := lo; j < hi; j++ {
						multiErr[j] = gmerr
					}
					return
				}
				for i, idx := range dixs[lo:hi] {
					if merr[i] == nil {
						handleProp(i, idx, true)
					} else {
						if merr[i] == datastore.ErrNoSuchEntity {
							handleProp(i, idx, false)
						}
						multiErr[idx] = merr[i]
					}
				}
			} else {
				for i, idx := range dixs[lo:hi] {
					handleProp(i, idx, true)
				}
			}
			if len(toCache) > 0 {
				// Populate memcache in a goroutine because there's network involved
				// and we can be doing useful work while waiting for I/O
				errc := make(chan error)
				go func() {
					errc <- g.putMemcache(toCache)
				}()
				// Populate local cache
				g.cache.SetMulti(toCache)
				// Wait for memcache population to finish
				err := <-errc
				// .. but only propagate the memcache error if configured to do so
				if propagateMemcachePutError && err != nil {
					mu.Lock()
					extraErr = err
					mu.Unlock()
				}
			}
		}(i)
	}
	wg.Wait()
	if anyErr {
		return realError(multiErr)
	} else if extraErr != nil {
		return extraErr
	}
	return nil
}

// Delete deletes the provided entity.
// Takes either *S or *datastore.Key.
func (g *Goon) Delete(src interface{}) error {
	var srcs interface{}
	if key, ok := src.(*datastore.Key); ok {
		srcs = []*datastore.Key{key}
	} else {
		v := reflect.ValueOf(src)
		if v.Kind() != reflect.Ptr {
			return fmt.Errorf("goon: expected pointer to a struct, got %#v", src)
		}
		srcs = []interface{}{src}
	}
	err := g.DeleteMulti(srcs)
	if err != nil {
		// Look for an embedded error if it's multi
		if me, ok := err.(appengine.MultiError); ok {
			return me[0]
		}
	}
	return err
}

// DeleteMulti is a batch version of Delete.
// Takes either []*S or []*datastore.Key.
func (g *Goon) DeleteMulti(src interface{}) error {
	keys, ok := src.([]*datastore.Key)
	if !ok {
		var err error
		keys, err = g.extractKeys(src, false) // don't allow incomplete keys on a Delete request
		if err != nil {
			return err
		}
	}
	if len(keys) == 0 {
		return nil
		// not an error, and it was "successful", so return nil
	}

	mu := new(sync.Mutex)
	multiErr, any := make(appengine.MultiError, len(keys)), false
	goroutines := (len(keys)-1)/datastoreDeleteMultiMaxItems + 1
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func(i int) {
			defer wg.Done()
			lo := i * datastoreDeleteMultiMaxItems
			hi := (i + 1) * datastoreDeleteMultiMaxItems
			if hi > len(keys) {
				hi = len(keys)
			}
			dmerr := datastore.DeleteMulti(g.Context, keys[lo:hi])
			if dmerr != nil {
				mu.Lock()
				any = true // this flag tells DeleteMulti to return multiErr later
				mu.Unlock()
				merr, ok := dmerr.(appengine.MultiError)
				if !ok {
					g.error(dmerr)
					for j := lo; j < hi; j++ {
						multiErr[j] = dmerr
					}
					return
				}
				copy(multiErr[lo:hi], merr)
			}
		}(i)
	}
	wg.Wait()

	// Caches need to be updated after the datastore to prevent a common race condition,
	// where a concurrent request will fetch the not-yet-updated data from the datastore
	// and populate the caches with it.
	cachekeys := make([]string, 0, len(keys))
	for _, key := range keys {
		cachekeys = append(cachekeys, cacheKey(key))
	}
	if g.inTransaction {
		g.txnCacheLock.Lock()
		for _, ck := range cachekeys {
			g.toDelete[ck] = struct{}{}
			g.toDeleteMC[ck] = struct{}{}
		}
		g.txnCacheLock.Unlock()
	} else {
		g.cache.DeleteMulti(cachekeys)
		g.memcacheDeleteError(memcache.DeleteMulti(g.Context, cachekeys))
	}

	if any {
		return realError(multiErr)
	}
	return nil
}

// NotFound returns true if err is an appengine.MultiError and err[idx] is a datastore.ErrNoSuchEntity.
func NotFound(err error, idx int) bool {
	if merr, ok := err.(appengine.MultiError); ok {
		return idx < len(merr) && merr[idx] == datastore.ErrNoSuchEntity
	}
	return false
}

// errFieldMismatch returns true if err is *datastore.ErrFieldMismatch
func errFieldMismatch(err error) bool {
	_, ok := err.(*datastore.ErrFieldMismatch)
	return ok
}

// Returns a single error if each error in MultiError is the same
// otherwise, returns multiError or nil (if multiError is empty)
func realError(multiError appengine.MultiError) error {
	if len(multiError) == 0 {
		return nil
	}
	init := multiError[0]
	// some errors are *always* returned in MultiError form from the datastore
	if _, ok := init.(*datastore.ErrFieldMismatch); ok { // returned in GetMulti
		return multiError
	}
	if init == datastore.ErrInvalidEntityType || // returned in GetMulti
		init == datastore.ErrNoSuchEntity { // returned in GetMulti
		return multiError
	}
	// check if all errors are the same
	for i := 1; i < len(multiError); i++ {
		// since type error could hold structs, pointers, etc,
		// the only way to compare non-nil errors is by their string output
		if init == nil || multiError[i] == nil {
			if init != multiError[i] {
				return multiError
			}
		} else if init.Error() != multiError[i].Error() {
			return multiError
		}
	}
	// datastore.ErrInvalidKey is returned as a single error in PutMulti
	return init
}
