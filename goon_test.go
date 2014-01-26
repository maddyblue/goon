/*
 * Copyright (c) 2013 Matt Jibson <matt.jibson@gmail.com>
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
	"reflect"
	"testing"
	"time"

	"appengine/aetest"
	"appengine/datastore"
	"appengine/memcache"
)

func TestGoon(t *testing.T) {
	c, err := aetest.NewContext(nil)
	if err != nil {
		t.Fatalf("Could not start aetest - %v", err)
	}
	defer c.Close()
	n := FromContext(c)

	// Don't want any of these tests to hit the timeout threshold on the devapp server
	MemcacheGetTimeout = time.Second
	MemcachePutTimeoutLarge = time.Second
	MemcachePutTimeoutSmall = time.Second

	// key tests
	noid := NoId{}
	if k, err := n.KeyError(noid); err == nil && !k.Incomplete() {
		t.Error("expected incomplete on noid")
	}
	if n.Key(noid) == nil {
		t.Error("expected to find a key")
	}

	var keyTests = []keyTest{
		{
			HasDefaultKind{},
			datastore.NewKey(c, "DefaultKind", "", 0, nil),
		},
		{
			HasId{Id: 1},
			datastore.NewKey(c, "HasId", "", 1, nil),
		},
		{
			HasKind{Id: 1, Kind: "OtherKind"},
			datastore.NewKey(c, "OtherKind", "", 1, nil),
		},

		{
			HasDefaultKind{Id: 1, Kind: "OtherKind"},
			datastore.NewKey(c, "OtherKind", "", 1, nil),
		},
		{
			HasDefaultKind{Id: 1},
			datastore.NewKey(c, "DefaultKind", "", 1, nil),
		},
		{
			HasString{Id: "new"},
			datastore.NewKey(c, "HasString", "new", 0, nil),
		},
	}

	for _, kt := range keyTests {
		if k, err := n.KeyError(kt.obj); err != nil {
			t.Errorf("error:", err.Error())
		} else if !k.Equal(kt.key) {
			t.Errorf("keys not equal")
		}
	}

	if _, err := n.KeyError(TwoId{IntId: 1, StringId: "1"}); err == nil {
		t.Errorf("expected key error")
	}

	// datastore tests
	keys, _ := datastore.NewQuery("HasId").KeysOnly().GetAll(c, nil)
	datastore.DeleteMulti(c, keys)
	memcache.Flush(c)
	if err := n.Get(&HasId{Id: 0}); err == nil {
		t.Errorf("ds: expected error, we're fetching from the datastore on an incomplete key!")
	}
	if err := n.Get(&HasId{Id: 1}); err != datastore.ErrNoSuchEntity {
		t.Errorf("ds: expected no such entity")
	}
	// run twice to make sure autocaching works correctly
	if err := n.Get(&HasId{Id: 1}); err != datastore.ErrNoSuchEntity {
		t.Errorf("ds: expected no such entity")
	}
	es := []*HasId{
		{Id: 1, Name: "one"},
		{Id: 2, Name: "two"},
	}
	var esk []*datastore.Key
	for _, e := range es {
		esk = append(esk, n.Key(e))
	}
	nes := []*HasId{
		{Id: 1},
		{Id: 2},
	}
	if err := n.GetMulti(es); err == nil {
		t.Errorf("ds: expected error")
	} else if !NotFound(err, 0) {
		t.Errorf("ds: not found error 0")
	} else if !NotFound(err, 1) {
		t.Errorf("ds: not found error 1")
	} else if NotFound(err, 2) {
		t.Errorf("ds: not found error 2")
	}

	if keys, err := n.PutMulti(es); err != nil {
		t.Errorf("put: unexpected error")
	} else if len(keys) != len(esk) {
		t.Errorf("put: got unexpected number of keys")
	} else {
		for i, k := range keys {
			if !k.Equal(esk[i]) {
				t.Errorf("put: got unexpected keys")
			}
		}
	}
	if err := n.GetMulti(nes); err != nil {
		t.Errorf("put: unexpected error")
	} else if *es[0] != *nes[0] || *es[1] != *nes[1] {
		t.Errorf("put: bad results")
	} else {
		nesk0 := n.Key(nes[0])
		if !nesk0.Equal(datastore.NewKey(c, "HasId", "", 1, nil)) {
			t.Errorf("put: bad key")
		}
		nesk1 := n.Key(nes[1])
		if !nesk1.Equal(datastore.NewKey(c, "HasId", "", 2, nil)) {
			t.Errorf("put: bad key")
		}
	}
	if _, err := n.Put(HasId{Id: 3}); err == nil {
		t.Errorf("put: expected error")
	}
	// force partial fetch from memcache and then datastore
	memcache.Flush(c)
	if err := n.Get(nes[0]); err != nil {
		t.Errorf("get: unexpected error")
	}
	if err := n.GetMulti(nes); err != nil {
		t.Errorf("get: unexpected error")
	}

	// put a HasId resource, then test pulling it from memory, memcache, and datastore
	hi := &HasId{Name: "hasid"} // no id given, should be automatically created by the datastore
	if _, err := n.Put(hi); err != nil {
		t.Errorf("put: unexpected error - %v", err)
	}
	if n.Key(hi) == nil {
		t.Errorf("key should not be nil")
	} else if n.Key(hi).Incomplete() {
		t.Errorf("key should not be incomplete")
	}

	hi2 := &HasId{Id: hi.Id}
	if err := n.Get(hi2); err != nil {
		t.Errorf("get: unexpected error - %v", err)
	}
	if hi2.Name != hi.Name {
		t.Errorf("Could not fetch HasId object from memory - %#v != %#v, memory=%#v", hi, hi2, n.cache[memkey(n.Key(hi2))])
	}

	hi3 := &HasId{Id: hi.Id}
	delete(n.cache, memkey(n.Key(hi)))
	if err := n.Get(hi3); err != nil {
		t.Errorf("get: unexpected error - %v", err)
	}
	if hi3.Name != hi.Name {
		t.Errorf("Could not fetch HasId object from memory - %#v != %#v", hi, hi3)
	}

	hi4 := &HasId{Id: hi.Id}
	delete(n.cache, memkey(n.Key(hi4)))
	if memcache.Flush(n.context) != nil {
		t.Errorf("Unable to flush memcache")
	}
	if err := n.Get(hi4); err != nil {
		t.Errorf("get: unexpected error - %v", err)
	}
	if hi4.Name != hi.Name {
		t.Errorf("Could not fetch HasId object from datastore- %#v != %#v", hi, hi4)
	}

	// Now do the opposite also using hi
	// Test pulling from local cache and memcache when datastore result is different
	// Note that this shouldn't happen with real goon usage,
	//   but this tests that goon isn't still pulling from the datastore (or memcache) unnecessarily
	// hi in datastore Name = hasid
	hiPull := &HasId{Id: hi.Id}
	n.cacheLock.Lock()
	n.cache[memkey(n.Key(hi))].(*HasId).Name = "changedincache"
	n.cacheLock.Unlock()
	if err := n.Get(hiPull); err != nil {
		t.Errorf("get: unexpected error - %v", err)
	}
	if hiPull.Name != "changedincache" {
		t.Errorf("hiPull.Name should be 'changedincache' but got %s", hiPull.Name)
	}

	hiPush := &HasId{Id: hi.Id, Name: "changedinmemcache"}
	n.putMemcache([]interface{}{hiPush})
	n.cacheLock.Lock()
	delete(n.cache, memkey(n.Key(hi)))
	n.cacheLock.Unlock()

	hiPull = &HasId{Id: hi.Id}
	if err := n.Get(hiPull); err != nil {
		t.Errorf("get: unexpected error - %v", err)
	}
	if hiPull.Name != "changedinmemcache" {
		t.Errorf("hiPull.Name should be 'changedinmemcache' but got %s", hiPull.Name)
	}

	// Since the datastore can't assign a key to a String ID, test to make sure goon stops it from happening
	hasString := new(HasString)
	_, err = n.Put(hasString)
	if err == nil {
		t.Errorf("Cannot put an incomplete string Id object as the datastore will populate an int64 id instead- %v", hasString)
	}
	hasString.Id = "hello"
	_, err = n.Put(hasString)
	if err != nil {
		t.Errorf("Error putting hasString object - %v", hasString)
	}

	// Test queries!

	// Test that zero result queries work properly
	qiZRes := []QueryItem{}
	if dskeys, err := n.GetAll(datastore.NewQuery("QueryItem"), &qiZRes); err != nil {
		t.Errorf("GetAll Zero: unexpected error: %v", err)
	} else if len(dskeys) != 0 {
		t.Errorf("GetAll Zero: expected 0 keys, got %v", len(dskeys))
	}

	// Create some entities that we will query for
	if _, err := n.PutMulti([]*QueryItem{{Id: 1, Data: "one"}, {Id: 2, Data: "two"}}); err != nil {
		t.Errorf("PutMulti: unexpected error: %v", err)
	}

	// Sleep a bit to wait for the HRD emulation to get out of our way
	time.Sleep(1000 * time.Millisecond)

	// Clear the local memory cache, because we want to test it being filled correctly by GetAll
	n.FlushLocalCache()

	// Get the entity using a slice of structs
	qiSRes := []QueryItem{}
	if dskeys, err := n.GetAll(datastore.NewQuery("QueryItem").Filter("data=", "one"), &qiSRes); err != nil {
		t.Errorf("GetAll SoS: unexpected error: %v", err)
	} else if len(dskeys) != 1 {
		t.Errorf("GetAll SoS: expected 1 key, got %v", len(dskeys))
	} else if dskeys[0].IntID() != 1 {
		t.Errorf("GetAll SoS: expected key IntID to be 1, got %v", dskeys[0].IntID())
	} else if len(qiSRes) != 1 {
		t.Errorf("GetAll SoS: expected 1 result, got %v", len(qiSRes))
	} else if qiSRes[0].Id != 1 {
		t.Errorf("GetAll SoS: expected entity id to be 1, got %v", qiSRes[0].Id)
	} else if qiSRes[0].Data != "one" {
		t.Errorf("GetAll SoS: expected entity data to be 'one', got '%v'", qiSRes[0].Data)
	}

	// Get the entity using normal Get to test local cache (provided the local cache actually got saved)
	qiS := &QueryItem{Id: 1}
	if err := n.Get(qiS); err != nil {
		t.Errorf("Get SoS: unexpected error: %v", err)
	} else if qiS.Id != 1 {
		t.Errorf("Get SoS: expected entity id to be 1, got %v", qiS.Id)
	} else if qiS.Data != "one" {
		t.Errorf("Get SoS: expected entity data to be 'one', got '%v'", qiS.Data)
	}

	// Clear the local memory cache, because we want to test it being filled correctly by GetAll
	n.FlushLocalCache()

	// Get the entity using a slice of pointers to struct
	qiPRes := []*QueryItem{}
	if dskeys, err := n.GetAll(datastore.NewQuery("QueryItem").Filter("data=", "one"), &qiPRes); err != nil {
		t.Errorf("GetAll SoPtS: unexpected error: %v", err)
	} else if len(dskeys) != 1 {
		t.Errorf("GetAll SoPtS: expected 1 key, got %v", len(dskeys))
	} else if dskeys[0].IntID() != 1 {
		t.Errorf("GetAll SoPtS: expected key IntID to be 1, got %v", dskeys[0].IntID())
	} else if len(qiPRes) != 1 {
		t.Errorf("GetAll SoPtS: expected 1 result, got %v", len(qiPRes))
	} else if qiPRes[0].Id != 1 {
		t.Errorf("GetAll SoPtS: expected entity id to be 1, got %v", qiPRes[0].Id)
	} else if qiPRes[0].Data != "one" {
		t.Errorf("GetAll SoPtS: expected entity data to be 'one', got '%v'", qiPRes[0].Data)
	}

	// Get the entity using normal Get to test local cache (provided the local cache actually got saved)
	qiP := &QueryItem{Id: 1}
	if err := n.Get(qiP); err != nil {
		t.Errorf("Get SoPtS: unexpected error: %v", err)
	} else if qiP.Id != 1 {
		t.Errorf("Get SoPtS: expected entity id to be 1, got %v", qiP.Id)
	} else if qiP.Data != "one" {
		t.Errorf("Get SoPtS: expected entity data to be 'one', got '%v'", qiP.Data)
	}

	// Clear the local memory cache, because we want to test it being filled correctly by Next
	n.FlushLocalCache()

	// Get the entity using an iterator
	qiIt := n.Run(datastore.NewQuery("QueryItem").Filter("data=", "one"))

	qiItRes := &QueryItem{}
	if dskey, err := qiIt.Next(qiItRes); err != nil {
		t.Errorf("Next: unexpected error: %v", err)
	} else if dskey.IntID() != 1 {
		t.Errorf("Next: expected key IntID to be 1, got %v", dskey.IntID())
	} else if qiItRes.Id != 1 {
		t.Errorf("Next: expected entity id to be 1, got %v", qiItRes.Id)
	} else if qiItRes.Data != "one" {
		t.Errorf("Next: expected entity data to be 'one', got '%v'", qiItRes.Data)
	}

	// Make sure the iterator ends correctly
	if _, err := qiIt.Next(&QueryItem{}); err != datastore.Done {
		t.Errorf("Next: expected iterator to end with the error datastore.Done, got %v", err)
	}

	// Get the entity using normal Get to test local cache (provided the local cache actually got saved)
	qiI := &QueryItem{Id: 1}
	if err := n.Get(qiI); err != nil {
		t.Errorf("Get Iterator: unexpected error: %v", err)
	} else if qiI.Id != 1 {
		t.Errorf("Get Iterator: expected entity id to be 1, got %v", qiI.Id)
	} else if qiI.Data != "one" {
		t.Errorf("Get Iterator: expected entity data to be 'one', got '%v'", qiI.Data)
	}

	// Clear the local memory cache, because we want to test it not being filled incorrectly when supplying a non-zero slice
	n.FlushLocalCache()

	// Get the entity using a non-zero slice of structs
	qiNZSRes := []QueryItem{{Id: 1, Data: "invalid cache"}}
	if dskeys, err := n.GetAll(datastore.NewQuery("QueryItem").Filter("data=", "two"), &qiNZSRes); err != nil {
		t.Errorf("GetAll NZSoS: unexpected error: %v", err)
	} else if len(dskeys) != 1 {
		t.Errorf("GetAll NZSoS: expected 1 key, got %v", len(dskeys))
	} else if dskeys[0].IntID() != 2 {
		t.Errorf("GetAll NZSoS: expected key IntID to be 2, got %v", dskeys[0].IntID())
	} else if len(qiNZSRes) != 2 {
		t.Errorf("GetAll NZSoS: expected slice len to be 2, got %v", len(qiNZSRes))
	} else if qiNZSRes[0].Id != 1 {
		t.Errorf("GetAll NZSoS: expected entity id to be 1, got %v", qiNZSRes[0].Id)
	} else if qiNZSRes[0].Data != "invalid cache" {
		t.Errorf("GetAll NZSoS: expected entity data to be 'invalid cache', got '%v'", qiNZSRes[0].Data)
	} else if qiNZSRes[1].Id != 2 {
		t.Errorf("GetAll NZSoS: expected entity id to be 2, got %v", qiNZSRes[1].Id)
	} else if qiNZSRes[1].Data != "two" {
		t.Errorf("GetAll NZSoS: expected entity data to be 'two', got '%v'", qiNZSRes[1].Data)
	}

	// Get the entities using normal GetMulti to test local cache
	qiNZSs := []*QueryItem{{Id: 1}, {Id: 2}} // TODO: Change this once GetMulti gets []S support
	if err := n.GetMulti(qiNZSs); err != nil {
		t.Errorf("GetMulti NZSoS: unexpected error: %v", err)
	} else if len(qiNZSs) != 2 {
		t.Errorf("GetMulti NZSoS: expected slice len to be 2, got %v", len(qiNZSs))
	} else if qiNZSs[0].Id != 1 {
		t.Errorf("GetMulti NZSoS: expected entity id to be 1, got %v", qiNZSs[0].Id)
	} else if qiNZSs[0].Data != "one" {
		t.Errorf("GetMulti NZSoS: expected entity data to be 'one', got '%v'", qiNZSs[0].Data)
	} else if qiNZSs[1].Id != 2 {
		t.Errorf("GetMulti NZSoS: expected entity id to be 2, got %v", qiNZSs[1].Id)
	} else if qiNZSs[1].Data != "two" {
		t.Errorf("GetMulti NZSoS: expected entity data to be 'two', got '%v'", qiNZSs[1].Data)
	}

	// Clear the local memory cache, because we want to test it not being filled incorrectly when supplying a non-zero slice
	n.FlushLocalCache()

	// Get the entity using a non-zero slice of pointers to struct
	qiNZPRes := []*QueryItem{{Id: 1, Data: "invalid cache"}}
	if dskeys, err := n.GetAll(datastore.NewQuery("QueryItem").Filter("data=", "two"), &qiNZPRes); err != nil {
		t.Errorf("GetAll NZSoPtS: unexpected error: %v", err)
	} else if len(dskeys) != 1 {
		t.Errorf("GetAll NZSoPtS: expected 1 key, got %v", len(dskeys))
	} else if dskeys[0].IntID() != 2 {
		t.Errorf("GetAll NZSoPtS: expected key IntID to be 2, got %v", dskeys[0].IntID())
	} else if len(qiNZPRes) != 2 {
		t.Errorf("GetAll NZSoPtS: expected slice len to be 2, got %v", len(qiNZPRes))
	} else if qiNZPRes[0].Id != 1 {
		t.Errorf("GetAll NZSoPtS: expected entity id to be 1, got %v", qiNZPRes[0].Id)
	} else if qiNZPRes[0].Data != "invalid cache" {
		t.Errorf("GetAll NZSoPtS: expected entity data to be 'invalid cache', got '%v'", qiNZPRes[0].Data)
	} else if qiNZPRes[1].Id != 2 {
		t.Errorf("GetAll NZSoPtS: expected entity id to be 2, got %v", qiNZPRes[1].Id)
	} else if qiNZPRes[1].Data != "two" {
		t.Errorf("GetAll NZSoPtS: expected entity data to be 'two', got '%v'", qiNZPRes[1].Data)
	}

	// Get the entities using normal GetMulti to test local cache
	qiNZPs := []*QueryItem{{Id: 1}, {Id: 2}}
	if err := n.GetMulti(qiNZPs); err != nil {
		t.Errorf("GetMulti NZSoPtS: unexpected error: %v", err)
	} else if len(qiNZPs) != 2 {
		t.Errorf("GetMulti NZSoPtS: expected slice len to be 2, got %v", len(qiNZPs))
	} else if qiNZPs[0].Id != 1 {
		t.Errorf("GetMulti NZSoPtS: expected entity id to be 1, got %v", qiNZPs[0].Id)
	} else if qiNZPs[0].Data != "one" {
		t.Errorf("GetMulti NZSoPtS: expected entity data to be 'one', got '%v'", qiNZPs[0].Data)
	} else if qiNZPs[1].Id != 2 {
		t.Errorf("GetMulti NZSoPtS: expected entity id to be 2, got %v", qiNZPs[1].Id)
	} else if qiNZPs[1].Data != "two" {
		t.Errorf("GetMulti NZSoPtS: expected entity data to be 'two', got '%v'", qiNZPs[1].Data)
	}

	// Clear the local memory cache, because we want to test it not being filled incorrectly by a keys-only query
	n.FlushLocalCache()

	// Test the simplest keys-only query
	if dskeys, err := n.GetAll(datastore.NewQuery("QueryItem").Filter("data=", "one").KeysOnly(), nil); err != nil {
		t.Errorf("GetAll KeysOnly: unexpected error: %v", err)
	} else if len(dskeys) != 1 {
		t.Errorf("GetAll KeysOnly: expected 1 key, got %v", len(dskeys))
	} else if dskeys[0].IntID() != 1 {
		t.Errorf("GetAll KeysOnly: expected key IntID to be 1, got %v", dskeys[0].IntID())
	}

	// Get the entity using normal Get to test that the local cache wasn't filled with incomplete data
	qiKO := &QueryItem{Id: 1}
	if err := n.Get(qiKO); err != nil {
		t.Errorf("Get KeysOnly: unexpected error: %v", err)
	} else if qiKO.Id != 1 {
		t.Errorf("Get KeysOnly: expected entity id to be 1, got %v", qiKO.Id)
	} else if qiKO.Data != "one" {
		t.Errorf("Get KeysOnly: expected entity data to be 'one', got '%v'", qiKO.Data)
	}

	// Clear the local memory cache, because we want to test it not being filled incorrectly by a keys-only query
	n.FlushLocalCache()

	// Test the keys-only query with slice of structs
	qiKOSRes := []QueryItem{}
	if dskeys, err := n.GetAll(datastore.NewQuery("QueryItem").Filter("data=", "one").KeysOnly(), &qiKOSRes); err != nil {
		t.Errorf("GetAll KeysOnly SoS: unexpected error: %v", err)
	} else if len(dskeys) != 1 {
		t.Errorf("GetAll KeysOnly SoS: expected 1 key, got %v", len(dskeys))
	} else if dskeys[0].IntID() != 1 {
		t.Errorf("GetAll KeysOnly SoS: expected key IntID to be 1, got %v", dskeys[0].IntID())
	} else if len(qiKOSRes) != 1 {
		t.Errorf("GetAll KeysOnly SoS: expected 1 result, got %v", len(qiKOSRes))
	} else if k := reflect.TypeOf(qiKOSRes[0]).Kind(); k != reflect.Struct {
		t.Errorf("GetAll KeysOnly SoS: expected struct, got %v", k)
	} else if qiKOSRes[0].Id != 1 {
		t.Errorf("GetAll KeysOnly SoS: expected entity id to be 1, got %v", qiKOSRes[0].Id)
	} else if qiKOSRes[0].Data != "" {
		t.Errorf("GetAll KeysOnly SoS: expected entity data to be empty, got '%v'", qiKOSRes[0].Data)
	}

	// Get the entity using normal Get to test that the local cache wasn't filled with incomplete data
	if err := n.Get(&qiKOSRes[0]); err != nil { // TODO: Change this to n.GetMulti(qiKOSRes) once GetMulti gets []S support
		t.Errorf("Get KeysOnly SoS: unexpected error: %v", err)
	} else if qiKOSRes[0].Id != 1 {
		t.Errorf("Get KeysOnly SoS: expected entity id to be 1, got %v", qiKOSRes[0].Id)
	} else if qiKOSRes[0].Data != "one" {
		t.Errorf("Get KeysOnly SoS: expected entity data to be 'one', got '%v'", qiKOSRes[0].Data)
	}

	// Clear the local memory cache, because we want to test it not being filled incorrectly by a keys-only query
	n.FlushLocalCache()

	// Test the keys-only query with slice of pointers to struct
	qiKOPRes := []*QueryItem{}
	if dskeys, err := n.GetAll(datastore.NewQuery("QueryItem").Filter("data=", "one").KeysOnly(), &qiKOPRes); err != nil {
		t.Errorf("GetAll KeysOnly SoPtS: unexpected error: %v", err)
	} else if len(dskeys) != 1 {
		t.Errorf("GetAll KeysOnly SoPtS: expected 1 key, got %v", len(dskeys))
	} else if dskeys[0].IntID() != 1 {
		t.Errorf("GetAll KeysOnly SoPtS: expected key IntID to be 1, got %v", dskeys[0].IntID())
	} else if len(qiKOPRes) != 1 {
		t.Errorf("GetAll KeysOnly SoPtS: expected 1 result, got %v", len(qiKOPRes))
	} else if k := reflect.TypeOf(qiKOPRes[0]).Kind(); k != reflect.Ptr {
		t.Errorf("GetAll KeysOnly SoPtS: expected pointer, got %v", k)
	} else if qiKOPRes[0].Id != 1 {
		t.Errorf("GetAll KeysOnly SoPtS: expected entity id to be 1, got %v", qiKOPRes[0].Id)
	} else if qiKOPRes[0].Data != "" {
		t.Errorf("GetAll KeysOnly SoPtS: expected entity data to be empty, got '%v'", qiKOPRes[0].Data)
	}

	// Get the entity using normal Get to test that the local cache wasn't filled with incomplete data
	if err := n.GetMulti(qiKOPRes); err != nil {
		t.Errorf("Get KeysOnly SoPtS: unexpected error: %v", err)
	} else if qiKOPRes[0].Id != 1 {
		t.Errorf("Get KeysOnly SoPtS: expected entity id to be 1, got %v", qiKOPRes[0].Id)
	} else if qiKOPRes[0].Data != "one" {
		t.Errorf("Get KeysOnly SoPtS: expected entity data to be 'one', got '%v'", qiKOPRes[0].Data)
	}

	// Clear the local memory cache, because we want to test it not being filled incorrectly when supplying a non-zero slice
	n.FlushLocalCache()

	// Test the keys-only query with non-zero slice of structs
	qiKONZSRes := []QueryItem{{Id: 1, Data: "invalid cache"}}
	if dskeys, err := n.GetAll(datastore.NewQuery("QueryItem").Filter("data=", "two").KeysOnly(), &qiKONZSRes); err != nil {
		t.Errorf("GetAll KeysOnly NZSoS: unexpected error: %v", err)
	} else if len(dskeys) != 1 {
		t.Errorf("GetAll KeysOnly NZSoS: expected 1 key, got %v", len(dskeys))
	} else if dskeys[0].IntID() != 2 {
		t.Errorf("GetAll KeysOnly NZSoS: expected key IntID to be 2, got %v", dskeys[0].IntID())
	} else if len(qiKONZSRes) != 2 {
		t.Errorf("GetAll KeysOnly NZSoS: expected slice len to be 2, got %v", len(qiKONZSRes))
	} else if qiKONZSRes[0].Id != 1 {
		t.Errorf("GetAll KeysOnly NZSoS: expected entity id to be 1, got %v", qiKONZSRes[0].Id)
	} else if qiKONZSRes[0].Data != "invalid cache" {
		t.Errorf("GetAll KeysOnly NZSoS: expected entity data to be 'invalid cache', got '%v'", qiKONZSRes[0].Data)
	} else if k := reflect.TypeOf(qiKONZSRes[1]).Kind(); k != reflect.Struct {
		t.Errorf("GetAll KeysOnly NZSoS: expected struct, got %v", k)
	} else if qiKONZSRes[1].Id != 2 {
		t.Errorf("GetAll KeysOnly NZSoS: expected entity id to be 2, got %v", qiKONZSRes[1].Id)
	} else if qiKONZSRes[1].Data != "" {
		t.Errorf("GetAll KeysOnly NZSoS: expected entity data to be empty, got '%v'", qiKONZSRes[1].Data)
	}

	// Get the entities using normal GetMulti to test local cache
	qiKONZSs := []*QueryItem{{Id: 1}, {Id: 2}}
	if err := n.GetMulti(qiKONZSs); err != nil { // TODO: Change this to n.GetMulti(qiKONZSRes) once GetMulti gets []S support
		t.Errorf("GetMulti NZSoS: unexpected error: %v", err)
	} else if len(qiKONZSs) != 2 {
		t.Errorf("GetMulti NZSoS: expected slice len to be 2, got %v", len(qiKONZSs))
	} else if qiKONZSs[0].Id != 1 {
		t.Errorf("GetMulti NZSoS: expected entity id to be 1, got %v", qiKONZSs[0].Id)
	} else if qiKONZSs[0].Data != "one" {
		t.Errorf("GetMulti NZSoS: expected entity data to be 'one', got '%v'", qiKONZSs[0].Data)
	} else if qiKONZSs[1].Id != 2 {
		t.Errorf("GetMulti NZSoS: expected entity id to be 2, got %v", qiKONZSs[1].Id)
	} else if qiKONZSs[1].Data != "two" {
		t.Errorf("GetMulti NZSoS: expected entity data to be 'two', got '%v'", qiKONZSs[1].Data)
	}

	// Clear the local memory cache, because we want to test it not being filled incorrectly when supplying a non-zero slice
	n.FlushLocalCache()

	// Test the keys-only query with non-zero slice of pointers to struct
	qiKONZPRes := []*QueryItem{{Id: 1, Data: "invalid cache"}}
	if dskeys, err := n.GetAll(datastore.NewQuery("QueryItem").Filter("data=", "two").KeysOnly(), &qiKONZPRes); err != nil {
		t.Errorf("GetAll KeysOnly NZSoPtS: unexpected error: %v", err)
	} else if len(dskeys) != 1 {
		t.Errorf("GetAll KeysOnly NZSoPtS: expected 1 key, got %v", len(dskeys))
	} else if dskeys[0].IntID() != 2 {
		t.Errorf("GetAll KeysOnly NZSoPtS: expected key IntID to be 2, got %v", dskeys[0].IntID())
	} else if len(qiKONZPRes) != 2 {
		t.Errorf("GetAll KeysOnly NZSoPtS: expected slice len to be 2, got %v", len(qiKONZPRes))
	} else if qiKONZPRes[0].Id != 1 {
		t.Errorf("GetAll KeysOnly NZSoPtS: expected entity id to be 1, got %v", qiKONZPRes[0].Id)
	} else if qiKONZPRes[0].Data != "invalid cache" {
		t.Errorf("GetAll KeysOnly NZSoPtS: expected entity data to be 'invalid cache', got '%v'", qiKONZPRes[0].Data)
	} else if k := reflect.TypeOf(qiKONZPRes[1]).Kind(); k != reflect.Ptr {
		t.Errorf("GetAll KeysOnly NZSoPtS: expected pointer, got %v", k)
	} else if qiKONZPRes[1].Id != 2 {
		t.Errorf("GetAll KeysOnly NZSoPtS: expected entity id to be 2, got %v", qiKONZPRes[1].Id)
	} else if qiKONZPRes[1].Data != "" {
		t.Errorf("GetAll KeysOnly NZSoPtS: expected entity data to be empty, got '%v'", qiKONZPRes[1].Data)
	}

	// Get the entities using normal GetMulti to test local cache
	if err := n.GetMulti(qiKONZPRes); err != nil {
		t.Errorf("GetMulti NZSoPtS: unexpected error: %v", err)
	} else if len(qiKONZPRes) != 2 {
		t.Errorf("GetMulti NZSoPtS: expected slice len to be 2, got %v", len(qiKONZPRes))
	} else if qiKONZPRes[0].Id != 1 {
		t.Errorf("GetMulti NZSoPtS: expected entity id to be 1, got %v", qiKONZPRes[0].Id)
	} else if qiKONZPRes[0].Data != "one" {
		t.Errorf("GetMulti NZSoPtS: expected entity data to be 'one', got '%v'", qiKONZPRes[0].Data)
	} else if qiKONZPRes[1].Id != 2 {
		t.Errorf("GetMulti NZSoPtS: expected entity id to be 2, got %v", qiKONZPRes[1].Id)
	} else if qiKONZPRes[1].Data != "two" {
		t.Errorf("GetMulti NZSoPtS: expected entity data to be 'two', got '%v'", qiKONZPRes[1].Data)
	}
}

type keyTest struct {
	obj interface{}
	key *datastore.Key
}

type NoId struct {
}

type HasId struct {
	Id   int64 `datastore:"-" goon:"id"`
	Name string
}

type HasKind struct {
	Id   int64  `datastore:"-" goon:"id"`
	Kind string `datastore:"-" goon:"kind"`
	Name string
}

type HasDefaultKind struct {
	Id   int64  `datastore:"-" goon:"id"`
	Kind string `datastore:"-" goon:"kind,DefaultKind"`
	Name string
}

type QueryItem struct {
	Id   int64  `datastore:"-" goon:"id"`
	Data string `datastore:"data"`
}

type HasString struct {
	Id string `datastore:"-" goon:"id"`
}

type TwoId struct {
	IntId    int64  `goon:"id"`
	StringId string `goon:"id"`
}

type PutGet struct {
	ID    int64 `datastore:"-" goon:"id"`
	Value int32
}

// Commenting out for issue https://code.google.com/p/googleappengine/issues/detail?id=10493
//func TestMemcachePutTimeout(t *testing.T) {
//	c, err := aetest.NewContext(nil)
//	if err != nil {
//		t.Fatalf("Could not start aetest - %v", err)
//	}
//	defer c.Close()
//	g := FromContext(c)
//	MemcachePutTimeoutSmall = time.Second
//	// put a HasId resource, then test pulling it from memory, memcache, and datastore
//	hi := &HasId{Name: "hasid"} // no id given, should be automatically created by the datastore
//	if _, err := g.Put(hi); err != nil {
//		t.Errorf("put: unexpected error - %v", err)
//	}

//	MemcachePutTimeoutSmall = 0
//	MemcacheGetTimeout = 0
//	if err := g.putMemcache([]interface{}{hi}); !appengine.IsTimeoutError(err) {
//		t.Errorf("Request should timeout - err = %v", err)
//	}
//	MemcachePutTimeoutSmall = time.Second
//	MemcachePutTimeoutThreshold = 0
//	MemcachePutTimeoutLarge = 0
//	if err := g.putMemcache([]interface{}{hi}); !appengine.IsTimeoutError(err) {
//		t.Errorf("Request should timeout - err = %v", err)
//	}

//	MemcachePutTimeoutLarge = time.Second
//	if err := g.putMemcache([]interface{}{hi}); err != nil {
//		t.Errorf("putMemcache: unexpected error - %v", err)
//	}

//	g.FlushLocalCache()
//	memcache.Flush(c)
//	// time out Get
//	MemcacheGetTimeout = 0
//	// time out Put too
//	MemcachePutTimeoutSmall = 0
//	MemcachePutTimeoutThreshold = 0
//	MemcachePutTimeoutLarge = 0
//	hiResult := &HasId{Id: hi.Id}
//	if err := g.Get(hiResult); err != nil {
//		t.Errorf("Request should not timeout cause we'll fetch from the datastore but got error  %v", err)
//		// Put timing out should also error, but it won't be returned here, just logged
//	}
//	if !reflect.DeepEqual(hi, hiResult) {
//		t.Errorf("Fetched object isn't accurate - want %v, fetched %v", hi, hiResult)
//	}

//	hiResult = &HasId{Id: hi.Id}
//	g.FlushLocalCache()
//	MemcacheGetTimeout = time.Second
//	if err := g.Get(hiResult); err != nil {
//		t.Errorf("Request should not timeout cause we'll fetch from memcache successfully but got error %v", err)
//	}
//	if !reflect.DeepEqual(hi, hiResult) {
//		t.Errorf("Fetched object isn't accurate - want %v, fetched %v", hi, hiResult)
//	}
//}

// This test won't fail but if run with -race flag, it will show known race conditions
// Using multiple goroutines per http request is recommended here:
// http://talks.golang.org/2013/highperf.slide#22
func TestRace(t *testing.T) {
	c, err := aetest.NewContext(nil)
	if err != nil {
		t.Fatalf("Could not start aetest - %v", err)
	}
	defer c.Close()
	g := FromContext(c)

	hasid := &HasId{Id: 1, Name: "Race"}
	_, err = g.Put(hasid)
	if err != nil {
		t.Fatalf("Could not put Race entity - %v", err)
	}
	for x := 0; x < 5; x++ {
		go func() {
			g.Get(hasid)
		}()
	}
}

func TestPutGet(t *testing.T) {
	c, err := aetest.NewContext(nil)
	if err != nil {
		t.Fatalf("Could not start aetest - %v", err)
	}
	defer c.Close()
	g := FromContext(c)

	key, err := g.Put(&PutGet{ID: 12, Value: 15})
	if err != nil {
		t.Fatal(err.Error())
	}
	if key.IntID() != 12 {
		t.Fatal("ID should be 12 but is", key.IntID())
	}

	// Datastore Get
	dsPutGet := &PutGet{}
	err = datastore.Get(c,
		datastore.NewKey(c, "PutGet", "", 12, nil), dsPutGet)
	if err != nil {
		t.Fatal(err.Error())
	}
	if dsPutGet.Value != 15 {
		t.Fatal("dsPutGet.Value should be 15 but is",
			dsPutGet.Value)
	}

	// Goon Get
	goonPutGet := &PutGet{ID: 12}
	err = g.Get(goonPutGet)
	if err != nil {
		t.Fatal(err.Error())
	}
	if goonPutGet.ID != 12 {
		t.Fatal("goonPutGet.ID should be 12 but is", goonPutGet.ID)
	}
	if goonPutGet.Value != 15 {
		t.Fatal("goonPutGet.Value should be 15 but is",
			goonPutGet.Value)
	}
}
