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

	"appengine"
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

	// key tests
	noid := NoId{}
	if k, err := n.KeyError(noid); err == nil && !k.Incomplete() {
		t.Error("expected incomplete on noid")
	}
	if n.Key(noid) != nil {
		t.Error("expected to not find a key")
	}

	var keyTests = []keyTest{
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

	if _, err := n.PutComplete(&HasId{}); err == nil {
		t.Errorf("put complete: expected error")
	}
	if _, err := n.PutComplete(&HasId{Id: 1}); err != nil {
		t.Errorf("put complete: unexpected error")
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

	// Since the datastore can't assign a key to a String ID, test to make sure goon stops it from happening
	hasString := new(HasString)
	_, err = n.PutComplete(hasString)
	if err == nil {
		t.Errorf("Cannot put an incomplete object using PutComplete - %v", hasString)
	}
	_, err = n.Put(hasString)
	if err == nil {
		t.Errorf("Cannot put an incomplete string Id object as the datastore will populate an int64 id instead- %v", hasString)
	}
	hasString.Id = "hello"
	_, err = n.PutComplete(hasString)
	if err != nil {
		t.Errorf("Error putting hasString object - %v", hasString)
	}
	_, err = n.Put(hasString)
	if err != nil {
		t.Errorf("Error putting hasString object - %v", hasString)
	}

	// Test queries!

	// Create an entity that we will query for
	if _, err := n.Put(&QueryItem{Id: 1, Data: "foo"}); err != nil {
		t.Errorf("Put: unexpected error: %v", err.Error())
	}

	// Sleep a bit to wait for the HRD emulation to get out of our way
	time.Sleep(1000 * time.Millisecond)

	// Clear the local memory cache, because we want to test it being filled correctly by GetAll
	n.FlushLocalCache()

	// Get the entity using a slice of structs
	qiSRes := []QueryItem{}
	if dskeys, err := n.GetAll(datastore.NewQuery("QueryItem"), &qiSRes); err != nil {
		t.Errorf("GetAll SoS: unexpected error: %v", err.Error())
	} else if len(dskeys) != 1 {
		t.Errorf("GetAll SoS: expected 1 key, got %v", len(dskeys))
	} else if dskeys[0].IntID() != 1 {
		t.Errorf("GetAll SoS: expected key IntID to be 1, got %v", dskeys[0].IntID())
	} else if qiSRes[0].Id != 1 {
		t.Errorf("GetAll SoS: expected entity id to be 1, got %v", qiSRes[0].Id)
	} else if qiSRes[0].Data != "foo" {
		t.Errorf("GetAll SoS: expected entity data to be 'foo', got '%v'", qiSRes[0].Data)
	}

	// Get the entity using normal Get to test local cache (provided the local cache actually got saved)
	qiS := &QueryItem{Id: 1}
	if err := n.Get(qiS); err != nil {
		t.Errorf("Get SoS: unexpected error: %v", err.Error())
	} else if qiS.Id != 1 {
		t.Errorf("Get SoS: expected entity id to be 1, got %v", qiS.Id)
	} else if qiS.Data != "foo" {
		t.Errorf("Get SoS: expected entity data to be 'foo', got '%v'", qiS.Data)
	}

	// Clear the local memory cache, because we want to test it being filled correctly by GetAll
	n.FlushLocalCache()

	// Get the entity using a slice of pointers to struct
	qiPRes := []*QueryItem{}
	if dskeys, err := n.GetAll(datastore.NewQuery("QueryItem"), &qiPRes); err != nil {
		t.Errorf("GetAll SoPtS: unexpected error: %v", err.Error())
	} else if len(dskeys) != 1 {
		t.Errorf("GetAll SoPtS: expected 1 key, got %v", len(dskeys))
	} else if dskeys[0].IntID() != 1 {
		t.Errorf("GetAll SoPtS: expected key IntID to be 1, got %v", dskeys[0].IntID())
	} else if qiPRes[0].Id != 1 {
		t.Errorf("GetAll SoPtS: expected entity id to be 1, got %v", qiPRes[0].Id)
	} else if qiPRes[0].Data != "foo" {
		t.Errorf("GetAll SoPtS: expected entity data to be 'foo', got '%v'", qiPRes[0].Data)
	}

	// Get the entity using normal Get to test local cache (provided the local cache actually got saved)
	qiP := &QueryItem{Id: 1}
	if err := n.Get(qiP); err != nil {
		t.Errorf("Get SoPtS: unexpected error: %v", err.Error())
	} else if qiP.Id != 1 {
		t.Errorf("Get SoPtS: expected entity id to be 1, got %v", qiP.Id)
	} else if qiP.Data != "foo" {
		t.Errorf("Get SoPtS: expected entity data to be 'foo', got '%v'", qiP.Data)
	}

	// Clear the local memory cache, because we want to test it being filled correctly by Next
	n.FlushLocalCache()

	// Get the entity using an iterator
	qiIt := n.Run(datastore.NewQuery("QueryItem"))

	qiItRes := &QueryItem{}
	if dskey, err := qiIt.Next(qiItRes); err != nil {
		t.Errorf("Next: unexpected error: %v", err.Error())
	} else if dskey.IntID() != 1 {
		t.Errorf("Next: expected key IntID to be 1, got %v", dskey.IntID())
	} else if qiItRes.Id != 1 {
		t.Errorf("Next: expected entity id to be 1, got %v", qiItRes.Id)
	} else if qiItRes.Data != "foo" {
		t.Errorf("Next: expected entity data to be 'foo', got '%v'", qiItRes.Data)
	}

	// Make sure the iterator ends correctly
	if _, err := qiIt.Next(&QueryItem{}); err != datastore.Done {
		t.Errorf("Next: expected iterator to end with the error datastore.Done, got %v", err.Error())
	}

	// Get the entity using normal Get to test local cache (provided the local cache actually got saved)
	qiI := &QueryItem{Id: 1}
	if err := n.Get(qiI); err != nil {
		t.Errorf("Get Iterator: unexpected error: %v", err.Error())
	} else if qiI.Id != 1 {
		t.Errorf("Get Iterator: expected entity id to be 1, got %v", qiI.Id)
	} else if qiI.Data != "foo" {
		t.Errorf("Get Iterator: expected entity data to be 'foo', got '%v'", qiI.Data)
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
	Data string `datastore:"data,noindex"`
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

func TestVariance(t *testing.T) {
	c, err := aetest.NewContext(nil)
	if err != nil {
		t.Fatalf("Could not start aetest - %v", err)
	}
	defer c.Close()
	g := FromContext(c)
	timeouts := []string{"none", "memcache", "datastore", "both"} // what services will timeout
	for _, timeout := range timeouts {
		switch timeout {
		case "none":
			// do nothing, use defaults already set
		case "memcache":
			MemcacheGetTimeout = 0
			MemcachePutTimeoutLarge = 0
			MemcachePutTimeoutSmall = 0
		}
		objects := []*HasId{
			&HasId{Id: 1, Name: "1"}, // stored in cache, memcache, and datastore
			&HasId{Id: 2, Name: "2"}, // stored in memcache and datastore
			&HasId{Id: 3, Name: "3"}, // stored only in datastore
		}
		// flush all locations
		keys, _ := datastore.NewQuery("HasId").KeysOnly().GetAll(c, nil)
		datastore.DeleteMulti(c, keys)
		memcache.Flush(c)
		g.FlushLocalCache()

		// add test data to proper locations only
		for _, obj := range objects {
			key, _ := g.getStructKey(obj)
			if _, err := datastore.Put(g.context, key, obj); err != nil {
				t.Errorf("Error putting object %v", obj)
			}
			if obj.Id == 3 {
				continue
			}
			gob, err := toGob(obj)
			if err != nil {
				t.Errorf("Unable to gob object - %v", err)
			}
			item := &memcache.Item{
				Key:   memkey(key),
				Value: gob,
			}
			if err = memcache.Set(g.context, item); err != nil {
				t.Errorf("Error putting object in memcache %v", obj)
			}
			if obj.Id == 2 {
				continue
			}
			g.putMemory(obj)
		}

		datastoreObjects := []*HasId{
			&HasId{Id: 1, Name: ""}, // stored in cache, memcache, and datastore
			&HasId{Id: 2, Name: ""}, // stored in memcache and datastore
			&HasId{Id: 3, Name: ""}, // stored only in datastore
		}
		memcacheObjects := []*HasId{
			&HasId{Id: 1, Name: ""}, // stored in cache, memcache, and datastore
			&HasId{Id: 2, Name: ""}, // stored in memcache and datastore
		}
		cacheObjects := []*HasId{
			&HasId{Id: 1, Name: ""}, // stored in cache, memcache, and datastore
		}
		resultObjects := map[string][]*HasId{
			"none": []*HasId{
				&HasId{Id: 1, Name: "1"},
				&HasId{Id: 2, Name: "2"},
				&HasId{Id: 3, Name: "3"},
			},
			"memcache": []*HasId{
				&HasId{Id: 1, Name: "1"},
				&HasId{Id: 2, Name: "2"},
				&HasId{Id: 3, Name: "3"},
			},
		}
		cacheGetErr := g.GetMulti(&cacheObjects)
		memcacheGetErr := g.GetMulti(&memcacheObjects)
		datastoreGetErr := g.GetMulti(&datastoreObjects)
		var fetchedObjects []*HasId
		switch {
		case timeout == "none":
			if cacheGetErr != nil || memcacheGetErr != nil || datastoreGetErr != nil {
				t.Errorf("There are no timeouts, there should be no errors - cache(%v), memcache(%v), datastore(%v)", cacheGetErr, memcacheGetErr, datastoreGetErr)
			}
			fetchedObjects = datastoreObjects
		case timeout == "memcache":
			if cacheGetErr != nil || memcacheGetErr != nil || datastoreGetErr != nil {
				t.Errorf("Memcache timeout, but datastore will cover it - cache(%v), memcache(%v), datastore(%v)", cacheGetErr, memcacheGetErr, datastoreGetErr)
			}
			fetchedObjects = datastoreObjects // memcache may timeout, but all objects exist in the datastore
		}
		for i := range fetchedObjects {
			if !reflect.DeepEqual(resultObjects[timeout][i], fetchedObjects[i]) {
				t.Errorf("%v does not equal %v", resultObjects[timeout][i], fetchedObjects[i])
			}
		}
	}
}

// Commenting out for issue https://code.google.com/p/googleappengine/issues/detail?id=10493
//func TestMemcachePutTimeout(t *testing.T) {
//	c, err := aetest.NewContext(nil)
//	if err != nil {
//		t.Fatalf("Could not start aetest - %v", err)
//	}
//	defer c.Close()
//	g := FromContext(c)

//	// put a HasId resource, then test pulling it from memory, memcache, and datastore
//	hi := &HasId{Name: "hasid"} // no id given, should be automatically created by the datastore
//	if _, err := g.Put(hi); err != nil {
//		t.Errorf("put: unexpected error - %v", err)
//	}

//	MemcachePutTimeout = 0
//	if err := g.putMemcache([]interface{}{hi}); !appengine.IsTimeoutError(err) {
//		t.Errorf("Request should timeout - err = %v", err)
//	}

//	MemcachePutTimeout = time.Second
//	if err := g.putMemcache([]interface{}{hi}); err != nil {
//		t.Errorf("putMemcache: unexpected error - %v", err)
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

func TestMultis(t *testing.T) {
	c, err := aetest.NewContext(nil)
	if err != nil {
		t.Fatalf("Could not start aetest - %v", err)
	}
	defer c.Close()
	n := FromContext(c)

	testAmounts := []int{1, 999, 1000, 1001, 1999, 2000, 2001, 2510}
	for _, x := range testAmounts {
		memcache.Flush(c)
		objects := make([]*HasId, x)
		for y := 0; y < x; y++ {
			objects[y] = &HasId{Id: int64(y + 1)}
		}
		if _, err := n.PutMulti(objects); err != nil {
			t.Fatalf("Error in PutMulti for %d objects - %v", x, err)
		}
		n.FlushLocalCache() // Put just put them in the local cache, get rid of it before doing the Get
		if err := n.GetMulti(objects); err != nil {
			t.Fatalf("Error in GetMulti - %v", err)
		}
	}

	// do it again, but only write numbers divisible by 100
	for _, x := range testAmounts {
		memcache.Flush(c)
		getobjects := make([]*HasId, 0, x)
		putobjects := make([]*HasId, 0, x/100+1)
		keys := make([]*datastore.Key, x)
		for y := 0; y < x; y++ {
			keys[y] = datastore.NewKey(c, "HasId", "", int64(y+1), nil)
		}
		if err := n.DeleteMulti(keys); err != nil {
			t.Fatalf("Error deleting keys - %v", err)
		}
		for y := 0; y < x; y++ {
			getobjects = append(getobjects, &HasId{Id: int64(y + 1)})
			if y%100 == 0 {
				putobjects = append(putobjects, &HasId{Id: int64(y + 1)})
			}
		}

		_, err := n.PutMulti(putobjects)
		if err != nil {
			t.Fatalf("Error in PutMulti for %d objects - %v", x, err)
		}
		n.FlushLocalCache() // Put just put them in the local cache, get rid of it before doing the Get
		err = n.GetMulti(getobjects)
		if err == nil && x != 1 { // a test size of 1 has no objects divisible by 100, so there's no cache miss to return
			t.Errorf("Should be receiving a multiError on %d objects, but got no errors", x)
			continue
		}

		merr, ok := err.(appengine.MultiError)
		if ok {
			if len(merr) != len(getobjects) {
				t.Errorf("Should have received a MultiError object of length %d but got length %d instead", len(getobjects), len(merr))
			}
			for x := range merr {
				switch { // record good conditions, fail in other conditions
				case merr[x] == nil && x%100 == 0:
				case merr[x] != nil && x%100 != 0:
				default:
					t.Errorf("Found bad condition on object[%d] and error %v", x+1, merr[x])
				}
			}
		} else if x != 1 {
			t.Errorf("Did not return a multierror on fetch but when fetching %d objects, received - %v", x, merr)
		}
	}
}
