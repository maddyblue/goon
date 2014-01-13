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
		keyTest{
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
			g.MemcacheTimeout = 0
		case "datastore":
			g.MemcacheTimeout = time.Millisecond * 2
			// can't timeout before memcache as memcache doesn't have a chance to return
			// but the datastore request needs to actually timeout... These times work on my machine consistently
			// I wasn't sure of a way to write this test any better - @mzimmerman
			g.DatastoreTimeout = time.Millisecond * 2
		case "both":
			g.MemcacheTimeout = 0
			g.DatastoreTimeout = 0
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
		g.cacheLock.Lock()
		g.cache = make(map[string]interface{})
		g.cacheLock.Unlock()

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
			"datastore": []*HasId{
				&HasId{Id: 1, Name: "1"},
				&HasId{Id: 2, Name: "2"},
				&HasId{Id: 3, Name: ""},
			},
			"both": []*HasId{
				&HasId{Id: 1, Name: "1"},
				&HasId{Id: 2, Name: ""},
				&HasId{Id: 3, Name: ""},
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
		case timeout == "datastore":
			if cacheGetErr != nil || memcacheGetErr != nil || datastoreGetErr == nil {
				t.Errorf("Datastore should timeout - cache(%v), memcache(%v), datastore(%v)", cacheGetErr, memcacheGetErr, datastoreGetErr)
			}
			fetchedObjects = memcacheObjects // since the datastore timed out, only memcache objects should exist
		case timeout == "both":
			if cacheGetErr != nil || memcacheGetErr == nil || datastoreGetErr == nil {
				t.Errorf("Memcache && Datastore should timeout - cache(%v), memcache(%v), datastore(%v)", cacheGetErr, memcacheGetErr, datastoreGetErr)
			}
			fetchedObjects = cacheObjects // datastore timed out, but there should be no errors fetching only these results
		}
		for i := range fetchedObjects {
			if !reflect.DeepEqual(resultObjects[timeout][i], fetchedObjects[i]) {
				t.Errorf("%v does not equal %v", resultObjects[timeout][i], fetchedObjects[i])
			}
		}
	}
}

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
