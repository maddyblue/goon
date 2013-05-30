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
	"appengine/datastore"
	"appengine/memcache"
	"github.com/icub3d/appenginetesting"
	"testing"
)

func TestMain(t *testing.T) {
	c, err := appenginetesting.NewContext(&appenginetesting.Options{Debug: "debug"})
	//c, err := appenginetesting.NewContext(nil)
	if err != nil {
		t.Fatalf("Could not create testing context")
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
		keyTest{
			HasId{Id: 1},
			datastore.NewKey(c, "HasId", "", 1, nil),
		},
		keyTest{
			HasKind{Id: 1, Kind: "OtherKind"},
			datastore.NewKey(c, "OtherKind", "", 1, nil),
		},
		keyTest{
			HasDefaultKind{Id: 1, Kind: "OtherKind"},
			datastore.NewKey(c, "OtherKind", "", 1, nil),
		},
		keyTest{
			HasDefaultKind{Id: 1},
			datastore.NewKey(c, "DefaultKind", "", 1, nil),
		},
		keyTest{
			HasKey{Key: datastore.NewKey(c, "HasKey", "", 0, nil)},
			datastore.NewKey(c, "HasKey", "", 0, nil),
		},
		keyTest{
			HasString{Id: "new"},
			datastore.NewKey(c, "HasString", "new", 0, nil),
		},
	}

	for _, kt := range keyTests {
		if k, err := n.KeyError(kt.obj); err != nil {
			t.Errorf(err.Error())
		} else if !k.Equal(kt.key) {
			t.Errorf("keys not equal - %v != %v", k, kt.key)
		}
	}

	// datastore tests
	keys, _ := datastore.NewQuery("HasId").KeysOnly().GetAll(c, nil)
	datastore.DeleteMulti(c, keys)
	memcache.Flush(c)
	if err := n.Get(&HasId{Id: 0}); err == nil {
		t.Errorf("ds: expected error")
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
	nes := []*HasId{
		{Id: 1},
		{Id: 2},
	}
	if err := n.GetMulti(es); err == nil {
		t.Errorf("ds: expected error")
	}
	if err := n.PutMulti(es); err != nil {
		t.Errorf("put: unexpected error")
	}
	if err := n.GetMulti(es); err != nil {
		t.Errorf("ds: unexpected error")
	}
	if err := n.GetMulti(nes); err != nil {
		t.Errorf("put: unexpected error")
	} else if es[0] != nes[0] || es[1] != nes[1] {
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
	// force partial fetch from memcache and then datastore
	memcache.Flush(c)
	if err := n.Get(nes[0]); err != nil {
		t.Errorf("get: unexpected error")
	}
	if err := n.GetMulti(nes); err != nil {
		t.Errorf("get: unexpected error")
	}
	hkp := &HasKey{}
	if err := n.Put(hkp); err != nil {
		t.Errorf("put: unexpected error - %v", err)
	}

	hkm := []HasKey{
		{Name: "one"},
		{Name: "two", Parent: hkp.Key},
	}
	err = n.PutMulti(hkm)
	if err != nil {
		t.Errorf("putmulti: unexpected error")
	}
	query := datastore.NewQuery("HasKey").Ancestor(hkp.Key).Filter("Name =", "two")
	var hks []HasKey
	_, err = n.GetAll(query, &hks)
	if err != nil {
		t.Errorf("getmulti: unexpected error - %v", err)
	}
	if len(hks) <= 0 || hks[0].Name != "two" {
		t.Errorf("getmulti: could not fetch resource - fetched %#v", hks[0])
	}

	hk := &HasKey{Name: "haskey"}
	if err := n.Put(hk); err != nil {
		t.Errorf("put: unexpected error - %v", err)
	}
	if hk.Key == nil {
		t.Errorf("key should not be nil")
	} else if hk.Key.Incomplete() {
		t.Errorf("key should no longer be incomplete")
	}

	hk2 := &HasKey{Key: hk.Key}
	if err := n.Get(hk2); err != nil {
		t.Errorf("get: unexpected error - %v", err)
	}
	if hk2.Name != hk.Name {
		t.Errorf("Could not fetch HasKey object from memory - %#v != %#v", hk, hk2)
	}

	hk3 := &HasKey{Key: hk.Key}
	delete(n.cache, memkey(hk3.Key))
	if err := n.Get(hk3); err != nil {
		t.Errorf("get: unexpected error - %v", err)
	}
	if hk3.Name != hk.Name {
		t.Errorf("Could not fetch HasKey object from memcache- %#v != %#v", hk, hk3)
	}

	hk4 := &HasKey{Key: hk.Key}
	delete(n.cache, memkey(hk4.Key))
	if memcache.Flush(n.context) != nil {
		t.Errorf("Unable to flush memcache")
	}
	if err := n.Get(hk4); err != nil {
		t.Errorf("get: unexpected error - %v", err)
	}
	if hk4.Name != hk.Name {
		t.Errorf("Could not fetch HasKey object from datastore- %#v != %#v", hk, hk4)
	}

	// put a HasId resource, then test pulling it from memory, memcache, and datastore
	hi := &HasId{Name: "hasid"}
	if err := n.Put(hi); err != nil {
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
		t.Errorf("Could not fetch HasKey object from memory - %#v != %#v", hi, hi3)
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
		t.Errorf("Could not fetch HasKey object from datastore- %#v != %#v", hk, hi4)
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

type HasKey struct {
	Key    *datastore.Key `datastore:"-" goon:"self"`
	Parent *datastore.Key `datastore:"-" goon:"parent"`
	Name   string
}

type HasString struct {
	Id string `datastore:"-" goon:"id"`
}

type HasDefaultKind struct {
	Id   int64  `datastore:"-" goon:"id"`
	Kind string `datastore:"-" goon:"kind,DefaultKind"`
	Name string
}
