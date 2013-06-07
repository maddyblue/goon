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
	"reflect"
	"testing"
)

func TestMain(t *testing.T) {
	//c, err := appenginetesting.NewContext(&appenginetesting.Options{Debug: "debug"})
	c, err := appenginetesting.NewContext(nil)
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
	nes := []*HasId{
		{Id: 1},
		{Id: 2},
	}
	if err := n.GetMulti(es); err == nil {
		t.Errorf("ds: expected error")
	}
	if _, err := n.PutMulti(es); err != nil {
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
	if _, err := n.Put(hkp); err != nil {
		t.Errorf("put: unexpected error - %v", err)
	}

	hkm := []HasKey{
		{Name: "one", P: hkp.Key},
		{Name: "two", P: hkp.Key},
	}
	_, err = n.PutMulti(hkm)
	if err != nil {
		t.Errorf("putmulti: unexpected error")
	}
	query := datastore.NewQuery("HasKey").Ancestor(hkp.Key).Filter("Name =", "two")
	var hks []HasKey
	_, err = n.GetAll(query, &hks)
	if err != nil {
		t.Errorf("getmulti: unexpected error - %v", err)
	}
	if len(hks) != 1 || hks[0].Name != "two" {
		t.Errorf("getmulti: could not fetch resource - fetched %#v", hks[0])
	}

	query = datastore.NewQuery("HasKey").Ancestor(hkp.Key).Filter("Name =", "one")
	_, err = n.GetAll(query, &hks)
	if err != nil {
		t.Errorf("getmulti: unexpected error - %v", err)
	}
	if len(hks) != 2 {
		t.Errorf("getmulti: could not fetch additional resource - fetched %#v", hks)
	}

	hk := &HasKey{Name: "haskey", P: hkp.Key}
	if _, err := n.Put(hk); err != nil {
		t.Errorf("put: unexpected error - %v", err)
	}
	if hk.Key == nil {
		t.Errorf("key should not be nil")
	} else if hk.Key.Incomplete() {
		t.Errorf("key should no longer be incomplete")
	}

	n.C().Debugf("Clear the incache memory")
	n.cache = make(map[string]interface{})

	hk2 := &HasKey{Key: hk.Key}
	if err := n.Get(hk2); err != nil {
		t.Errorf("get: unexpected error - %v", err)
	}
	if hk2.Name != hk.Name {
		t.Errorf("Could not fetch HasKey object from memory - %#v != %#v", hk, hk2)
	}
	if !hk2.P.Equal(hkp.Key) {
		t.Errorf("Parent not loaded for %#v", hk2)
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

	dad := &HasParent{Name: "dad"}
	if _, err := n.Put(dad); err != nil {
		t.Errorf("dad not able to be stored")
	}

	son := &HasParent{Name: "son", P: n.Key(dad)}
	if _, err := n.Put(son); err != nil {
		t.Errorf("son not able to be stored")
	}

	sonCopy := &HasParent{Id: son.Id, P: son.P}
	if err := n.Get(sonCopy); err != nil {
		t.Errorf("son not able to be fetched - %v", err)
	}
	if sonCopy.Name != "son" {
		t.Errorf("Name not fetched for son")
	}
	if !sonCopy.P.Equal(n.Key(dad)) {
		t.Errorf("did not properly populate the Parent() key for son - %#v", sonCopy)
	}

	var sons []HasParent
	allSonsQuery := datastore.NewQuery("HasParent").Ancestor(n.Key(dad))
	if _, err := n.GetAll(allSonsQuery, &sons); err != nil {
		t.Errorf("sons not able to be fetched")
	}

	for _, child := range sons {
		if child.Name == "" {
			t.Errorf("did not properly fetch sons with GetAll")
		}
		if child.Name == "son" && !child.P.Equal(n.Key(dad)) {
			t.Errorf("did not properly populate the Parent() key for son - %#v", child)
		}
	}
	if len(sons) != 2 {
		t.Errorf("Should have two HasParent structs")
	}

	hasParentTest := &HasParent{}
	fakeParent := datastore.NewKey(c, "FakeParent", "", 1, nil)
	hasParentKey := datastore.NewKey(c, "HasParent", "", 2, fakeParent)
	setStructKey(hasParentTest, hasParentKey)
	if hasParentTest.Id != 2 {
		t.Errorf("setStructKey not setting stringid properly")
	}
	if hasParentTest.P != fakeParent {
		t.Errorf("setStructKey not setting parent properly")
	}
	hps := []HasParent{HasParent{}}
	setStructKey(&hps[0], hasParentKey)
	if hps[0].Id != 2 {
		t.Errorf("setStructKey not setting stringid properly when src is a slice of structs")
	}
	if hps[0].P != fakeParent {
		t.Errorf("setStructKey not setting parent properly when src is a slice of structs")
	}

	hs := HasString{Id: "hasstringid"}
	if err := n.Get(hs); err == nil {
		t.Errorf("Should have received an error because didn't pass a pointer to a struct")
	}
	father := &HasId{}
	fatherkey, err := n.Put(father)
	if err != nil {
		t.Fatalf("Could not put father")
	}
	if !fatherkey.Equal(father.Self(n)) {
		t.Fatalf("Father key not populated")
	}

	goontests := []GoonTest{
		GoonTest{&HasDefaultKind{Name: "bah"}, false},
		GoonTest{&HasId{Name: "bah"}, false},
		GoonTest{&HasKey{Name: "bah"}, false},
		GoonTest{&HasKey{Name: "bah", P: father.Self(n)}, false},
		GoonTest{&HasKind{Name: "bah", Kind: "other"}, false},
		GoonTest{&HasParent{Name: "bah"}, false},
		GoonTest{&HasParent{Name: "bah", P: father.Self(n)}, false},
		GoonTest{HasString{Id: "bah"}, true},
		GoonTest{&HasString{Name: "bah", Id: "hasstringid"}, false},
	}

	for _, gt := range goontests {
		key, err := n.Put(gt.orig)
		if (err == nil) && gt.putErr {
			t.Errorf("Put request for %#v should have errored but didn't", gt.orig)
			continue
		} else if err != nil {
			if !gt.putErr {
				t.Errorf("Put request had unexpected error %v for %#v", err, gt.orig)
			}
			// put error'd, nothing else to test
			continue
		}
		if !key.Equal(gt.orig.Self(n)) {
			t.Errorf("Key not equal for object %#v ***** %v != %v", gt.orig, key, gt.orig.Self(n))
			continue
		}
		if key.Parent() != nil && gt.orig.Self(n) != nil && !key.Parent().Equal(gt.orig.Parent(n)) {
			t.Errorf("Parent not equal for object %#v ***** %v != %v", gt.orig, key.Parent(), gt.orig.Parent(n))
		}
		for _, fetchType := range []string{"memory", "memcache", "datastore"} {
			switch fetchType {
			case "datastore":
				err = memcache.Delete(c, gt.orig.Self(n).Encode())
				if err != nil {
					t.Fatalf("Error clearing memcache for %#v", gt.orig)
				}
				fallthrough
			case "memcache":
				n.cache = make(map[string]interface{})
			}

			putDest := reflect.New(reflect.TypeOf(gt.orig).Elem()).Interface().(GoonStore)
			setStructKey(putDest, key)
			if hk, ok := putDest.(*HasKey); ok {
				hk.P = nil
			}
			err = n.Get(putDest)
			if err != nil {
				t.Errorf("%s - Get failed on object %#v", fetchType, putDest)
			}
			if putDest.Data() != gt.orig.Data() {
				t.Errorf("%s - Get request failed to populate data of %#v to %#v", fetchType, gt.orig, putDest)
			}
		}
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

func (src *HasId) Parent(g *Goon) *datastore.Key {
	return src.Self(g).Parent()
}

func (src *HasId) Self(g *Goon) *datastore.Key {
	key, err := g.getStructKey(src)
	if err == nil {
		return key
	}
	panic("err - " + err.Error())
}

func (src *HasId) Data() string {
	return src.Name
}

type HasKind struct {
	Id   int64  `datastore:"-" goon:"id"`
	Kind string `datastore:"-" goon:"kind"`
	Name string
}

func (src *HasKind) Parent(g *Goon) *datastore.Key {
	return src.Self(g).Parent()
}

func (src *HasKind) Self(g *Goon) *datastore.Key {
	key, err := g.getStructKey(src)
	if err == nil {
		return key
	}
	panic("err - " + err.Error())
}

func (src *HasKind) Data() string {
	return src.Name
}

type HasKey struct {
	Key  *datastore.Key `datastore:"-" goon:"id"`
	P    *datastore.Key `datastore:"-" goon:"parent"`
	Name string
}

func (src *HasKey) Parent(g *Goon) *datastore.Key {
	return src.P
}

func (src *HasKey) Self(g *Goon) *datastore.Key {
	key, err := g.getStructKey(src)
	if err == nil {
		return key
	}
	panic("err - " + err.Error())
}

func (src *HasKey) Data() string {
	return src.Name
}

type HasDefaultKind struct {
	Id   int64  `datastore:"-" goon:"id"`
	Kind string `datastore:"-" goon:"kind,DefaultKind"`
	Name string
}

func (src *HasDefaultKind) Parent(g *Goon) *datastore.Key {
	return src.Self(g).Parent()
}

func (src *HasDefaultKind) Self(g *Goon) *datastore.Key {
	key, err := g.getStructKey(src)
	if err == nil {
		return key
	}
	panic("err - " + err.Error())
}

func (src *HasDefaultKind) Data() string {
	return src.Name
}

type HasString struct {
	Name string
	Id   string `datastore:"-" goon:"id"`
}

func (src HasString) Parent(g *Goon) *datastore.Key {
	return src.Self(g).Parent()
}

func (src HasString) Self(g *Goon) *datastore.Key {
	key, err := g.getStructKey(src)
	if err == nil {
		return key
	}
	panic("err - " + err.Error())
}

func (src HasString) Data() string {
	return src.Name
}

type HasParent struct {
	Id   int64          `datastore:"-" goon:"id"`
	P    *datastore.Key `datastore:"-" goon:"parent"`
	Name string
}

func (src *HasParent) Parent(g *Goon) *datastore.Key {
	return src.P
}

func (src *HasParent) Self(g *Goon) *datastore.Key {
	key, err := g.getStructKey(src)
	if err == nil {
		return key
	}
	panic("err - " + err.Error())
}

func (src *HasParent) Data() string {
	return src.Name
}

type GoonStore interface {
	Data() string
	Parent(*Goon) *datastore.Key
	Self(*Goon) *datastore.Key
}

type GoonTest struct {
	orig   GoonStore
	putErr bool
}
