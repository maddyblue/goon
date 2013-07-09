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

package goapp

import (
	"appengine"
	"appengine/datastore"
	"appengine/memcache"
	"fmt"
	"github.com/mjibson/goon"
	"net/http"
	"time"
)

func init() {
	http.HandleFunc("/", Main)
}

func Main(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		return
	}

	c := appengine.NewContext(r)
	n := goon.NewGoon(r)

	// key tests

	noid := NoId{}
	if k, err := n.KeyError(noid); err != nil || !k.Incomplete() {
		fmt.Fprintln(w, "expected incomplete on noid")
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
	}

	for _, kt := range keyTests {
		if k, err := n.KeyError(kt.obj); err != nil {
			fmt.Fprintln(w, "error:", err.Error())
		} else if !k.Equal(kt.key) {
			fmt.Fprintln(w, "keys not equal")
		}
	}

	if _, err := n.KeyError(TwoId{IntId: 1, StringId: "1"}); err == nil {
		fmt.Fprintln(w, "expected key error")
	}

	// datastore tests

	initTest(c)
	if err := n.Get(&HasId{Id: 0}); err == nil {
		fmt.Fprintln(w, "ds: expected error")
	}
	if err := n.Get(&HasId{Id: 1}); err != datastore.ErrNoSuchEntity {
		fmt.Fprintln(w, "ds: expected no such entity")
	}
	// run twice to make sure autocaching works correctly
	if err := n.Get(&HasId{Id: 1}); err != datastore.ErrNoSuchEntity {
		fmt.Fprintln(w, "ds: expected no such entity")
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
		fmt.Fprintln(w, "ds: expected error")
	} else if !goon.NotFound(err, 0) {
		fmt.Fprintln(w, "ds: not found error 0")
	} else if !goon.NotFound(err, 1) {
		fmt.Fprintln(w, "ds: not found error 1")
	} else if goon.NotFound(err, 2) {
		fmt.Fprintln(w, "ds: not found error 2")
	}
	if keys, err := n.PutMulti(es); err != nil {
		fmt.Fprintln(w, "put: unexpected error")
	} else if len(keys) != len(esk) {
		fmt.Fprintln(w, "put: got unexpected number of keys")
	} else {
		for i, k := range keys {
			if !k.Equal(esk[i]) {
				fmt.Fprintln(w, "put: got unexpected keys")
			}
		}
	}
	if err := n.GetMulti(nes); err != nil {
		fmt.Fprintln(w, "put: unexpected error")
	} else if es[0] != nes[0] || es[1] != nes[1] {
		fmt.Fprintln(w, "put: bad results")
	} else {
		nesk0 := n.Key(nes[0])
		if !nesk0.Equal(datastore.NewKey(c, "HasId", "", 1, nil)) {
			fmt.Fprintln(w, "put: bad key")
		}
		nesk1 := n.Key(nes[1])
		if !nesk1.Equal(datastore.NewKey(c, "HasId", "", 2, nil)) {
			fmt.Fprintln(w, "put: bad key")
		}
	}
	if _, err := n.Put(HasId{Id: 3}); err == nil {
		fmt.Fprintln(w, "put: expected error")
	}
	// force partial fetch from memcache and then datastore
	memcache.Flush(c)
	if err := n.Get(nes[0]); err != nil {
		fmt.Fprintln(w, "get: unexpected error")
	}
	if err := n.GetMulti(nes); err != nil {
		fmt.Fprintln(w, "get: unexpected error")
	}

	if _, err := n.PutComplete(&HasId{}); err == nil {
		fmt.Fprintln(w, "put complete: expected error")
	}
	if _, err := n.PutComplete(&HasId{Id: 1}); err != nil {
		fmt.Fprintln(w, "put complete: unexpected error")
	}

	fmt.Fprintln(w, "done", time.Now())
}

func initTest(c appengine.Context) {
	keys, _ := datastore.NewQuery("HasId").KeysOnly().GetAll(c, nil)
	datastore.DeleteMulti(c, keys)
	memcache.Flush(c)
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

type TwoId struct {
	IntId    int64  `goon:"id"`
	StringId string `goon:"id"`
}
