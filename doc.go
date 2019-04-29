/*
Package goon provides an autocaching interface to the app engine datastore
similar to the python NDB package.

Goon differs from the datastore package in various ways: it remembers the
appengine Context, which need only be specified once at creation time; kinds
need not be specified as they are computed, by default, from a type's name;
keys are inferred from specially-tagged fields on types, removing the need to
pass key objects around.

In general, the difference is that Goon's API is identical to the datastore API,
it's just shorter.

Keys in Goon are stored in the structs themselves. Below is an example struct
with a field to specify the id (see the Key Specifications section below for
full documentation).
	type User struct {
		Id    string `datastore:"-" goon:"id"`
		Name  string
	}

Thus, to get a User with id 2:
	userid := 2
	g := goon.NewGoon(r)
	u := &User{Id: userid}
	g.Get(u)

Key Specifications

For both the Key and KeyError functions, src must be a S or *S for some
struct type S. The key is extracted based on various fields of S. If a field
of type int64 or string has a struct tag named goon with value "id", it is
used as the key's id. If a field of type *datastore.Key has a struct tag
named goon with value "parent", it is used as the key's parent. If a field
of type string has a struct tag named goon with value "kind", it is used
as the key's kind. The "kind" field supports an optional second parameter
which is the default kind name. If no kind field exists, the struct's name
is used. These fields should all have their datastore field marked as "-".

Example, with kind User:
	type User struct {
		Id    string `datastore:"-" goon:"id"`
		Read  time.Time
	}

Example, with kind U if _kind is the empty string:
	type User struct {
		_kind string `goon:"kind,U"`
		Id    string `datastore:"-" goon:"id"`
		Read  time.Time
	}

To override kind of a single entity to UserKind:
	u := User{_kind: "UserKind"}

An example with both parent and kind:
	type UserData struct {
		Id     string         `datastore:"-" goon:"id"`
		_kind  string         `goon:"kind,UD"`
		Parent *datastore.Key `datastore:"-" goon:"parent"`
		Data   []byte
	}

Features

Datastore interaction with: Get, GetMulti, Put, PutMulti, Delete, DeleteMulti, Queries.

All key-based operations backed by memory and memcache.

Per-request, in-memory cache: fetch the same key twice, the second request is served from local memory.

Intelligent multi support: running GetMulti correctly fetches from memory, then memcache, then the datastore; each tier only sends keys off to the next one if they were missing.

Memcache control variance: long memcache requests are cancelled.

Transactions use a separate context, but locally cache any results on success.

Automatic kind naming: struct names are inferred by reflection, removing the need to manually specify key kinds.

Simpler API than appengine/datastore.

API comparison between goon and datastore

put with incomplete key

datastore:

	type Group struct {
		Name string
	}
	c := appengine.NewContext(r)
	g := &Group{Name: "name"}
	k := datastore.NewIncompleteKey(c, "Group", nil)
	err := datastore.Put(c, k, g)

goon:

	type Group struct {
		Id   int64 `datastore:"-" goon:"id"`
		Name string
	}
	n := goon.NewGoon(r)
	g := &Group{Name: "name"}
	err := n.Put(g)

get with known key

datastore:

	type Group struct {
		Name string
	}
	c := appengine.NewContext(r)
	g := &Group{}
	k := datastore.NewKey(c, "Group", "", 1, nil)
	err := datastore.Get(c, k, g)

goon:

	type Group struct {
		Id   int64 `datastore:"-" goon:"id"`
		Name string
	}
	n := goon.NewGoon(r)
	g := &Group{Id: 1}
	err := n.Get(g)

Memcache Control Variance

Memcache is generally fast. When it is slow, goon will timeout the memcache
requests and proceed to use the datastore directly. The memcache put and
get timeout variables determine how long to wait for various kinds of
requests. The default settings were determined experimentally and should
provide reasonable defaults for most applications.

See: http://talks.golang.org/2013/highperf.slide#23


PropertyLoadSaver support

Structs that implement the PropertyLoadSaver interface are guaranteed to call
the Save() method once and only once per Put/PutMulti call and never elsewhere.
Similarly the Load() method is guaranteed to be called once and only once per
Get/GetMulti/GetAll/Next call and never elsewhere.

Keep in mind that the goon local cache is just a pointer to the previous result.
This means that when you use Get to fetch something into a PropertyLoadSaver
implementing struct, that struct's pointer is saved. Subsequent calls to Get
will just return that pointer. This means that although Load() was called once
during the initial Get, the subsequent calls to Get won't call Load() again.
Generally this shouldn't be an issue, but e.g. if you generate some random
data in the Load() call then sequential calls to Get that share the same goon
local cache will always return the random data of the first call.

A gotcha can be encountered with the local cache where Load() is never called.
Specifically if you first call Get to load some entity into a struct S which
does *not* implement PropertyLoadSaver. The local cache will now have a pointer
to this struct S. When you now proceed to call Get again on the same id but into
a struct PLS which implements PropertyLoadSaver but is also convertible from S.
Then your struct PLS will be applied the value of S, however Load() will never
be called, because it wasn't the first time and it's never called when loading
from the local cache. This is a very specific edge case that won't affect 99.9%
of developers using goon. This issue does not exist with memcache/datastore,
so either flushing the local cache or doing the S->PLS migration in different
requests will solve the issue.


Local memory cache

Entities retrieved via Get/GetMulti/GetAll/Next are stored in the local cache.
The local cache is an in-memory per-goon-instance cache that keeps a pointer
to the returned result. When attempting to retrieve the same entity again you
will be returned a shallow copy of the pointer's value.

Imagine a struct type Foo:

	type Foo struct {
		Id int64 `datastore:"-" goon:"id"`
		X int
		Y []byte
	}

Then behavior is as follows:

	fA := &Foo{Id: 1}    // fA.X == 0 | fA.Y == []
	g.Get(fA)            // fA.X == 1 | fA.Y == [1, 2]  Initial datastore values
	fA.X = 2             // fA.X == 2 | fA.Y == [1, 2]  We change the int
	fA.Y = []byte{3, 4}  // fA.X == 2 | fA.Y == [3, 4]  ..and create a new slice
	fB := &Foo{Id: 1}    // fB.X == 0 | fB.Y == []
	g.Get(fB)            // fB.X == 2 | fB.Y == [3, 4]  fB is now a copy of fA
	fB.X = 3             // fB.X == 3 | fB.Y == [3, 4]  Changing fB.X is local
	fB.Y[0] = 5          // fB.X == 3 | fB.Y == [5, 4]  ..but changing deep data
	                     // fA.X == 2 | fA.Y == [5, 4]  will also appear in fA.Y
	fA.Y = []byte{6, 7}  // fA.X == 2 | fA.Y == [6, 7]  However creating a new
	                     // fB.X == 3 | fB.Y == [5, 4]  slice will not propagate

The reason the cache works with pointers is because App Engine is very memory
limited and storing just pointers takes up very little memory. However this
means that any changes you perform on an entity that was cached will also be
reflected in any future gets for that same entity with the same goon instance.
What's more, the copy will be shallow. So any value-types likes primitive ints
can be then changed and will only be changed for that specific struct instance.
However deep modifications of slices will propagate to all copies of the struct.

Thus for the most care-free life you should treat fetched entities as immutable,
which means you never modify their data. That way the whole pointer/copy
strategy won't have any effect on your app logic. For modifications, always do
them inside a transaction because local cache is disabled there.

This means that the local cache is **not** thread-safe for modifications. Make
sure you either don't modify entities fetched outside of transactions or that
you never fetch the same id from different goroutines using one goon instance.

*/
package goon
