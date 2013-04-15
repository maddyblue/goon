# goon

An autocaching interface to the app engine datastore for Go. Designed to be similar to the python NDB package.

Documentation: [http://godoc.org/github.com/mjibson/goon](http://godoc.org/github.com/mjibson/goon)

## features

- **datastore** interaction with: Get, GetMulti, Put, PutMulti, Delete, DeleteMulti, Queries
- all key-based operations backed by **memory** and **memcache**
- per-request, **in-memory cache**: fetch the same key twice, the second request is served from local memory
- intelligent **multi** support: running GetMulti on correctly fetches from memory, then memcache, then the datastore; each tier only sends keys off to the next one if they were missing
- **transactions** use a separate context, but locally cache any results on success
- automatic **kind naming**: struct names are inferred by reflection, removing the need to manually specify key kinds
- simpler **api** than appengine/datastore

## api comparisons between goon and appengine/datastore

Assume the following exists:

```
type Group struct {
  name string
}

c := appengine.NewContext(r)
n := goon.NewGoon(r)
g := new(Group)
```

### Put with new, unknown key

datastore:
```
g.name = "test"
k := datastore.NewIncompleteKey(c, "Group", nil)
err := datastore.Put(c, k, g)
```

goon:
```
g.name = "test"
e, _ := n.NewEntity(nil, g)
err := n.Put(e)
```

### Get with known key

datastore:
```
k := datastore.NewKey(c, "Group", "stringID", 0, nil)
err := datastore.Get(c, k, g)
```

goon:
```
e, err := n.GetById(g, "stringId", 0, nil)
```
