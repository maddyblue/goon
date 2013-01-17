package goon

import (
	"appengine"
	"appengine/datastore"
	"appengine/memcache"
	"bytes"
	"encoding/gob"
	"fmt"
	"net/http"
)

type Goon struct {
	context appengine.Context
	cache   map[string]*Entity
}

type Entity struct {
	Key      *datastore.Key
	Src      Kind
	StringID string
	IntID    int64
}

func (e *Entity) memkey() string {
	return memkey(e.Key)
}

func memkey(k *datastore.Key) string {
	return k.String()
}

func (e *Entity) Gob() ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(e.Src)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (e *Entity) String() string {
	return fmt.Sprintf("%v: %v", e.Key, e.Src)
}

func NewG(r *http.Request) *Goon {
	return &Goon{
		context: appengine.NewContext(r),
		cache:   make(map[string]*Entity),
	}
}

func (g *Goon) Put(e *Entity) error {
	var err error

	if !e.Key.Incomplete() {
		_ = memcache.Delete(g.context, e.memkey())
	}

	k, err := datastore.Put(g.context, e.Key, e.Src)

	if err != nil {
		return err
	}

	e.Key = k
	return g.putMemcache(e)
}

func (g *Goon) putMemory(e *Entity) {
	g.cache[memkey(e.Key)] = e
}

func (g *Goon) putMemcache(e *Entity) error {
	gob, _ := e.Gob()
	err := memcache.Set(g.context, &memcache.Item{
		Key:   e.memkey(),
		Value: gob,
	})

	if err != nil {
		return err
	}

	g.putMemory(e)
	return nil
}

func NewEntity(key *datastore.Key, src Kind) *Entity {
	return &Entity{
		Key:      key,
		IntID:    key.IntID(),
		StringID: key.StringID(),
		Src:      src,
	}
}

func (g *Goon) NewEntity(parent *datastore.Key, src Kind) *Entity {
	return NewEntity(datastore.NewIncompleteKey(g.context, src.Kind(), parent), src)
}

func (g *Goon) Get(src Kind, stringID string, intID int64, parent *datastore.Key) (*Entity, error) {
	key := datastore.NewKey(g.context, src.Kind(), stringID, intID, parent)

	// try request cache
	if e, present := g.cache[memkey(key)]; present {
		return e, nil
	}

	// try memcache
	if item, err := memcache.Get(g.context, memkey(key)); err == nil {
		err := fromGob(src, item.Value)
		if err != nil {
			return nil, err
		}

		e := NewEntity(key, src)
		g.putMemory(e)
		return e, nil
	}

	// try datastore
	if err := datastore.Get(g.context, key, src); err != nil {
		return nil, err
	}

	e := NewEntity(key, src)
	err := g.putMemcache(e)

	if err != nil {
		return nil, err
	}

	return e, nil
}

func fromGob(src Kind, b []byte) error {
	var buf bytes.Buffer
	_, _ = buf.Write(b)
	dec := gob.NewDecoder(&buf)
	return dec.Decode(src)
}
