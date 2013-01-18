package goon

import (
	"appengine/datastore"
	"bytes"
	"encoding/gob"
	"fmt"
)

type Entity struct {
	Key      *datastore.Key
	Src      Kind
	StringID string
	IntID    int64

	NotFound bool
}

func (e *Entity) memkey() string {
	return memkey(e.Key)
}

type partialEntity struct {
	Src Kind
	NotFound bool
}

func (e *Entity) Gob() ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	gob.Register(e.Src)
	p := &partialEntity{
		Src: e.Src,
		NotFound: e.NotFound,
	}
	err := enc.Encode(p)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (e *Entity) String() string {
	return fmt.Sprintf("%v: %v", e.Key, e.Src)
}

func NewEntity(key *datastore.Key, src Kind) *Entity {
	e := &Entity{
		Src:      src,
	}
	e.setKey(key)
	return e
}

func (e *Entity) setKey(key *datastore.Key) {
	e.Key = key
	e.IntID = key.IntID()
	e.StringID = key.StringID()
}

func (g *Goon) NewEntity(parent *datastore.Key, src Kind) *Entity {
	return NewEntity(datastore.NewIncompleteKey(g.context, src.Kind(), parent), src)
}

func (g *Goon) KeyEntity(src Kind, stringID string, intID int64, parent *datastore.Key) *Entity {
	return NewEntity(datastore.NewKey(g.context, src.Kind(), stringID, intID, parent), src)
}
