/*
 * Copyright (c) 2012 The Goon Authors
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
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"google.golang.org/appengine"
	"google.golang.org/appengine/aetest"
	"google.golang.org/appengine/datastore"
	"google.golang.org/appengine/memcache"
)

func init() {
	// The SDK emulators are extremely slow, so we can't use production timeouts
	MemcachePutTimeoutSmall = 10 * time.Second
	MemcacheGetTimeout = 10 * time.Second
	// Make sure to propagate all errors for better testing
	propagateMemcachePutError = true
}

// *[]S, *[]*S, *[]I, []S, []*S, []I,
// *[]PLS, *[]*PLS, *[]IPLS, []PLS, []*PLS, []IPLS
const (
	ivTypePtrToSliceOfStructs = iota
	ivTypePtrToSliceOfPtrsToStruct
	ivTypePtrToSliceOfInterfaces
	ivTypeSliceOfStructs
	ivTypeSliceOfPtrsToStruct
	ivTypeSliceOfInterfaces
	ivTypePtrToSliceOfPLS
	ivTypePtrToSliceOfPtrsToPLS
	ivTypePtrToSliceOfInterfacesPLS
	ivTypeSliceOfPLS
	ivTypeSliceOfPtrsToPLS
	ivTypeSliceOfInterfacesPLS
	ivTypeTotal
)

const (
	ivModeDatastore = iota
	ivModeMemcache
	ivModeMemcacheAndDatastore
	ivModeLocalcache
	ivModeLocalcacheAndMemcache
	ivModeLocalcacheAndDatastore
	ivModeLocalcacheAndMemcacheAndDatastore
	ivModeTotal
)

func cloneKey(key *datastore.Key) *datastore.Key {
	if key == nil {
		return nil
	}
	dupe, err := datastore.DecodeKey(key.Encode())
	if err != nil {
		panic(fmt.Sprintf("Failed to clone key: %v", err))
	}
	return dupe
}

func cloneKeys(keys []*datastore.Key) []*datastore.Key {
	if keys == nil {
		return nil
	}
	dupe := make([]*datastore.Key, 0, len(keys))
	for _, key := range keys {
		if key == nil {
			dupe = append(dupe, nil)
		} else {
			dupe = append(dupe, cloneKey(key))
		}
	}
	return dupe
}

func TestCloneIVItem(t *testing.T) {
	c, done, err := aetest.NewContext()
	if err != nil {
		t.Fatalf("Could not start aetest - %v", err)
	}
	defer done()

	initializeIvItems(c)

	for i := range ivItems {
		clone := *ivItems[i].clone()
		if !reflect.DeepEqual(ivItems[i], clone) {
			t.Fatalf("ivItem clone failed!\n%v", getDiff(ivItems[i], clone, fmt.Sprintf("ivItems[%d]", i), "clone"))
		}
	}
}

// Have a bunch of different supported types to detect any wild errors
// https://cloud.google.com/appengine/docs/standard/go/datastore/reference
//
// - signed integers (int, int8, int16, int32 and int64),
// - bool,
// - string,
// - float32 and float64,
// - []byte (up to 1 megabyte in length),
// - any type whose underlying type is one of the above predeclared types,
// - ByteString,
// - *Key,
// - time.Time (stored with microsecond precision),
// - appengine.BlobKey,
// - appengine.GeoPoint,
// - structs whose fields are all valid value types,
// - slices of any of the above.
//
// In addition, although undocumented, there's also support for any type,
// whose underlying type is a legal slice.
type ivItem struct {
	Id           int64                `datastore:"-" goon:"id"`
	Kind         string               `datastore:"-" goon:"kind,ivItem"`
	Int          int                  `datastore:"int,noindex"`
	Int8         int8                 `datastore:"int8,noindex"`
	Int16        int16                `datastore:"int16,noindex"`
	Int32        int32                `datastore:"int32,noindex"`
	Int64        int64                `datastore:"int64,noindex"`
	Bool         bool                 `datastore:"bool,noindex"`
	String       string               `datastore:"string,noindex"`
	Float32      float32              `datastore:"float32,noindex"`
	Float64      float64              `datastore:"float64,noindex"`
	ByteSlice    []byte               `datastore:"byte_slice,noindex"`
	CustomTypes  ivItemCustom         `datastore:"custom,noindex"`
	BString      datastore.ByteString `datastore:"bstr,noindex"`
	Key          *datastore.Key       `datastore:"key,noindex"`
	Time         time.Time            `datastore:"time,noindex"`
	BlobKey      appengine.BlobKey    `datastore:"bk,noindex"`
	GeoPoint     appengine.GeoPoint   `datastore:"gp,noindex"`
	Sub          ivItemSub            `datastore:"sub,noindex"`
	SliceTypes   ivItemSlice          `datastore:"slice,noindex"`
	CustomSlices ivItemSliceCustom    `datastore:"custom_slice,noindex"`

	NoIndex     int `datastore:",noindex"`
	Casual      string
	Ζεύς        string
	ChildKey    *datastore.Key
	ZeroKey     *datastore.Key
	KeySliceNil []*datastore.Key

	SaveCount int
	LoadCount int
}

func (ivi ivItem) clone() *ivItem {
	return &ivItem{
		Id:           ivi.Id,
		Int:          ivi.Int,
		Int8:         ivi.Int8,
		Int16:        ivi.Int16,
		Int32:        ivi.Int32,
		Int64:        ivi.Int64,
		Bool:         ivi.Bool,
		String:       ivi.String,
		Float32:      ivi.Float32,
		Float64:      ivi.Float64,
		ByteSlice:    append(ivi.ByteSlice[:0:0], ivi.ByteSlice...),
		CustomTypes:  *ivi.CustomTypes.clone(),
		BString:      append(ivi.BString[:0:0], ivi.BString...),
		Key:          cloneKey(ivi.Key),
		Time:         ivi.Time,
		BlobKey:      ivi.BlobKey,
		GeoPoint:     ivi.GeoPoint,
		Sub:          *ivi.Sub.clone(),
		SliceTypes:   *ivi.SliceTypes.clone(),
		CustomSlices: *ivi.CustomSlices.clone(),
		NoIndex:      ivi.NoIndex,
		Casual:       ivi.Casual,
		Ζεύς:         ivi.Ζεύς,
		ChildKey:     cloneKey(ivi.ChildKey),
		ZeroKey:      cloneKey(ivi.ZeroKey),
		KeySliceNil:  cloneKeys(ivi.KeySliceNil),
		SaveCount:    ivi.SaveCount,
		LoadCount:    ivi.LoadCount,
	}
}

type ivItemInt int
type ivItemInt8 int8
type ivItemInt16 int16
type ivItemInt32 int32
type ivItemInt64 int64
type ivItemBool bool
type ivItemString string
type ivItemFloat32 float32
type ivItemFloat64 float64
type ivItemByteSlice []byte

type ivItemDeepInt ivItemInt

type ivItemCustom struct {
	Int       ivItemInt
	Int8      ivItemInt8
	Int16     ivItemInt16
	Int32     ivItemInt32
	Int64     ivItemInt64
	Bool      ivItemBool
	String    ivItemString
	Float32   ivItemFloat32
	Float64   ivItemFloat64
	ByteSlice ivItemByteSlice
	DeepInt   ivItemDeepInt
}

func (ivic ivItemCustom) clone() *ivItemCustom {
	return &ivItemCustom{
		Int:       ivic.Int,
		Int8:      ivic.Int8,
		Int16:     ivic.Int16,
		Int32:     ivic.Int32,
		Int64:     ivic.Int64,
		Bool:      ivic.Bool,
		String:    ivic.String,
		Float32:   ivic.Float32,
		Float64:   ivic.Float64,
		ByteSlice: append(ivic.ByteSlice[:0:0], ivic.ByteSlice...),
		DeepInt:   ivic.DeepInt,
	}
}

type ivItemSlice struct {
	Int       []int
	Int8      []int8
	Int16     []int16
	Int32     []int32
	Int64     []int64
	Bool      []bool
	String    []string
	Float32   []float32
	Float64   []float64
	BSSlice   [][]byte
	IntC      []ivItemInt
	Int8C     []ivItemInt8
	Int16C    []ivItemInt16
	Int32C    []ivItemInt32
	Int64C    []ivItemInt64
	BoolC     []ivItemBool
	StringC   []ivItemString
	Float32C  []ivItemFloat32
	Float64C  []ivItemFloat64
	BSSliceC  []ivItemByteSlice
	DeepInt   []ivItemDeepInt
	BStrSlice []datastore.ByteString
	KeySlice  []*datastore.Key
	TimeSlice []time.Time
	BKSlice   []appengine.BlobKey
	GPSlice   []appengine.GeoPoint
	Subs      []ivItemSubs
}

func (ivis ivItemSlice) clone() *ivItemSlice {
	bsSlice := ivis.BSSlice[:0:0]
	for _, bs := range ivis.BSSlice {
		bsSlice = append(bsSlice, append(bs[:0:0], bs...))
	}
	bsSliceC := ivis.BSSliceC[:0:0]
	for _, bsc := range ivis.BSSliceC {
		bsSliceC = append(bsSliceC, append(bsc[:0:0], bsc...))
	}
	bstrSlice := ivis.BStrSlice[:0:0]
	for _, bstr := range ivis.BStrSlice {
		bstrSlice = append(bstrSlice, append(bstr[:0:0], bstr...))
	}
	subs := ivis.Subs[:0:0]
	for _, sub := range ivis.Subs {
		subs = append(subs, *sub.clone())
	}

	return &ivItemSlice{
		Int:       append(ivis.Int[:0:0], ivis.Int...),
		Int8:      append(ivis.Int8[:0:0], ivis.Int8...),
		Int16:     append(ivis.Int16[:0:0], ivis.Int16...),
		Int32:     append(ivis.Int32[:0:0], ivis.Int32...),
		Int64:     append(ivis.Int64[:0:0], ivis.Int64...),
		Bool:      append(ivis.Bool[:0:0], ivis.Bool...),
		String:    append(ivis.String[:0:0], ivis.String...),
		Float32:   append(ivis.Float32[:0:0], ivis.Float32...),
		Float64:   append(ivis.Float64[:0:0], ivis.Float64...),
		BSSlice:   bsSlice,
		IntC:      append(ivis.IntC[:0:0], ivis.IntC...),
		Int8C:     append(ivis.Int8C[:0:0], ivis.Int8C...),
		Int16C:    append(ivis.Int16C[:0:0], ivis.Int16C...),
		Int32C:    append(ivis.Int32C[:0:0], ivis.Int32C...),
		Int64C:    append(ivis.Int64C[:0:0], ivis.Int64C...),
		BoolC:     append(ivis.BoolC[:0:0], ivis.BoolC...),
		StringC:   append(ivis.StringC[:0:0], ivis.StringC...),
		Float32C:  append(ivis.Float32C[:0:0], ivis.Float32C...),
		Float64C:  append(ivis.Float64C[:0:0], ivis.Float64C...),
		BSSliceC:  bsSliceC,
		DeepInt:   append(ivis.DeepInt[:0:0], ivis.DeepInt...),
		BStrSlice: bstrSlice,
		KeySlice:  cloneKeys(ivis.KeySlice),
		TimeSlice: append(ivis.TimeSlice[:0:0], ivis.TimeSlice...),
		BKSlice:   append(ivis.BKSlice[:0:0], ivis.BKSlice...),
		GPSlice:   append(ivis.GPSlice[:0:0], ivis.GPSlice...),
		Subs:      subs,
	}
}

type IntS []int
type Int8S []int8
type Int16S []int16
type Int32S []int32
type Int64S []int64
type BoolS []bool
type StringS []string
type Float32S []float32
type Float64S []float64
type BSSliceS [][]byte
type IntCS []ivItemInt
type Int8CS []ivItemInt8
type Int16CS []ivItemInt16
type Int32CS []ivItemInt32
type Int64CS []ivItemInt64
type BoolCS []ivItemBool
type StringCS []ivItemString
type Float32CS []ivItemFloat32
type Float64CS []ivItemFloat64
type BSSliceCS []ivItemByteSlice
type DeepIntS []ivItemDeepInt
type BStrSliceS []datastore.ByteString
type KeySliceS []*datastore.Key
type TimeSliceS []time.Time
type BKSliceS []appengine.BlobKey
type GPSliceS []appengine.GeoPoint
type SubsS []ivItemSubs

type ivItemSliceCustom struct {
	Int       IntS
	Int8      Int8S
	Int16     Int16S
	Int32     Int32S
	Int64     Int64S
	Bool      BoolS
	String    StringS
	Float32   Float32S
	Float64   Float64S
	BSSlice   BSSliceS
	IntC      IntCS
	Int8C     Int8CS
	Int16C    Int16CS
	Int32C    Int32CS
	Int64C    Int64CS
	BoolC     BoolCS
	StringC   StringCS
	Float32C  Float32CS
	Float64C  Float64CS
	BSSliceC  BSSliceCS
	DeepInt   DeepIntS
	BStrSlice BStrSliceS
	KeySlice  KeySliceS
	TimeSlice TimeSliceS
	BKSlice   BKSliceS
	GPSlice   GPSliceS
	Subs      SubsS
}

func (ivisc ivItemSliceCustom) clone() *ivItemSliceCustom {
	bsSlice := ivisc.BSSlice[:0:0]
	for _, bs := range ivisc.BSSlice {
		bsSlice = append(bsSlice, append(bs[:0:0], bs...))
	}
	bsSliceC := ivisc.BSSliceC[:0:0]
	for _, bsc := range ivisc.BSSliceC {
		bsSliceC = append(bsSliceC, append(bsc[:0:0], bsc...))
	}
	bstrSlice := ivisc.BStrSlice[:0:0]
	for _, bstr := range ivisc.BStrSlice {
		bstrSlice = append(bstrSlice, append(bstr[:0:0], bstr...))
	}
	subs := ivisc.Subs[:0:0]
	for _, sub := range ivisc.Subs {
		subs = append(subs, *sub.clone())
	}

	return &ivItemSliceCustom{
		Int:       append(ivisc.Int[:0:0], ivisc.Int...),
		Int8:      append(ivisc.Int8[:0:0], ivisc.Int8...),
		Int16:     append(ivisc.Int16[:0:0], ivisc.Int16...),
		Int32:     append(ivisc.Int32[:0:0], ivisc.Int32...),
		Int64:     append(ivisc.Int64[:0:0], ivisc.Int64...),
		Bool:      append(ivisc.Bool[:0:0], ivisc.Bool...),
		String:    append(ivisc.String[:0:0], ivisc.String...),
		Float32:   append(ivisc.Float32[:0:0], ivisc.Float32...),
		Float64:   append(ivisc.Float64[:0:0], ivisc.Float64...),
		BSSlice:   bsSlice,
		IntC:      append(ivisc.IntC[:0:0], ivisc.IntC...),
		Int8C:     append(ivisc.Int8C[:0:0], ivisc.Int8C...),
		Int16C:    append(ivisc.Int16C[:0:0], ivisc.Int16C...),
		Int32C:    append(ivisc.Int32C[:0:0], ivisc.Int32C...),
		Int64C:    append(ivisc.Int64C[:0:0], ivisc.Int64C...),
		BoolC:     append(ivisc.BoolC[:0:0], ivisc.BoolC...),
		StringC:   append(ivisc.StringC[:0:0], ivisc.StringC...),
		Float32C:  append(ivisc.Float32C[:0:0], ivisc.Float32C...),
		Float64C:  append(ivisc.Float64C[:0:0], ivisc.Float64C...),
		BSSliceC:  bsSliceC,
		DeepInt:   append(ivisc.DeepInt[:0:0], ivisc.DeepInt...),
		BStrSlice: bstrSlice,
		KeySlice:  cloneKeys(ivisc.KeySlice),
		TimeSlice: append(ivisc.TimeSlice[:0:0], ivisc.TimeSlice...),
		BKSlice:   append(ivisc.BKSlice[:0:0], ivisc.BKSlice...),
		GPSlice:   append(ivisc.GPSlice[:0:0], ivisc.GPSlice...),
		Subs:      subs,
	}
}

type ivItemSub struct {
	Data string `datastore:"data,noindex"`
	Ints []int  `datastore:"ints,noindex"`
}

func (ivis ivItemSub) clone() *ivItemSub {
	return &ivItemSub{
		Data: ivis.Data,
		Ints: append(ivis.Ints[:0:0], ivis.Ints...),
	}
}

type ivItemSubs struct {
	Key   *datastore.Key `datastore:"key,noindex"`
	Data  string         `datastore:"data,noindex"`
	Extra string         `datastore:",noindex"`
}

func (ivis ivItemSubs) clone() *ivItemSubs {
	return &ivItemSubs{
		Key:   cloneKey(ivis.Key),
		Data:  ivis.Data,
		Extra: ivis.Extra,
	}
}

func (ivi *ivItem) ForInterface()    {}
func (ivi *ivItemPLS) ForInterface() {}

type ivItemI interface {
	ForInterface()
}

// Implement the PropertyLoadSave interface for ivItem
type ivItemPLS ivItem

func (ivi *ivItemPLS) Save() ([]datastore.Property, error) {
	ivi.SaveCount++
	return datastore.SaveStruct(ivi)
}

func (ivi *ivItemPLS) Load(props []datastore.Property) error {
	err := datastore.LoadStruct(ivi, props)
	ivi.LoadCount++
	return err
}

var ivItems []ivItem
var ivItemKeys []*datastore.Key

func initializeIvItems(c context.Context) {
	// We force UTC, because the datastore API will always return UTC
	t1 := time.Now().UTC().Truncate(time.Microsecond)
	t2 := t1.Add(time.Second * 1)
	t3 := t1.Add(time.Second * 2)

	ivi1 := &ivItem{
		Id:        1,
		Int:       123,
		Int8:      77,
		Int16:     13001,
		Int32:     1234567890,
		Int64:     123456789012345,
		Bool:      true,
		String:    "one",
		Float32:   (float32(10) / float32(3)),
		Float64:   (float64(10000000) / float64(9998)),
		ByteSlice: []byte{0xDE, 0xAD},
		CustomTypes: ivItemCustom{
			Int:       123,
			Int8:      77,
			Int16:     13001,
			Int32:     1234567890,
			Int64:     123456789012345,
			Bool:      true,
			String:    "one",
			Float32:   ivItemFloat32(float32(10) / float32(3)),
			Float64:   ivItemFloat64(float64(10000000) / float64(9998)),
			ByteSlice: ivItemByteSlice([]byte{0x01, 0x02, 0xAA}),
			DeepInt:   1,
		},
		BString:  datastore.ByteString([]byte{0xAB}),
		Key:      datastore.NewKey(c, "Fruit", "Apple", 0, nil),
		Time:     t1,
		BlobKey:  appengine.BlobKey("fake #1"),
		GeoPoint: appengine.GeoPoint{Lat: 1.1, Lng: 2.2},
		Sub: ivItemSub{
			Data: "yay #1",
			Ints: []int{1, 2, 3},
		},
		SliceTypes: ivItemSlice{
			Int:       []int{1, 2},
			Int8:      []int8{1, 2},
			Int16:     []int16{1, 2},
			Int32:     []int32{1, 2},
			Int64:     []int64{1, 2},
			Bool:      []bool{true, false},
			String:    []string{"one", "two"},
			Float32:   []float32{1.0, 2.0},
			Float64:   []float64{1.0, 2.0},
			BSSlice:   [][]byte{{0x01, 0x02}, {0x03, 0x04}},
			IntC:      []ivItemInt{1, 2},
			Int8C:     []ivItemInt8{1, 2},
			Int16C:    []ivItemInt16{1, 2},
			Int32C:    []ivItemInt32{1, 2},
			Int64C:    []ivItemInt64{1, 2},
			BoolC:     []ivItemBool{true, false},
			StringC:   []ivItemString{"one", "two"},
			Float32C:  []ivItemFloat32{1.0, 2.0},
			Float64C:  []ivItemFloat64{1.0, 2.0},
			BSSliceC:  []ivItemByteSlice{{0x01, 0x02}, {0x03, 0x04}},
			DeepInt:   []ivItemDeepInt{1, 2},
			BStrSlice: []datastore.ByteString{datastore.ByteString("one"), datastore.ByteString("two")},
			KeySlice:  []*datastore.Key{datastore.NewKey(c, "Key", "", 1, nil), datastore.NewKey(c, "Key", "", 2, nil), datastore.NewKey(c, "Key", "", 3, nil)},
			TimeSlice: []time.Time{t1, t2, t3},
			BKSlice:   []appengine.BlobKey{appengine.BlobKey("fake #1.1"), appengine.BlobKey("fake #1.2")},
			GPSlice:   []appengine.GeoPoint{{Lat: 1.1, Lng: -2.2}, {Lat: -3.3, Lng: 4.4}},
			Subs: []ivItemSubs{
				{Key: datastore.NewKey(c, "Fruit", "Banana", 0, nil), Data: "sub #1.1", Extra: "xtra #1.1"},
				{Key: nil, Data: "sub #1.2", Extra: "xtra #1.2"},
				{Key: datastore.NewKey(c, "Fruit", "Cherry", 0, nil), Data: "sub #1.3", Extra: "xtra #1.3"},
			},
		},
		CustomSlices: ivItemSliceCustom{
			Int:       IntS{1, 2},
			Int8:      Int8S{1, 2},
			Int16:     Int16S{1, 2},
			Int32:     Int32S{1, 2},
			Int64:     Int64S{1, 2},
			Bool:      BoolS{true, false},
			String:    StringS{"one", "two"},
			Float32:   Float32S{1.0, 2.0},
			Float64:   Float64S{1.0, 2.0},
			BSSlice:   BSSliceS{[]byte{0x01, 0x02}, []byte{0x03, 0x04}},
			IntC:      IntCS{1, 2},
			Int8C:     Int8CS{1, 2},
			Int16C:    Int16CS{1, 2},
			Int32C:    Int32CS{1, 2},
			Int64C:    Int64CS{1, 2},
			BoolC:     BoolCS{true, false},
			StringC:   StringCS{"one", "two"},
			Float32C:  Float32CS{1.0, 2.0},
			Float64C:  Float64CS{1.0, 2.0},
			BSSliceC:  BSSliceCS{ivItemByteSlice{0x01, 0x02}, ivItemByteSlice{0x03, 0x04}},
			DeepInt:   DeepIntS{1, 2},
			BStrSlice: BStrSliceS{datastore.ByteString("one"), datastore.ByteString("two")},
			KeySlice:  KeySliceS{datastore.NewKey(c, "Key", "", 1, nil), datastore.NewKey(c, "Key", "", 2, nil), datastore.NewKey(c, "Key", "", 3, nil)},
			TimeSlice: TimeSliceS{t1, t2, t3},
			BKSlice:   BKSliceS{appengine.BlobKey("fake #1.1"), appengine.BlobKey("fake #1.2")},
			GPSlice:   GPSliceS{appengine.GeoPoint{Lat: 1.1, Lng: -2.2}, appengine.GeoPoint{Lat: -3.3, Lng: 4.4}},
			Subs: SubsS{
				{Key: datastore.NewKey(c, "Fruit", "Banana", 0, nil), Data: "sub #1.1", Extra: "xtra #1.1"},
				{Key: datastore.NewKey(c, "Fruit", "Cherry", 0, nil), Data: "sub #1.2", Extra: "xtra #1.2"},
				{Key: nil, Data: "sub #1.3", Extra: "xtra #1.3"},
			},
		},
		NoIndex:     1,
		Casual:      "clothes",
		Ζεύς:        "Zeus",
		ChildKey:    datastore.NewKey(c, "Person", "Jane", 0, datastore.NewKey(c, "Person", "John", 0, datastore.NewKey(c, "Person", "Jack", 0, nil))),
		ZeroKey:     nil,
		KeySliceNil: []*datastore.Key{datastore.NewKey(c, "Number", "", 1, nil), nil, datastore.NewKey(c, "Number", "", 2, nil)},
	}

	ivi2 := ivi1.clone()
	ivi2.Id = 2

	ivi3 := ivi1.clone()
	ivi3.Id = 3

	ivItems = append(ivItems, *ivi1)
	ivItems = append(ivItems, *ivi2)
	ivItems = append(ivItems, *ivi3)

	g := FromContext(c)
	for i := range ivItems {
		ivItemKeys = append(ivItemKeys, g.Key(&ivItems[i]))
	}
}

func getInputVarietyItem(t *testing.T, g *Goon, ivType int, empty bool, indices ...int) interface{} {
	var result interface{}

	getItem := func(index int) *ivItem {
		if empty {
			return &ivItem{Id: ivItems[index].Id}
		}
		return ivItems[index].clone()
	}

	switch ivType {
	case ivTypePtrToSliceOfStructs:
		s := []ivItem{}
		for _, index := range indices {
			s = append(s, *getItem(index))
		}
		result = &s
	case ivTypePtrToSliceOfPtrsToStruct:
		s := []*ivItem{}
		for _, index := range indices {
			s = append(s, getItem(index))
		}
		result = &s
	case ivTypePtrToSliceOfInterfaces:
		s := []ivItemI{}
		for _, index := range indices {
			s = append(s, getItem(index))
		}
		result = &s
	case ivTypeSliceOfStructs:
		s := []ivItem{}
		for _, index := range indices {
			s = append(s, *getItem(index))
		}
		result = s
	case ivTypeSliceOfPtrsToStruct:
		s := []*ivItem{}
		for _, index := range indices {
			s = append(s, getItem(index))
		}
		result = s
	case ivTypeSliceOfInterfaces:
		s := []ivItemI{}
		for _, index := range indices {
			s = append(s, getItem(index))
		}
		result = s
	case ivTypePtrToSliceOfPLS:
		s := []ivItemPLS{}
		for _, index := range indices {
			s = append(s, (ivItemPLS)(*getItem(index)))
		}
		result = &s
	case ivTypePtrToSliceOfPtrsToPLS:
		s := []*ivItemPLS{}
		for _, index := range indices {
			s = append(s, (*ivItemPLS)(getItem(index)))
		}
		result = &s
	case ivTypePtrToSliceOfInterfacesPLS:
		s := []ivItemI{}
		for _, index := range indices {
			s = append(s, (*ivItemPLS)(getItem(index)))
		}
		result = &s
	case ivTypeSliceOfPLS:
		s := []ivItemPLS{}
		for _, index := range indices {
			s = append(s, (ivItemPLS)(*getItem(index)))
		}
		result = s
	case ivTypeSliceOfPtrsToPLS:
		s := []*ivItemPLS{}
		for _, index := range indices {
			s = append(s, (*ivItemPLS)(getItem(index)))
		}
		result = s
	case ivTypeSliceOfInterfacesPLS:
		s := []ivItemI{}
		for _, index := range indices {
			s = append(s, (*ivItemPLS)(getItem(index)))
		}
		result = s
	default:
		t.Fatalf("Invalid input variety type! %v", ivType)
		return nil
	}

	return result
}

func getPrettyIVMode(ivMode int) string {
	result := "N/A"

	switch ivMode {
	case ivModeDatastore:
		result = "DS"
	case ivModeMemcache:
		result = "MC"
	case ivModeMemcacheAndDatastore:
		result = "DS+MC"
	case ivModeLocalcache:
		result = "LC"
	case ivModeLocalcacheAndMemcache:
		result = "MC+LC"
	case ivModeLocalcacheAndDatastore:
		result = "DS+LC"
	case ivModeLocalcacheAndMemcacheAndDatastore:
		result = "DS+MC+LC"
	}

	return result
}

func getPrettyIVType(ivType int) string {
	result := "N/A"

	switch ivType {
	case ivTypePtrToSliceOfStructs:
		result = "*[]S"
	case ivTypePtrToSliceOfPtrsToStruct:
		result = "*[]*S"
	case ivTypePtrToSliceOfInterfaces:
		result = "*[]I"
	case ivTypeSliceOfStructs:
		result = "[]S"
	case ivTypeSliceOfPtrsToStruct:
		result = "[]*S"
	case ivTypeSliceOfInterfaces:
		result = "[]I"
	case ivTypePtrToSliceOfPLS:
		result = "*[]PLS"
	case ivTypePtrToSliceOfPtrsToPLS:
		result = "*[]*PLS"
	case ivTypePtrToSliceOfInterfacesPLS:
		result = "*[]IPLS"
	case ivTypeSliceOfPLS:
		result = "[]PLS"
	case ivTypeSliceOfPtrsToPLS:
		result = "[]*PLS"
	case ivTypeSliceOfInterfacesPLS:
		result = "[]IPLS"
	}

	return result
}

func isIVTypePLS(ivType int) bool {
	switch ivType {
	case ivTypePtrToSliceOfPLS,
		ivTypePtrToSliceOfPtrsToPLS,
		ivTypePtrToSliceOfInterfacesPLS,
		ivTypeSliceOfPLS,
		ivTypeSliceOfPtrsToPLS,
		ivTypeSliceOfInterfacesPLS:
		return true
	}
	return false
}

// getDiff is a helper function that returns string lines describing the differences between a & b
func getDiff(a, b interface{}, aName, bName string) string {
	var buf bytes.Buffer

	av := reflect.Indirect(reflect.ValueOf(a))
	bv := reflect.Indirect(reflect.ValueOf(b))

	switch av.Kind() {
	case reflect.Slice:
		if av.Len() != bv.Len() {
			buf.WriteString(fmt.Sprintf("%v has len %v, but %v has len %v\n", aName, av.Len(), bName, bv.Len()))
		} else {
			for i := 0; i < av.Len(); i++ {
				avi := av.Index(i).Interface()
				bvi := bv.Index(i).Interface()
				buf.WriteString(getDiff(avi, bvi, fmt.Sprintf("%s[%d]", aName, i), fmt.Sprintf("%s[%d]", bName, i)))
			}
		}
	case reflect.Struct:
		if av.NumField() != bv.NumField() {
			buf.WriteString(fmt.Sprintf("%v has %v fields, but %v has %v fields\n", aName, av.NumField(), bName, bv.NumField()))
		} else {
			for i := 0; i < av.NumField(); i++ {
				avf := av.Field(i)
				bvf := bv.Field(i)

				avft := av.Type().Field(i)
				bvft := bv.Type().Field(i)

				avftName := fmt.Sprintf("%s.%s", aName, avft.Name)
				bvftName := fmt.Sprintf("%s.%s", bName, bvft.Name)

				if avft.Type != bvft.Type {
					buf.WriteString(fmt.Sprintf("%v has type %v, but %v has type %v\n", avftName, avft.Type, bvftName, bvft.Type))
				} else {
					if avft.PkgPath == "" || avft.Anonymous || bvft.PkgPath == "" || bvft.Anonymous {
						buf.WriteString(getDiff(avf.Interface(), bvf.Interface(), avftName, bvftName))
					}
				}
			}
		}
	default:
		if !reflect.DeepEqual(a, b) {
			buf.WriteString(fmt.Sprintf("MISMATCH: %v == %v | %v == %v\n", aName, a, bName, b))
		}
	}

	return buf.String()
}

func onlyErrNoSuchEntity(err error) bool {
	if err == nil {
		return false
	}
	merr, ok := err.(appengine.MultiError)
	if !ok || len(merr) == 0 {
		return false
	}
	for i := 0; i < len(merr); i++ {
		if merr[i] != datastore.ErrNoSuchEntity {
			return false
		}
	}
	return true
}

func ivGetMulti(t *testing.T, g *Goon, ref, dst interface{}, prettyInfo string) error {
	// Get our data back and make sure it's correct
	if err := g.GetMulti(dst); err != nil {
		t.Fatalf("%s > Unexpected error on GetMulti - %v", prettyInfo, err)
		return err
	} else {
		dstLen := reflect.Indirect(reflect.ValueOf(dst)).Len()
		refLen := reflect.Indirect(reflect.ValueOf(ref)).Len()

		if dstLen != refLen {
			t.Fatalf("%s > Unexpected dst len (%v) doesn't match ref len (%v)", prettyInfo, dstLen, refLen)
		} else if !reflect.DeepEqual(ref, dst) {
			t.Fatalf("%s > ivGetMulti didn't return what was expected:\n%s", prettyInfo, getDiff(ref, dst, "ref", "dst"))
		}
	}
	return nil
}

func setPLSCounts(ref interface{}, saveCount, loadCount bool) {
	// Confirm that Save() and Load() are called as specified
	v := reflect.Indirect(reflect.ValueOf(ref))
	for i := 0; i < v.Len(); i++ {
		vi := reflect.Indirect(v.Index(i))
		if vi.Kind() == reflect.Interface {
			vi = reflect.Indirect(vi.Elem())
		}
		if saveCount {
			vi.FieldByName("SaveCount").SetInt(1)
		}
		if loadCount {
			vi.FieldByName("LoadCount").SetInt(1)
		}
	}
}

// This function marks either all or the provided indices of target as dirty.
// It's purpose is to use it on entities fetched via Get,
// and then see if refetching those entities returns the dirty entities.
func makeDirty(target interface{}, indices ...int) {
	if target == nil {
		return
	}
	v := reflect.Indirect(reflect.ValueOf(target))
	for i := 0; i < v.Len(); i++ {
		found := (len(indices) == 0) // If no indices are provided, we dirty everything
		for _, index := range indices {
			if index == i {
				found = true
				break
			}
		}
		if !found {
			continue
		}
		vi := reflect.Indirect(v.Index(i))
		if vi.Kind() == reflect.Interface {
			vi = reflect.Indirect(vi.Elem())
		}
		vi.FieldByName("String").SetString("dirty")
	}
}

func validateInputVariety(t *testing.T, g *Goon, srcType, dstType, mode int, txn bool) {
	if mode >= ivModeTotal {
		t.Fatalf("Invalid input variety mode! %v >= %v", mode, ivModeTotal)
		return
	}

	// Generate a nice debug info string for clear logging
	prettyInfo := getPrettyIVType(srcType) + " " + getPrettyIVType(dstType) + " " + getPrettyIVMode(mode)
	if txn {
		prettyInfo += " TXN"
	}

	// Generate test data with the specified types
	src := getInputVarietyItem(t, g, srcType, false, 0, 1, 2)
	ref := getInputVarietyItem(t, g, dstType, false, 0, 1, 2)
	dstA := getInputVarietyItem(t, g, dstType, true, 0, 1, 2)
	dstB := getInputVarietyItem(t, g, dstType, true, 0, 1, 2)
	dstC := getInputVarietyItem(t, g, dstType, true, 0, 1, 2)

	setPLSCounts(ref, isIVTypePLS(srcType), isIVTypePLS(dstType))

	// Save our test data
	if txn {
		if err := g.RunInTransaction(func(tg *Goon) error {
			_, err := tg.PutMulti(src)
			return err
		}, &datastore.TransactionOptions{XG: true}); err != nil {
			t.Fatalf("%s > Unexpected error on PutMulti - %v", prettyInfo, err)
		}
	} else {
		if _, err := g.PutMulti(src); err != nil {
			t.Fatalf("%s > Unexpected error on PutMulti - %v", prettyInfo, err)
		}
	}

	// Attempt an immediate get, which should catch any faulty Put-based caching
	ivGetMulti(t, g, ref, dstA, prettyInfo+" PC")

	// Clear the caches, as we're going to precisely set the caches via loadIVItem
	// TODO: Instead of clear, fill the caches with invalid data
	g.FlushLocalCache()
	memcache.Flush(g.Context)

	// This function just populates the cache via GetMulti
	loadIVItem := func(indices ...int) {
		dst := getInputVarietyItem(t, g, dstType, true, indices...)
		if err := g.GetMulti(dst); err != nil {
			t.Fatalf("%s > Unexpected error on GetMulti - %v", prettyInfo, err)
		}
		makeDirty(dst) // Make these dirty to confirm the cache doesn't reflect it
	}

	// Set the caches into proper state based on given mode
	switch mode {
	case ivModeDatastore:
		// Caches already clear
	case ivModeMemcache:
		loadIVItem(0, 1, 2) // Left in memcache
		g.FlushLocalCache()
	case ivModeMemcacheAndDatastore:
		loadIVItem(0, 1) // Left in memcache
		g.FlushLocalCache()
	case ivModeLocalcache:
		loadIVItem(0, 1, 2) // Left in local cache
	case ivModeLocalcacheAndMemcache:
		loadIVItem(0) // Left in memcache
		g.FlushLocalCache()
		loadIVItem(1, 2) // Left in local cache
	case ivModeLocalcacheAndDatastore:
		loadIVItem(0, 1) // Left in local cache
	case ivModeLocalcacheAndMemcacheAndDatastore:
		loadIVItem(0) // Left in memcache
		g.FlushLocalCache()
		loadIVItem(1) // Left in local cache
	}

	// Get our data back and make sure it's correct
	if txn {
		if err := g.RunInTransaction(func(tg *Goon) error {
			return ivGetMulti(t, tg, ref, dstB, prettyInfo+" GC")
		}, &datastore.TransactionOptions{XG: true}); err != nil {
			t.Fatalf("%s > Unexpected error on transaction - %v", prettyInfo, err)
		}
	} else {
		ivGetMulti(t, g, ref, dstB, prettyInfo+" GC")
	}

	// Delete our data
	if txn {
		if err := g.RunInTransaction(func(tg *Goon) error {
			return tg.DeleteMulti(ivItemKeys)
		}, &datastore.TransactionOptions{XG: true}); err != nil {
			t.Fatalf("%s > Unexpected error on DeleteMulti - %v", prettyInfo, err)
		}
	} else {
		if err := g.DeleteMulti(ivItemKeys); err != nil {
			t.Fatalf("%s > Unexpected error on DeleteMulti - %v", prettyInfo, err)
		}
	}

	// Make sure our data isn't retrievable from any layer
	if err := g.GetMulti(dstC); !onlyErrNoSuchEntity(err) {
		t.Fatalf("%s > Expected ErrNoSuchEntity but got %v", prettyInfo, err)
	}

	// Do final clean-up of any negative cache
	g.FlushLocalCache()
	memcache.Flush(g.Context)
}

func TestInputVariety(t *testing.T) {
	c, done, err := aetest.NewContext()
	if err != nil {
		t.Fatalf("Could not start aetest - %v", err)
	}
	defer done()
	g := FromContext(c)

	initializeIvItems(c)

	for srcType := 0; srcType < ivTypeTotal; srcType++ {
		for dstType := 0; dstType < ivTypeTotal; dstType++ {
			for mode := 0; mode < ivModeTotal; mode++ {
				for txn := 0; txn < 2; txn++ {
					validateInputVariety(t, g, srcType, dstType, mode, txn == 1)
				}
			}
		}
	}
}

func TestSerialization(t *testing.T) {
	c, done, err := aetest.NewContext()
	if err != nil {
		t.Fatalf("Could not start aetest - %v", err)
	}
	defer done()

	initializeIvItems(c)

	// Test that size is the same in back-to-back
	iviOut := ivItems[0].clone()
	iviOut.SaveCount = 1 // Set it to non-zero so that serialized size will match PLS
	data, err := serializeStruct(iviOut)
	if err != nil {
		t.Fatalf("Failed to serialize iviOut: %v", err)
	}
	dataB, err := serializeStruct(iviOut)
	if err != nil {
		t.Fatalf("Failed to serialize iviOut: %v", err)
	}
	if len(data) != len(dataB) {
		t.Fatalf("Back-to-back serialization returned different length data: %v != %v", len(data), len(dataB))
	}

	// Test that we can deserialize back to the struct
	iviIn := &ivItem{}
	err = deserializeStruct(iviIn, data)
	if err != nil {
		t.Fatalf("Failed to deserialize to iviIn: %v", err)
	}
	iviIn.Id = iviOut.Id // Manually set the id
	if !reflect.DeepEqual(iviOut, iviIn) {
		t.Errorf("Invalid result!\n%v", getDiff(iviOut, iviIn, "iviOut", "iviIn"))
	}

	// PropertyLoadSaver serialization
	var iviplsOut *ivItemPLS
	iviplsOut = (*ivItemPLS)(ivItems[0].clone())
	dataPLS, err := serializeStruct(iviplsOut)
	if err != nil {
		t.Fatalf("Failed to serialize iviplsOut: %v", err)
	}
	iviplsIn := &ivItemPLS{}
	err = deserializeStruct(iviplsIn, dataPLS)
	if err != nil {
		t.Fatalf("Failed to deserialize to iviplsIn: %v", err)
	}
	iviplsIn.Id = iviplsOut.Id               // Manually set the id
	iviplsIn.LoadCount = iviplsOut.LoadCount // Reset the load count
	if !reflect.DeepEqual(iviplsOut, iviplsIn) {
		t.Errorf("Invalid PLS result!\n%v", getDiff(iviplsOut, iviplsIn, "iviplsOut", "iviplsIn"))

	}

	// Make sure both normal & PLS result in the same length data
	if len(data) != len(dataPLS) {
		t.Fatalf("Serialization returned different length data for normal vs PLS: %v != %v", len(data), len(dataPLS))
	}

	t.Logf("data size: %v", len(data))

	// Test that the retrieved data is stable
	s1, s2 := &HasId{Id: 1, Name: "qqq"}, &HasId{Id: 2, Name: "zzzz"}
	d1, err := serializeStruct(s1)
	if err != nil {
		t.Fatalf("Failed to serialize: %v", err)
	}
	d1Copy := make([]byte, len(d1))
	copy(d1Copy, d1)
	_, err = serializeStruct(s2)
	if err != nil {
		t.Fatalf("Failed to serialize: %v", err)
	}
	if !bytes.Equal(d1, d1Copy) {
		t.Fatalf("Serialization bytes are not stable! Expected %x but got %x", d1Copy, d1)
	}
}

type dummyPLS struct {
	Id     int64  `datastore:"-" goon:"id"`
	ValueA string `datastore:"a"`
	ValueB string `datastore:"-"`
}

func (d *dummyPLS) Save() ([]datastore.Property, error) {
	props, err := datastore.SaveStruct(d)
	if err != nil {
		return nil, err
	}
	props = append([]datastore.Property{{Name: "ValueB" + d.ValueB, NoIndex: true, Multiple: true, Value: nil}}, props...)
	return props, nil
}

func (d *dummyPLS) Load(props []datastore.Property) error {
	for _, prop := range props {
		if strings.HasPrefix(prop.Name, "ValueB") && prop.NoIndex && prop.Multiple && prop.Value == nil {
			d.ValueB = prop.Name[len("ValueB"):]
			break
		}
	}
	return datastore.LoadStruct(d, props)
}

// Tests that only matter for PLS and can't be done via ivItem
func TestPropertyLoadSaver(t *testing.T) {
	c, done, err := aetest.NewContext()
	if err != nil {
		t.Fatalf("Could not start aetest - %v", err)
	}
	defer done()
	g := FromContext(c)

	// Save the entity
	dA := &dummyPLS{Id: 1, ValueA: "one", ValueB: "two"}
	if _, err := g.Put(dA); err != nil {
		t.Fatalf("Unexpected error on Put: %v", err)
	}

	for i := 0; i < 5; i++ {
		switch i {
		case 0:
			// Test immediately after Put, leave caches as is
		case 1:
			// Clear the local cache to test memcache
			g.FlushLocalCache()
		case 2:
			// Clear both caches to test datastore
			g.FlushLocalCache()
			memcache.Flush(g.Context)
		case 3:
			// Test local cache from the Get
		case 4:
			// Clear the local cache to test memcache from the Get
			g.FlushLocalCache()
		}

		dB := &dummyPLS{Id: dA.Id}
		if err := g.Get(dB); err != nil {
			t.Fatalf("Unexpected error on Get #%d: %v", i, err)
		}
		if !reflect.DeepEqual(dA, dB) {
			t.Errorf("dA & dB don't match #%d!\n%s", i, getDiff(dA, dB, "dA", "dB"))
		}
	}
}

type MigrationEntity interface {
	number() int32
	parent() *datastore.Key
}

type MigrationA struct {
	_kind            string            `goon:"kind,Migration"`
	Parent           *datastore.Key    `datastore:"-" goon:"parent"`
	Id               int64             `datastore:"-" goon:"id"`
	Number           int32             `datastore:"number"`
	Word             string            `datastore:"word,noindex"`
	Car              string            `datastore:"car,noindex"`
	Holiday          time.Time         `datastore:"holiday,noindex"`
	α                int               `datastore:",noindex"`
	Level            MigrationIntA     `datastore:"level,noindex"`
	Floor            MigrationIntA     `datastore:"floor,noindex"`
	BunchOfBytes     MigrationBSSA     `datastore:"bb,noindex"`
	Sub              MigrationSub      `datastore:"sub,noindex"`
	Son              MigrationPerson   `datastore:"son,noindex"`
	Daughter         MigrationPerson   `datastore:"daughter,noindex"`
	Parents          []MigrationPerson `datastore:"parents,noindex"`
	DeepSlice        MigrationDeepA    `datastore:"deep,noindex"`
	ZZs              []ZigZag          `datastore:"zigzag,noindex"`
	ZeroKey          *datastore.Key    `datastore:",noindex"`
	File             []byte
	LoadCount        int
	DeprecatedField  string       `datastore:"depf,noindex"`
	DeprecatedStruct MigrationSub `datastore:"deps,noindex"`
	FinalField       string       `datastore:"final,noindex"` // This should always be last, to test deprecating middle properties
}

func (m MigrationA) parent() *datastore.Key {
	return m.Parent
}

func (m MigrationA) number() int32 {
	return m.Number
}

type MigrationSub struct {
	Data  string          `datastore:"data,noindex"`
	Noise []int           `datastore:"noise,noindex"`
	Sub   MigrationSubSub `datastore:"sub,noindex"`
}

type MigrationSubSub struct {
	Data string `datastore:"data,noindex"`
}

type MigrationPerson struct {
	Name string `datastore:"name,noindex"`
	Age  int    `datastore:"age,noindex"`
}

type MigrationDeepA struct {
	Deep MigrationDeepB `datastore:"deep,noindex"`
}

type MigrationDeepB struct {
	Deep MigrationDeepC `datastore:"deep,noindex"`
}

type MigrationDeepC struct {
	Slice []int `datastore:"slice,noindex"`
}

type ZigZag struct {
	Zig int `datastore:"zig,noindex"`
	Zag int `datastore:"zag,noindex"`
}

type ZigZags struct {
	Zig []int `datastore:"zig,noindex"`
	Zag []int `datastore:"zag,noindex"`
}

type MigrationIntA int
type MigrationIntB int

type MigrationBSA []byte
type MigrationBSB []byte
type MigrationBSSA []MigrationBSA
type MigrationBSSB []MigrationBSB

type MigrationB struct {
	_kind          string            `goon:"kind,Migration"`
	Parent         *datastore.Key    `datastore:"-" goon:"parent"`
	Identification int64             `datastore:"-" goon:"id"`
	FancyNumber    int32             `datastore:"number"`
	Slang          string            `datastore:"word,noindex"`
	Cars           []string          `datastore:"car,noindex"`
	Holidays       []time.Time       `datastore:"holiday,noindex"`
	β              int               `datastore:"α,noindex"`
	Level          MigrationIntB     `datastore:"level,noindex"`
	Floors         []MigrationIntB   `datastore:"floor,noindex"`
	BunchOfBytes   MigrationBSSB     `datastore:"bb,noindex"`
	Animal         string            `datastore:"sub.data,noindex"`
	Music          []int             `datastore:"sub.noise,noindex"`
	Flower         string            `datastore:"sub.sub.data,noindex"`
	Sons           []MigrationPerson `datastore:"son,noindex"`
	DaughterName   string            `datastore:"daughter.name,noindex"`
	DaughterAge    int               `datastore:"daughter.age,noindex"`
	OldFolks       []MigrationPerson `datastore:"parents,noindex"`
	FarSlice       MigrationDeepA    `datastore:"deep,noindex"`
	ZZs            ZigZags           `datastore:"zigzag,noindex"`
	Keys           []*datastore.Key  `datastore:"ZeroKey,noindex"`
	Files          [][]byte          `datastore:"File,noindex"`
	LoadCount      int               `datastore:"LoadCount,noindex"`
	FinalField     string            `datastore:"final,noindex"`
}

func (m MigrationB) parent() *datastore.Key {
	return m.Parent
}

func (m MigrationB) number() int32 {
	return m.FancyNumber
}

// MigrationA with PropertyLoadSaver interface
type MigrationPlsA MigrationA

// MigrationB with PropertyLoadSaver interface
type MigrationPlsB MigrationB

func (m MigrationPlsA) parent() *datastore.Key {
	return m.Parent
}
func (m MigrationPlsA) number() int32 {
	return m.Number
}
func (m *MigrationPlsA) Save() ([]datastore.Property, error) {
	return datastore.SaveStruct(m)
}
func (m *MigrationPlsA) Load(props []datastore.Property) error {
	err := datastore.LoadStruct(m, props)
	m.LoadCount++
	return err
}

func (m MigrationPlsB) parent() *datastore.Key {
	return m.Parent
}
func (m MigrationPlsB) number() int32 {
	return m.FancyNumber
}
func (m *MigrationPlsB) Save() ([]datastore.Property, error) {
	return datastore.SaveStruct(m)
}
func (m *MigrationPlsB) Load(props []datastore.Property) error {
	err := datastore.LoadStruct(m, props)
	m.LoadCount++
	return err
}

// Make sure these implement datastore.PropertyLoadSaver
var _, _ datastore.PropertyLoadSaver = &MigrationPlsA{}, &MigrationPlsB{}

const (
	migrationMethodGet = iota
	migrationMethodGetAll
	migrationMethodGetAllMulti
	migrationMethodNext
	migrationMethodCount
)

func TestMigration(t *testing.T) {
	c, done, err := aetest.NewContext()
	if err != nil {
		t.Fatalf("Could not start aetest - %v", err)
	}
	defer done()

	origIFM := IgnoreFieldMismatch
	defer func() {
		IgnoreFieldMismatch = origIFM
	}()

	g := FromContext(c)

	// Create & save an entity with the original structure
	parentKey := g.Key(&HasId{Id: 9999})
	migA := &MigrationA{Parent: parentKey, Id: 1, Number: 123, Word: "rabbit", Car: "BMW",
		Holiday: time.Now().UTC().Truncate(time.Microsecond), α: 1, Level: 9001, Floor: 5,
		BunchOfBytes: MigrationBSSA{MigrationBSA{0x01, 0x02}, MigrationBSA{0x03, 0x04}},
		Sub:          MigrationSub{Data: "fox", Noise: []int{1, 2, 3}, Sub: MigrationSubSub{Data: "rose"}},
		Son:          MigrationPerson{Name: "John", Age: 5}, Daughter: MigrationPerson{Name: "Nancy", Age: 6},
		Parents:   []MigrationPerson{{Name: "Sven", Age: 56}, {Name: "Sonya", Age: 49}},
		DeepSlice: MigrationDeepA{Deep: MigrationDeepB{Deep: MigrationDeepC{Slice: []int{1, 2, 3}}}},
		ZZs:       []ZigZag{{Zig: 1}, {Zag: 1}}, File: []byte{0xF0, 0x0D},
		DeprecatedField: "dep", DeprecatedStruct: MigrationSub{Data: "dep", Noise: []int{1, 2, 3}}, FinalField: "fin"}
	if _, err := g.Put(migA); err != nil {
		t.Fatalf("Unexpected error on Put: %v", err)
	}
	// Also save an already migrated structure
	migB := &MigrationB{Parent: migA.Parent, Identification: migA.Id + 1, FancyNumber: migA.Number + 1}
	if _, err := g.Put(migB); err != nil {
		t.Fatalf("Unexpected error on Put: %v", err)
	}

	// Run migration tests with both IgnoreFieldMismatch on & off
	for _, IgnoreFieldMismatch = range []bool{true, false} {
		testcase := []struct {
			name string
			src  MigrationEntity
			dst  MigrationEntity
		}{
			{
				name: "NormalCache -> NormalCache",
				src:  &MigrationA{Parent: parentKey, Id: 1},
				dst:  &MigrationB{Parent: parentKey, Identification: 1},
			},
			{
				name: "PropertyListCache -> NormalCache",
				src:  &MigrationPlsA{Parent: parentKey, Id: 1},
				dst:  &MigrationB{Parent: parentKey, Identification: 1},
			},
			{
				name: "PropertyListCache -> PropertyListCache",
				src:  &MigrationPlsA{Parent: parentKey, Id: 1},
				dst:  &MigrationPlsB{Parent: parentKey, Identification: 1},
			},
			{
				name: "NormalCache -> PropertyListCache",
				src:  &MigrationA{Parent: parentKey, Id: 1},
				dst:  &MigrationPlsB{Parent: parentKey, Identification: 1},
			},
		}
		for _, tt := range testcase {
			// Clear all the caches
			g.FlushLocalCache()
			memcache.Flush(c)

			// Get it back, so it's in the cache
			if err := g.Get(tt.src); err != nil {
				t.Fatalf("Unexpected error on Get: %v", err)
			}

			// Test whether local cache supports migration
			var fetched MigrationEntity
			debugInfo := fmt.Sprintf("%s LC-%v", tt.name, IgnoreFieldMismatch)
			fetched = verifyMigration(t, g, tt.src, tt.dst, migrationMethodGet, debugInfo)
			checkMigrationResult(t, g, tt.src, fetched, debugInfo)

			// Clear the local cache, to test memcache
			g.FlushLocalCache()

			// Test whether memcache supports migration
			debugInfo = fmt.Sprintf("%s MC-%v", tt.name, IgnoreFieldMismatch)
			fetched = verifyMigration(t, g, tt.src, tt.dst, migrationMethodGet, debugInfo)
			checkMigrationResult(t, g, tt.src, fetched, debugInfo)

			// Test whether datastore supports migration
			for method := 0; method < migrationMethodCount; method++ {
				// Test both inside a transaction and outside
				for tx := 0; tx < 2; tx++ {
					// Clear all the caches
					g.FlushLocalCache()
					memcache.Flush(c)

					debugInfo := fmt.Sprintf("%s DS-%v-%v-%v", tt.name, tx, method, IgnoreFieldMismatch)
					if tx == 1 {
						if err := g.RunInTransaction(func(tg *Goon) error {
							fetched = verifyMigration(t, tg, tt.src, tt.dst, method, debugInfo)
							return nil
						}, &datastore.TransactionOptions{XG: false}); err != nil {
							t.Fatalf("Unexpected error with TXN - %v", err)
						}
					} else {
						fetched = verifyMigration(t, g, tt.src, tt.dst, method, debugInfo)
					}
					checkMigrationResult(t, g, tt.src, fetched, debugInfo)
				}
			}
		}
	}
}

func verifyMigration(t *testing.T, g *Goon, src, dst MigrationEntity, method int, debugInfo string) (fetched MigrationEntity) {
	sliceType := reflect.SliceOf(reflect.TypeOf(dst))
	slicePtr := reflect.New(sliceType)
	slicePtr.Elem().Set(reflect.MakeSlice(sliceType, 0, 10))
	slice := slicePtr.Interface()
	sliceVal := slicePtr.Elem()

	switch method {
	case migrationMethodGet:
		if err := g.Get(dst); err != nil && (IgnoreFieldMismatch || !errFieldMismatch(err)) {
			t.Fatalf("%v > Unexpected error on Get: %v", debugInfo, err)
			return
		}
		return dst
	case migrationMethodGetAll:
		if _, err := g.GetAll(datastore.NewQuery("Migration").Ancestor(src.parent()).Filter("number=", src.number()), slice); err != nil && (IgnoreFieldMismatch || !errFieldMismatch(err)) {
			t.Fatalf("%v > Unexpected error on GetAll: %v", debugInfo, err)
			return
		} else if sliceVal.Len() != 1 {
			t.Fatalf("%v > Unexpected query result, expected %v entities, got %v", debugInfo, 1, sliceVal.Len())
			return
		}
		return sliceVal.Index(0).Interface().(MigrationEntity)
	case migrationMethodGetAllMulti:
		// Get both Migration entities
		if _, err := g.GetAll(datastore.NewQuery("Migration").Ancestor(src.parent()).Order("number"), slice); err != nil && (IgnoreFieldMismatch || !errFieldMismatch(err)) {
			t.Fatalf("%v > Unexpected error on GetAll: %v", debugInfo, err)
			return
		} else if sliceVal.Len() != 2 {
			t.Fatalf("%v > Unexpected query result, expected %v entities, got %v", debugInfo, 2, sliceVal.Len())
			return
		}
		return sliceVal.Index(0).Interface().(MigrationEntity)
	case migrationMethodNext:
		it := g.Run(datastore.NewQuery("Migration").Ancestor(src.parent()).Filter("number=", src.number()))
		if _, err := it.Next(dst); err != nil && (IgnoreFieldMismatch || !errFieldMismatch(err)) {
			t.Fatalf("%v > Unexpected error on Next: %v", debugInfo, err)
			return
		}
		// Make sure the iterator ends correctly
		if _, err := it.Next(dst); err != datastore.Done {
			t.Fatalf("Next: expected iterator to end with the error datastore.Done, got %v", err)
		}
		return dst
	}
	return nil
}

func checkMigrationResult(t *testing.T, g *Goon, src, fetched interface{}, debugInfo string) {
	var migA *MigrationA
	switch v := src.(type) {
	case *MigrationA:
		migA = v
	case *MigrationPlsA:
		migA = (*MigrationA)(v)
	}
	var migB *MigrationB
	switch v := fetched.(type) {
	case *MigrationB:
		migB = v
	case *MigrationPlsB:
		migB = (*MigrationB)(v)

		if migB.LoadCount != 1 {
			t.Errorf("%v > Expected LoadCount 1 but got %d", debugInfo, migB.LoadCount)
		}
	}

	if migA.Id != migB.Identification {
		t.Errorf("%v > Ids don't match: %v != %v", debugInfo, migA.Id, migB.Identification)
	} else if migA.Number != migB.FancyNumber {
		t.Errorf("%v > Numbers don't match: %v != %v", debugInfo, migA.Number, migB.FancyNumber)
	} else if migA.Word != migB.Slang {
		t.Errorf("%v > Words don't match: %v != %v", debugInfo, migA.Word, migB.Slang)
	} else if len(migB.Cars) != 1 {
		t.Fatalf("%v > Expected 1 car! Got: %v", debugInfo, len(migB.Cars))
	} else if migA.Car != migB.Cars[0] {
		t.Errorf("%v > Cars don't match: %v != %v", debugInfo, migA.Car, migB.Cars[0])
	} else if len(migB.Holidays) != 1 {
		t.Fatalf("%v > Expected 1 holiday! Got: %v", debugInfo, len(migB.Holidays))
	} else if migA.Holiday != migB.Holidays[0] {
		t.Errorf("%v > Holidays don't match: %v != %v", debugInfo, migA.Holiday, migB.Holidays[0])
	} else if migA.α != migB.β {
		t.Errorf("%v > Greek doesn't match: %v != %v", debugInfo, migA.α, migB.β)
	} else if int(migA.Level) != int(migB.Level) {
		t.Errorf("%v > Level doesn't match: %v != %v", debugInfo, migA.Level, migB.Level)
	} else if len(migB.Floors) != 1 {
		t.Fatalf("%v > Expected 1 floor! Got: %v", debugInfo, len(migB.Floors))
	} else if int(migA.Floor) != int(migB.Floors[0]) {
		t.Errorf("%v > Floor doesn't match: %v != %v", debugInfo, migA.Floor, migB.Floors[0])
	} else if len(migA.BunchOfBytes) != len(migB.BunchOfBytes) || len(migA.BunchOfBytes) != 2 {
		t.Fatalf("%v > BunchOfBytes len doesn't match (expected 2): %v != %v", debugInfo, len(migA.BunchOfBytes), len(migB.BunchOfBytes))
	} else if !reflect.DeepEqual([]byte(migA.BunchOfBytes[0]), []byte(migB.BunchOfBytes[0])) ||
		!reflect.DeepEqual([]byte(migA.BunchOfBytes[1]), []byte(migB.BunchOfBytes[1])) {
		t.Errorf("%v > BunchOfBytes doesn't match: %+v != %+v", debugInfo, migA.BunchOfBytes, migB.BunchOfBytes)
	} else if migA.Sub.Data != migB.Animal {
		t.Errorf("%v > Animal doesn't match: %v != %v", debugInfo, migA.Sub.Data, migB.Animal)
	} else if !reflect.DeepEqual(migA.Sub.Noise, migB.Music) {
		t.Errorf("%v > Music doesn't match: %v != %v", debugInfo, migA.Sub.Noise, migB.Music)
	} else if migA.Sub.Sub.Data != migB.Flower {
		t.Errorf("%v > Flower doesn't match: %v != %v", debugInfo, migA.Sub.Sub.Data, migB.Flower)
	} else if len(migB.Sons) != 1 {
		t.Fatalf("%v > Expected 1 son! Got: %v", debugInfo, len(migB.Sons))
	} else if migA.Son.Name != migB.Sons[0].Name {
		t.Errorf("%v > Son names don't match: %v != %v", debugInfo, migA.Son.Name, migB.Sons[0].Name)
	} else if migA.Son.Age != migB.Sons[0].Age {
		t.Errorf("%v > Son ages don't match: %v != %v", debugInfo, migA.Son.Age, migB.Sons[0].Age)
	} else if migA.Daughter.Name != migB.DaughterName {
		t.Errorf("%v > Daughter names don't match: %v != %v", debugInfo, migA.Daughter.Name, migB.DaughterName)
	} else if migA.Daughter.Age != migB.DaughterAge {
		t.Errorf("%v > Daughter ages don't match: %v != %v", debugInfo, migA.Daughter.Age, migB.DaughterAge)
	} else if !reflect.DeepEqual(migA.Parents, migB.OldFolks) {
		t.Errorf("%v > Parents don't match: %v != %v", debugInfo, migA.Parents, migB.OldFolks)
	} else if !reflect.DeepEqual(migA.DeepSlice, migB.FarSlice) {
		t.Errorf("%v > Deep slice doesn't match: %v != %v", debugInfo, migA.DeepSlice, migB.FarSlice)
	} else if len(migB.ZZs.Zig) != 2 {
		t.Fatalf("%v > Expected 2 Zigs, got: %v", debugInfo, len(migB.ZZs.Zig))
	} else if len(migB.ZZs.Zag) != 2 {
		t.Fatalf("%v > Expected 2 Zags, got: %v", debugInfo, len(migB.ZZs.Zag))
	} else if migA.ZZs[0].Zig != migB.ZZs.Zig[0] {
		t.Errorf("%v > Invalid zig #1: %v != %v", debugInfo, migA.ZZs[0].Zig, migB.ZZs.Zig[0])
	} else if migA.ZZs[1].Zig != migB.ZZs.Zig[1] {
		t.Errorf("%v > Invalid zig #2: %v != %v", debugInfo, migA.ZZs[1].Zig, migB.ZZs.Zig[1])
	} else if migA.ZZs[0].Zag != migB.ZZs.Zag[0] {
		t.Errorf("%v > Invalid zag #1: %v != %v", debugInfo, migA.ZZs[0].Zag, migB.ZZs.Zag[0])
	} else if migA.ZZs[1].Zag != migB.ZZs.Zag[1] {
		t.Errorf("%v > Invalid zag #2: %v != %v", debugInfo, migA.ZZs[1].Zag, migB.ZZs.Zag[1])
	} else if len(migB.Keys) != 1 {
		t.Fatalf("%v > Expected 1 keys, got %v", debugInfo, len(migB.Keys))
	} else if len(migB.Files) != 1 {
		t.Fatalf("%v > Expected 1 file, got %v", debugInfo, len(migB.Files))
	} else if !reflect.DeepEqual(migA.File, migB.Files[0]) {
		t.Errorf("%v > Files don't match: %v != %v", debugInfo, migA.File, migB.Files[0])
	} else if migA.FinalField != migB.FinalField {
		t.Errorf("%v > FinalField doesn't match: %v != %v", debugInfo, migA.FinalField, migB.FinalField)
	}
}

func TestTXNRace(t *testing.T) {
	c, done, err := aetest.NewContext()
	if err != nil {
		t.Fatalf("Could not start aetest - %v", err)
	}
	defer done()
	g := FromContext(c)

	// Create & store some test data
	hid := &HasId{Id: 1, Name: "foo"}
	if _, err := g.Put(hid); err != nil {
		t.Fatalf("Unexpected error on Put %v", err)
	}

	// Make sure the local cache is empty
	g.FlushLocalCache()

	// Get this data back, to populate caches
	if err := g.Get(hid); err != nil {
		t.Fatalf("Unexpected error on Get %v", err)
	}

	// Clear the local cache, as we are testing for proper memcache usage
	g.FlushLocalCache()

	// Update the test data inside a transction
	if err := g.RunInTransaction(func(tg *Goon) error {
		// Get the current data
		thid := &HasId{Id: 1}
		if err := tg.Get(thid); err != nil {
			t.Fatalf("Unexpected error on TXN Get %v", err)
			return err
		}

		// Update the data
		thid.Name = "bar"
		if _, err := tg.Put(thid); err != nil {
			t.Fatalf("Unexpected error on TXN Put %v", err)
			return err
		}

		// Concurrent request emulation
		//   We are running this inside the transaction block to always get the correct timing for testing.
		//   In the real world, this concurrent request may run in another instance.
		//   The transaction block may contain multiple other operations after the preceding Put(),
		//   allowing for ample time for the concurrent request to run before the transaction is committed.
		hid = &HasId{Id: 1}
		if err := g.Get(hid); err != nil {
			t.Fatalf("Unexpected error on Get %v", err)
		} else if hid.Name != "foo" {
			t.Fatalf("Expected 'foo', got %v", hid.Name)
		}

		// Commit the transaction
		return nil
	}, &datastore.TransactionOptions{XG: false}); err != nil {
		t.Fatalf("Unexpected error with TXN - %v", err)
	}

	// Clear the local cache, as we are testing for proper memcache usage
	g.FlushLocalCache()

	// Get the data back again, to confirm it was changed in the transaction
	hid = &HasId{Id: 1}
	if err := g.Get(hid); err != nil {
		t.Fatalf("Unexpected error on Get %v", err)
	} else if hid.Name != "bar" {
		t.Fatalf("Expected 'bar', got %v", hid.Name)
	}

	// Clear the local cache, as we are testing for proper memcache usage
	g.FlushLocalCache()

	// Delete the test data inside a transction
	if err := g.RunInTransaction(func(tg *Goon) error {
		thid := &HasId{Id: 1}
		if err := tg.Delete(tg.Key(thid)); err != nil {
			t.Fatalf("Unexpected error on TXN Delete %v", err)
			return err
		}

		// Concurrent request emulation
		hid = &HasId{Id: 1}
		if err := g.Get(hid); err != nil {
			t.Fatalf("Unexpected error on Get %v", err)
		} else if hid.Name != "bar" {
			t.Fatalf("Expected 'bar', got %v", hid.Name)
		}

		// Commit the transaction
		return nil
	}, &datastore.TransactionOptions{XG: false}); err != nil {
		t.Fatalf("Unexpected error with TXN - %v", err)
	}

	// Clear the local cache, as we are testing for proper memcache usage
	g.FlushLocalCache()

	// Attempt to get the data back again, to confirm it was deleted in the transaction
	hid = &HasId{Id: 1}
	if err := g.Get(hid); err != datastore.ErrNoSuchEntity {
		t.Errorf("Expected ErrNoSuchEntity, got %v", err)
	}
}

func TestNegativeCacheHit(t *testing.T) {
	c, done, err := aetest.NewContext()
	if err != nil {
		t.Fatalf("Could not start aetest - %v", err)
	}
	defer done()
	g := FromContext(c)

	// Run twice to test local cache & memcache
	for _, mode := range []string{"MC", "LC"} {
		debugInfo := fmt.Sprintf("%s", mode)

		hid := &HasId{Id: rand.Int63()}
		if err := g.Get(hid); err != datastore.ErrNoSuchEntity {
			t.Fatalf("%s > Expected ErrNoSuchEntity, got %v", debugInfo, err)
		}

		// Do a sneaky save straight to the datastore
		if _, err := datastore.Put(c, datastore.NewKey(c, "HasId", "", hid.Id, nil), &HasId{Id: hid.Id, Name: "one"}); err != nil {
			t.Fatalf("%s > Unexpected error on datastore.Put: %v", debugInfo, err)
		}

		switch mode {
		case "MC":
			g.FlushLocalCache()
		case "LC":
			memcache.Flush(c)
		}

		// Get the entity again via goon, to make sure we cached the non-existance
		if err := g.Get(hid); err != datastore.ErrNoSuchEntity {
			t.Errorf("%s > Expected ErrNoSuchEntity, got %v", debugInfo, err)
		}
	}
}

func TestNegativeCacheClear(t *testing.T) {
	c, done, err := aetest.NewContext()
	if err != nil {
		t.Fatalf("Could not start aetest - %v", err)
	}
	defer done()
	g := FromContext(c)

	// Run twice to test local cache & memcache
	for _, mode := range []string{"MC", "LC"} {
		for tx := 0; tx < 2; tx++ {
			debugInfo := fmt.Sprintf("%s-%v", mode, tx == 1)

			hid := &HasId{Name: "one"}
			var id int64

			ided := make(chan bool)
			cached := make(chan bool)
			ended := make(chan bool)

			go func() {
				var err error
				if tx == 0 {
					id = rand.Int63()
					hid.Id = id
					ided <- true
					<-cached
					_, err = g.Put(hid)
				} else {
					err = g.RunInTransaction(func(tg *Goon) error {
						_, err := tg.Put(hid)
						id = hid.Id
						ided <- true
						<-cached
						return err
					}, nil)
				}
				if err != nil {
					t.Errorf("%s > Unexpected error on RunInTransaction: %v", debugInfo, err)
				}
				ended <- true
			}()

			// Populate the cache with a negative hit
			{
				<-ided
				negative := &HasId{Id: id}
				if err := g.Get(negative); err != datastore.ErrNoSuchEntity {
					t.Fatalf("%s > Expected ErrNoSuchEntity, got %v", debugInfo, err)
				}
				cached <- true
			}

			// Make sure the negative cache no longer exists
			{
				<-ended
				want := &HasId{Id: id}
				if mode == "MC" {
					g.FlushLocalCache()
				}
				if err := g.Get(want); err != nil {
					t.Fatalf("%s > Unexpected error on get: %v", debugInfo, err)
				}
				if want.Name != hid.Name {
					t.Fatalf("%s > Expected '%v' but got '%v'", debugInfo, hid.Name, want.Name)
				}
			}
		}
	}
}

func TestCaches(t *testing.T) {
	c, done, err := aetest.NewContext()
	if err != nil {
		t.Fatalf("Could not start aetest - %v", err)
	}
	defer done()
	g := FromContext(c)

	// Put *struct{}
	phid := &HasId{Name: "cacheFail"}
	_, err = g.Put(phid)
	if err != nil {
		t.Fatalf("Unexpected error on put - %v", err)
	}

	// Test the scenario where Put would populate local cache

	// fetch *struct{} from cache
	ghid := &HasId{Id: phid.Id}
	err = g.Get(ghid)
	if err != nil {
		t.Fatalf("Unexpected error on get - %v", err)
	}
	if !reflect.DeepEqual(phid, ghid) {
		t.Fatalf("Invalid result!\n%s", getDiff(phid, ghid, "phid", "ghid"))
	}

	// fetch []struct{} from cache
	ghids := []HasId{{Id: phid.Id}}
	err = g.GetMulti(&ghids)
	if err != nil {
		t.Fatalf("Unexpected error on get - %v", err)
	}
	if !reflect.DeepEqual(*phid, ghids[0]) {
		t.Fatalf("Invalid result!\n%s", getDiff(*phid, ghids[0], "*phid", "ghids[0]"))
	}

	// Now flush localcache and fetch them again to test memcache
	g.FlushLocalCache()

	// fetch *struct{} from memcache
	ghid = &HasId{Id: phid.Id}
	err = g.Get(ghid)
	if err != nil {
		t.Fatalf("Unexpected error on get - %v", err)
	}
	if !reflect.DeepEqual(phid, ghid) {
		t.Fatalf("Invalid result!\n%s", getDiff(phid, ghid, "phid", "ghid"))
	}

	// Need to flush local cache again because it was populated by Get
	g.FlushLocalCache()

	// fetch []struct{} from memcache
	ghids = []HasId{{Id: phid.Id}}
	err = g.GetMulti(&ghids)
	if err != nil {
		t.Fatalf("Unexpected error on get - %v", err)
	}
	if !reflect.DeepEqual(*phid, ghids[0]) {
		t.Fatalf("Invalid result!\n%s", getDiff(*phid, ghids[0], "*phid", "ghids[0]"))
	}
}

func TestGoon(t *testing.T) {
	c, done, err := aetest.NewContext()
	if err != nil {
		t.Fatalf("Could not start aetest - %v", err)
	}
	defer done()
	n := FromContext(c)

	// key tests
	noid := NoId{}
	if k, err := n.KeyError(noid); err == nil && !k.Incomplete() {
		t.Fatalf("expected incomplete on noid")
	}
	if n.Key(noid) == nil {
		t.Fatalf("expected to find a key")
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
			t.Fatalf("error: %v", err)
		} else if !k.Equal(kt.key) {
			t.Fatalf("keys not equal")
		}
	}

	if _, err := n.KeyError(TwoId{IntId: 1, StringId: "1"}); err == nil {
		t.Fatalf("expected key error")
	}

	// datastore tests
	keys, _ := datastore.NewQuery("HasId").KeysOnly().GetAll(c, nil)
	datastore.DeleteMulti(c, keys)
	memcache.Flush(c)
	if err := n.Get(&HasId{Id: 0}); err == nil {
		t.Fatalf("ds: expected error, we're fetching from the datastore on an incomplete key!")
	}
	if err := n.Get(&HasId{Id: 1}); err != datastore.ErrNoSuchEntity {
		t.Fatalf("ds: expected no such entity")
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
		t.Fatalf("ds: expected error")
	} else if !NotFound(err, 0) {
		t.Fatalf("ds: not found error 0")
	} else if !NotFound(err, 1) {
		t.Fatalf("ds: not found error 1")
	} else if NotFound(err, 2) {
		t.Fatalf("ds: not found error 2")
	}

	if keys, err := n.PutMulti(es); err != nil {
		t.Fatalf("put: unexpected error")
	} else if len(keys) != len(esk) {
		t.Fatalf("put: got unexpected number of keys")
	} else {
		for i, k := range keys {
			if !k.Equal(esk[i]) {
				t.Fatalf("put: got unexpected keys")
			}
		}
	}
	if err := n.GetMulti(nes); err != nil {
		t.Fatalf("put: unexpected error")
	} else if !reflect.DeepEqual(es, nes) {
		t.Fatalf("put: bad results\n%s", getDiff(es, nes, "es", "nes"))
	} else {
		nesk0 := n.Key(nes[0])
		if !nesk0.Equal(datastore.NewKey(c, "HasId", "", 1, nil)) {
			t.Fatalf("put: bad key")
		}
		nesk1 := n.Key(nes[1])
		if !nesk1.Equal(datastore.NewKey(c, "HasId", "", 2, nil)) {
			t.Fatalf("put: bad key")
		}
	}
	if _, err := n.Put(HasId{Id: 3}); err == nil {
		t.Fatalf("put: expected error")
	}
	// force partial fetch from memcache and then datastore
	memcache.Flush(c)
	if err := n.Get(nes[0]); err != nil {
		t.Fatalf("get: unexpected error")
	}
	if err := n.GetMulti(nes); err != nil {
		t.Fatalf("get: unexpected error")
	}

	// put a HasId resource, then test pulling it from memory, memcache, and datastore
	hi := &HasId{Name: "hasid"} // no id given, should be automatically created by the datastore
	if _, err := n.Put(hi); err != nil {
		t.Fatalf("put: unexpected error - %v", err)
	}
	if n.Key(hi) == nil {
		t.Fatalf("key should not be nil")
	} else if n.Key(hi).Incomplete() {
		t.Fatalf("key should not be incomplete")
	}

	hi2 := &HasId{Id: hi.Id}
	if err := n.Get(hi2); err != nil {
		t.Fatalf("get: unexpected error - %v", err)
	}
	if hi2.Name != hi.Name {
		t.Fatalf("Could not fetch HasId object from memory - %#v != %#v", hi, hi2)
	}

	hi3 := &HasId{Id: hi.Id}
	n.cache.Delete(cacheKey(n.Key(hi)))
	if err := n.Get(hi3); err != nil {
		t.Fatalf("get: unexpected error - %v", err)
	}
	if hi3.Name != hi.Name {
		t.Fatalf("Could not fetch HasId object from memory - %#v != %#v", hi, hi3)
	}

	hi4 := &HasId{Id: hi.Id}
	n.cache.Delete(cacheKey(n.Key(hi4)))
	if memcache.Flush(n.Context) != nil {
		t.Fatalf("Unable to flush memcache")
	}
	if err := n.Get(hi4); err != nil {
		t.Fatalf("get: unexpected error - %v", err)
	}
	if hi4.Name != hi.Name {
		t.Fatalf("Could not fetch HasId object from datastore- %#v != %#v", hi, hi4)
	}

	// Now do the opposite also using hi
	// Test pulling from local cache and memcache when datastore result is different
	// Note that this shouldn't happen with real goon usage,
	//   but this tests that goon isn't still pulling from the datastore (or memcache) unnecessarily
	// hi in datastore Name = hasid
	hiPull := &HasId{Id: hi.Id}
	{
		ckey := cacheKey(n.Key(hi))
		hiTamper := &HasId{Id: hi.Id, Name: "changedincache"}
		data, err := serializeStruct(hiTamper)
		if err != nil {
			t.Fatalf("Unexpected error serializing: %v", err)
		}
		n.cache.Set(&cacheItem{key: ckey, value: data})
	}
	if err := n.Get(hiPull); err != nil {
		t.Fatalf("get: unexpected error - %v", err)
	}
	if hiPull.Name != "changedincache" {
		t.Fatalf("hiPull.Name should be 'changedincache' but got %s", hiPull.Name)
	}
	{
		ckey := cacheKey(n.Key(hi))
		hiPush := &HasId{Id: hi.Id, Name: "changedinmemcache"}
		data, err := serializeStruct(hiPush)
		if err != nil {
			t.Fatalf("Unexpected error serializing: %v", err)
		}
		n.putMemcache([]*cacheItem{{key: ckey, value: data}})
		n.cache.Delete(ckey)
	}

	hiPull = &HasId{Id: hi.Id}
	if err := n.Get(hiPull); err != nil {
		t.Fatalf("get: unexpected error - %v", err)
	}
	if hiPull.Name != "changedinmemcache" {
		t.Fatalf("hiPull.Name should be 'changedinmemcache' but got %s", hiPull.Name)
	}

	// Since the datastore can't assign a key to a String ID, test to make sure goon stops it from happening
	hasString := new(HasString)
	_, err = n.Put(hasString)
	if err == nil {
		t.Fatalf("Cannot put an incomplete string Id object as the datastore will populate an int64 id instead- %v", hasString)
	}
	hasString.Id = "hello"
	_, err = n.Put(hasString)
	if err != nil {
		t.Fatalf("Error putting hasString object - %v", hasString)
	}

	// Test queries!

	// Test that zero result queries work properly
	qiZRes := []QueryItem{}
	if dskeys, err := n.GetAll(datastore.NewQuery("QueryItem"), &qiZRes); err != nil {
		t.Fatalf("GetAll Zero: unexpected error: %v", err)
	} else if len(dskeys) != 0 {
		t.Fatalf("GetAll Zero: expected 0 keys, got %v", len(dskeys))
	}

	createEntities := func() {
		// Create some entities that we will query for
		if getKeys, err := n.PutMulti([]*QueryItem{{Id: 1, Data: "one"}, {Id: 2, Data: "two"}}); err != nil {
			t.Fatalf("PutMulti: unexpected error: %v", err)
		} else {
			// do a datastore Get by *Key so that data is written to the datstore and indexes generated before subsequent query
			if err := datastore.GetMulti(c, getKeys, make([]QueryItem, 2)); err != nil {
				t.Error(err)
			}
		}
		// Make sure we clear the cache, as we will fill it later with a query
		n.FlushLocalCache()
	}
	deleteEntities := func(ids ...int64) {
		if len(ids) == 0 {
			ids = []int64{1, 2}
		}
		var keys []*datastore.Key
		for _, id := range ids {
			keys = append(keys, datastore.NewKey(c, "QueryItem", "", id, nil))
		}
		if err := datastore.DeleteMulti(c, keys); err != nil {
			t.Error(err)
		}
	}

	createEntities()

	// Get the entity using a slice of structs that needs to be appended but has garbage data
	qiSGRes := []QueryItem{{Id: 1, Data: "invalid cache"}, {Garbage: "uninitialized memory"}}[:1]
	if dskeys, err := n.GetAll(datastore.NewQuery("QueryItem").Filter("data=", "two"), &qiSGRes); err != nil {
		t.Fatalf("GetAll SoS: unexpected error: %v", err)
	} else if len(dskeys) != 1 {
		t.Fatalf("GetAll SoS: expected 1 key, got %v", len(dskeys))
	} else if dskeys[0].IntID() != 2 {
		t.Fatalf("GetAll SoS: expected key IntID to be 2, got %v", dskeys[0].IntID())
	} else if len(qiSGRes) != 2 {
		t.Fatalf("GetAll SoS: expected 2 results, got %v", len(qiSGRes))
	} else if qiSGRes[1].Id != 2 {
		t.Fatalf("GetAll SoS: expected entity id to be 2, got %v", qiSGRes[1].Id)
	} else if qiSGRes[1].Data != "two" {
		t.Fatalf("GetAll SoS: expected entity data to be 'two', got '%v'", qiSGRes[1].Data)
	} else if qiSGRes[1].Garbage != "" {
		t.Fatalf("GetAll SoS: expected no garbage data, but got '%v'", qiSGRes[1].Garbage)
	}

	n.FlushLocalCache()

	// Get the entity using a slice of structs
	qiSRes := []QueryItem{}
	if dskeys, err := n.GetAll(datastore.NewQuery("QueryItem").Filter("data=", "one"), &qiSRes); err != nil {
		t.Fatalf("GetAll SoS: unexpected error: %v", err)
	} else if len(dskeys) != 1 {
		t.Fatalf("GetAll SoS: expected 1 key, got %v", len(dskeys))
	} else if dskeys[0].IntID() != 1 {
		t.Fatalf("GetAll SoS: expected key IntID to be 1, got %v", dskeys[0].IntID())
	} else if len(qiSRes) != 1 {
		t.Fatalf("GetAll SoS: expected 1 result, got %v", len(qiSRes))
	} else if qiSRes[0].Id != 1 {
		t.Fatalf("GetAll SoS: expected entity id to be 1, got %v", qiSRes[0].Id)
	} else if qiSRes[0].Data != "one" {
		t.Fatalf("GetAll SoS: expected entity data to be 'one', got '%v'", qiSRes[0].Data)
	}

	deleteEntities()

	// Get the entity using normal Get to test local cache
	qiS := &QueryItem{Id: 1}
	if err := n.Get(qiS); err != nil {
		t.Fatalf("Get SoS: unexpected error: %v", err)
	} else if qiS.Id != 1 {
		t.Fatalf("Get SoS: expected entity id to be 1, got %v", qiS.Id)
	} else if qiS.Data != "one" {
		t.Fatalf("Get SoS: expected entity data to be 'one', got '%v'", qiS.Data)
	}

	createEntities()

	// Get the entity using a slice of pointers to struct
	qiPRes := []*QueryItem{}
	if dskeys, err := n.GetAll(datastore.NewQuery("QueryItem").Filter("data=", "one"), &qiPRes); err != nil {
		t.Fatalf("GetAll SoPtS: unexpected error: %v", err)
	} else if len(dskeys) != 1 {
		t.Fatalf("GetAll SoPtS: expected 1 key, got %v", len(dskeys))
	} else if dskeys[0].IntID() != 1 {
		t.Fatalf("GetAll SoPtS: expected key IntID to be 1, got %v", dskeys[0].IntID())
	} else if len(qiPRes) != 1 {
		t.Fatalf("GetAll SoPtS: expected 1 result, got %v", len(qiPRes))
	} else if qiPRes[0].Id != 1 {
		t.Fatalf("GetAll SoPtS: expected entity id to be 1, got %v", qiPRes[0].Id)
	} else if qiPRes[0].Data != "one" {
		t.Fatalf("GetAll SoPtS: expected entity data to be 'one', got '%v'", qiPRes[0].Data)
	}

	deleteEntities()

	// Get the entity using normal Get to test local cache
	qiP := &QueryItem{Id: 1}
	if err := n.Get(qiP); err != nil {
		t.Fatalf("Get SoPtS: unexpected error: %v", err)
	} else if qiP.Id != 1 {
		t.Fatalf("Get SoPtS: expected entity id to be 1, got %v", qiP.Id)
	} else if qiP.Data != "one" {
		t.Fatalf("Get SoPtS: expected entity data to be 'one', got '%v'", qiP.Data)
	}

	createEntities()

	// Get the entity using an iterator
	qiIt := n.Run(datastore.NewQuery("QueryItem").Filter("data=", "one"))

	qiItRes := &QueryItem{}
	if dskey, err := qiIt.Next(qiItRes); err != nil {
		t.Fatalf("Next: unexpected error: %v", err)
	} else if dskey.IntID() != 1 {
		t.Fatalf("Next: expected key IntID to be 1, got %v", dskey.IntID())
	} else if qiItRes.Id != 1 {
		t.Fatalf("Next: expected entity id to be 1, got %v", qiItRes.Id)
	} else if qiItRes.Data != "one" {
		t.Fatalf("Next: expected entity data to be 'one', got '%v'", qiItRes.Data)
	}

	// Make sure the iterator ends correctly
	if _, err := qiIt.Next(&QueryItem{}); err != datastore.Done {
		t.Fatalf("Next: expected iterator to end with the error datastore.Done, got %v", err)
	}

	deleteEntities()

	// Get the entity using normal Get to test local cache
	qiI := &QueryItem{Id: 1}
	if err := n.Get(qiI); err != nil {
		t.Fatalf("Get Iterator: unexpected error: %v", err)
	} else if qiI.Id != 1 {
		t.Fatalf("Get Iterator: expected entity id to be 1, got %v", qiI.Id)
	} else if qiI.Data != "one" {
		t.Fatalf("Get Iterator: expected entity data to be 'one', got '%v'", qiI.Data)
	}

	createEntities()

	// Get the entity using a non-zero slice of structs, to also test the cache being filled incorrectly
	qiNZSRes := []QueryItem{{Id: 1, Data: "invalid cache"}}
	if dskeys, err := n.GetAll(datastore.NewQuery("QueryItem").Filter("data=", "two"), &qiNZSRes); err != nil {
		t.Fatalf("GetAll NZSoS: unexpected error: %v", err)
	} else if len(dskeys) != 1 {
		t.Fatalf("GetAll NZSoS: expected 1 key, got %v", len(dskeys))
	} else if dskeys[0].IntID() != 2 {
		t.Fatalf("GetAll NZSoS: expected key IntID to be 2, got %v", dskeys[0].IntID())
	} else if len(qiNZSRes) != 2 {
		t.Fatalf("GetAll NZSoS: expected slice len to be 2, got %v", len(qiNZSRes))
	} else if qiNZSRes[0].Id != 1 {
		t.Fatalf("GetAll NZSoS: expected entity id to be 1, got %v", qiNZSRes[0].Id)
	} else if qiNZSRes[0].Data != "invalid cache" {
		t.Fatalf("GetAll NZSoS: expected entity data to be 'invalid cache', got '%v'", qiNZSRes[0].Data)
	} else if qiNZSRes[1].Id != 2 {
		t.Fatalf("GetAll NZSoS: expected entity id to be 2, got %v", qiNZSRes[1].Id)
	} else if qiNZSRes[1].Data != "two" {
		t.Fatalf("GetAll NZSoS: expected entity data to be 'two', got '%v'", qiNZSRes[1].Data)
	}

	deleteEntities(2)

	// Get the entities using normal GetMulti to test local cache
	qiNZSs := []QueryItem{{Id: 1}, {Id: 2}}
	if err := n.GetMulti(qiNZSs); err != nil {
		t.Fatalf("GetMulti NZSoS: unexpected error: %v", err)
	} else if len(qiNZSs) != 2 {
		t.Fatalf("GetMulti NZSoS: expected slice len to be 2, got %v", len(qiNZSs))
	} else if qiNZSs[0].Id != 1 {
		t.Fatalf("GetMulti NZSoS: expected entity id to be 1, got %v", qiNZSs[0].Id)
	} else if qiNZSs[0].Data != "one" {
		t.Fatalf("GetMulti NZSoS: expected entity data to be 'one', got '%v'", qiNZSs[0].Data)
	} else if qiNZSs[1].Id != 2 {
		t.Fatalf("GetMulti NZSoS: expected entity id to be 2, got %v", qiNZSs[1].Id)
	} else if qiNZSs[1].Data != "two" {
		t.Fatalf("GetMulti NZSoS: expected entity data to be 'two', got '%v'", qiNZSs[1].Data)
	}

	createEntities()

	// Get the entity using a non-zero slice of pointers to struct, to also test the cache being filled incorrectly
	qiNZPRes := []*QueryItem{{Id: 1, Data: "invalid cache"}}
	if dskeys, err := n.GetAll(datastore.NewQuery("QueryItem").Filter("data=", "two"), &qiNZPRes); err != nil {
		t.Fatalf("GetAll NZSoPtS: unexpected error: %v", err)
	} else if len(dskeys) != 1 {
		t.Fatalf("GetAll NZSoPtS: expected 1 key, got %v", len(dskeys))
	} else if dskeys[0].IntID() != 2 {
		t.Fatalf("GetAll NZSoPtS: expected key IntID to be 2, got %v", dskeys[0].IntID())
	} else if len(qiNZPRes) != 2 {
		t.Fatalf("GetAll NZSoPtS: expected slice len to be 2, got %v", len(qiNZPRes))
	} else if qiNZPRes[0].Id != 1 {
		t.Fatalf("GetAll NZSoPtS: expected entity id to be 1, got %v", qiNZPRes[0].Id)
	} else if qiNZPRes[0].Data != "invalid cache" {
		t.Fatalf("GetAll NZSoPtS: expected entity data to be 'invalid cache', got '%v'", qiNZPRes[0].Data)
	} else if qiNZPRes[1].Id != 2 {
		t.Fatalf("GetAll NZSoPtS: expected entity id to be 2, got %v", qiNZPRes[1].Id)
	} else if qiNZPRes[1].Data != "two" {
		t.Fatalf("GetAll NZSoPtS: expected entity data to be 'two', got '%v'", qiNZPRes[1].Data)
	}

	deleteEntities(2)

	// Get the entities using normal GetMulti to test local cache
	qiNZPs := []*QueryItem{{Id: 1}, {Id: 2}}
	if err := n.GetMulti(qiNZPs); err != nil {
		t.Fatalf("GetMulti NZSoPtS: unexpected error: %v", err)
	} else if len(qiNZPs) != 2 {
		t.Fatalf("GetMulti NZSoPtS: expected slice len to be 2, got %v", len(qiNZPs))
	} else if qiNZPs[0].Id != 1 {
		t.Fatalf("GetMulti NZSoPtS: expected entity id to be 1, got %v", qiNZPs[0].Id)
	} else if qiNZPs[0].Data != "one" {
		t.Fatalf("GetMulti NZSoPtS: expected entity data to be 'one', got '%v'", qiNZPs[0].Data)
	} else if qiNZPs[1].Id != 2 {
		t.Fatalf("GetMulti NZSoPtS: expected entity id to be 2, got %v", qiNZPs[1].Id)
	} else if qiNZPs[1].Data != "two" {
		t.Fatalf("GetMulti NZSoPtS: expected entity data to be 'two', got '%v'", qiNZPs[1].Data)
	}

	createEntities()

	// Get the entity using a keys-only iterator
	qiItKO := n.Run(datastore.NewQuery("QueryItem").Filter("data=", "one").KeysOnly())

	qiItKORes := &QueryItem{}
	if dskey, err := qiItKO.Next(qiItKORes); err != nil {
		t.Fatalf("Next: unexpected error: %v", err)
	} else if dskey.IntID() != 1 {
		t.Fatalf("Next: expected key IntID to be 1, got %v", dskey.IntID())
	} else if qiItKORes.Id != 1 {
		t.Fatalf("Next: expected entity id to be 1, got %v", qiItKORes.Id)
	} else if qiItKORes.Data != "" {
		t.Fatalf("Next: expected entity data to be empty, got '%v'", qiItKORes.Data)
	}

	// Make sure the iterator ends correctly
	if _, err := qiItKO.Next(&QueryItem{}); err != datastore.Done {
		t.Fatalf("Next: expected iterator to end with the error datastore.Done, got %v", err)
	}

	// Get the entity using normal Get to test local cache
	qiIKO := &QueryItem{Id: 1}
	if err := n.Get(qiIKO); err != nil {
		t.Fatalf("Get Iterator: unexpected error: %v", err)
	} else if qiIKO.Id != 1 {
		t.Fatalf("Get Iterator: expected entity id to be 1, got %v", qiIKO.Id)
	} else if qiIKO.Data != "one" {
		t.Fatalf("Get Iterator: expected entity data to be 'one', got '%v'", qiIKO.Data)
	}

	n.FlushLocalCache()

	// Test the simplest keys-only query, also test the cache not being filled incorrectly by a keys-only query
	if dskeys, err := n.GetAll(datastore.NewQuery("QueryItem").Filter("data=", "one").KeysOnly(), nil); err != nil {
		t.Fatalf("GetAll KeysOnly: unexpected error: %v", err)
	} else if len(dskeys) != 1 {
		t.Fatalf("GetAll KeysOnly: expected 1 key, got %v", len(dskeys))
	} else if dskeys[0].IntID() != 1 {
		t.Fatalf("GetAll KeysOnly: expected key IntID to be 1, got %v", dskeys[0].IntID())
	}

	// Get the entity using normal Get to test that the local cache wasn't filled with incomplete data
	qiKO := &QueryItem{Id: 1}
	if err := n.Get(qiKO); err != nil {
		t.Fatalf("Get KeysOnly: unexpected error: %v", err)
	} else if qiKO.Id != 1 {
		t.Fatalf("Get KeysOnly: expected entity id to be 1, got %v", qiKO.Id)
	} else if qiKO.Data != "one" {
		t.Fatalf("Get KeysOnly: expected entity data to be 'one', got '%v'", qiKO.Data)
	}

	n.FlushLocalCache()

	// Test the keys-only query with slice of structs, also test the cache not being filled incorrectly by a keys-only query
	qiKOSRes := []QueryItem{}
	if dskeys, err := n.GetAll(datastore.NewQuery("QueryItem").Filter("data=", "one").KeysOnly(), &qiKOSRes); err != nil {
		t.Fatalf("GetAll KeysOnly SoS: unexpected error: %v", err)
	} else if len(dskeys) != 1 {
		t.Fatalf("GetAll KeysOnly SoS: expected 1 key, got %v", len(dskeys))
	} else if dskeys[0].IntID() != 1 {
		t.Fatalf("GetAll KeysOnly SoS: expected key IntID to be 1, got %v", dskeys[0].IntID())
	} else if len(qiKOSRes) != 1 {
		t.Fatalf("GetAll KeysOnly SoS: expected 1 result, got %v", len(qiKOSRes))
	} else if k := reflect.TypeOf(qiKOSRes[0]).Kind(); k != reflect.Struct {
		t.Fatalf("GetAll KeysOnly SoS: expected struct, got %v", k)
	} else if qiKOSRes[0].Id != 1 {
		t.Fatalf("GetAll KeysOnly SoS: expected entity id to be 1, got %v", qiKOSRes[0].Id)
	} else if qiKOSRes[0].Data != "" {
		t.Fatalf("GetAll KeysOnly SoS: expected entity data to be empty, got '%v'", qiKOSRes[0].Data)
	}

	// Get the entity using normal Get to test that the local cache wasn't filled with incomplete data
	if err := n.GetMulti(qiKOSRes); err != nil {
		t.Fatalf("Get KeysOnly SoS: unexpected error: %v", err)
	} else if qiKOSRes[0].Id != 1 {
		t.Fatalf("Get KeysOnly SoS: expected entity id to be 1, got %v", qiKOSRes[0].Id)
	} else if qiKOSRes[0].Data != "one" {
		t.Fatalf("Get KeysOnly SoS: expected entity data to be 'one', got '%v'", qiKOSRes[0].Data)
	}

	n.FlushLocalCache()

	// Test the keys-only query with slice of pointers to struct, also test the cache not being filled incorrectly by a keys-only query
	qiKOPRes := []*QueryItem{}
	if dskeys, err := n.GetAll(datastore.NewQuery("QueryItem").Filter("data=", "one").KeysOnly(), &qiKOPRes); err != nil {
		t.Fatalf("GetAll KeysOnly SoPtS: unexpected error: %v", err)
	} else if len(dskeys) != 1 {
		t.Fatalf("GetAll KeysOnly SoPtS: expected 1 key, got %v", len(dskeys))
	} else if dskeys[0].IntID() != 1 {
		t.Fatalf("GetAll KeysOnly SoPtS: expected key IntID to be 1, got %v", dskeys[0].IntID())
	} else if len(qiKOPRes) != 1 {
		t.Fatalf("GetAll KeysOnly SoPtS: expected 1 result, got %v", len(qiKOPRes))
	} else if k := reflect.TypeOf(qiKOPRes[0]).Kind(); k != reflect.Ptr {
		t.Fatalf("GetAll KeysOnly SoPtS: expected pointer, got %v", k)
	} else if qiKOPRes[0].Id != 1 {
		t.Fatalf("GetAll KeysOnly SoPtS: expected entity id to be 1, got %v", qiKOPRes[0].Id)
	} else if qiKOPRes[0].Data != "" {
		t.Fatalf("GetAll KeysOnly SoPtS: expected entity data to be empty, got '%v'", qiKOPRes[0].Data)
	}

	// Get the entity using normal Get to test that the local cache wasn't filled with incomplete data
	if err := n.GetMulti(qiKOPRes); err != nil {
		t.Fatalf("Get KeysOnly SoPtS: unexpected error: %v", err)
	} else if qiKOPRes[0].Id != 1 {
		t.Fatalf("Get KeysOnly SoPtS: expected entity id to be 1, got %v", qiKOPRes[0].Id)
	} else if qiKOPRes[0].Data != "one" {
		t.Fatalf("Get KeysOnly SoPtS: expected entity data to be 'one', got '%v'", qiKOPRes[0].Data)
	}

	n.FlushLocalCache()

	// Test the keys-only query with non-zero slice of structs
	qiKONZSRes := []QueryItem{{Id: 1, Data: "invalid cache"}}
	if dskeys, err := n.GetAll(datastore.NewQuery("QueryItem").Filter("data=", "two").KeysOnly(), &qiKONZSRes); err != nil {
		t.Fatalf("GetAll KeysOnly NZSoS: unexpected error: %v", err)
	} else if len(dskeys) != 1 {
		t.Fatalf("GetAll KeysOnly NZSoS: expected 1 key, got %v", len(dskeys))
	} else if dskeys[0].IntID() != 2 {
		t.Fatalf("GetAll KeysOnly NZSoS: expected key IntID to be 2, got %v", dskeys[0].IntID())
	} else if len(qiKONZSRes) != 2 {
		t.Fatalf("GetAll KeysOnly NZSoS: expected slice len to be 2, got %v", len(qiKONZSRes))
	} else if qiKONZSRes[0].Id != 1 {
		t.Fatalf("GetAll KeysOnly NZSoS: expected entity id to be 1, got %v", qiKONZSRes[0].Id)
	} else if qiKONZSRes[0].Data != "invalid cache" {
		t.Fatalf("GetAll KeysOnly NZSoS: expected entity data to be 'invalid cache', got '%v'", qiKONZSRes[0].Data)
	} else if k := reflect.TypeOf(qiKONZSRes[1]).Kind(); k != reflect.Struct {
		t.Fatalf("GetAll KeysOnly NZSoS: expected struct, got %v", k)
	} else if qiKONZSRes[1].Id != 2 {
		t.Fatalf("GetAll KeysOnly NZSoS: expected entity id to be 2, got %v", qiKONZSRes[1].Id)
	} else if qiKONZSRes[1].Data != "" {
		t.Fatalf("GetAll KeysOnly NZSoS: expected entity data to be empty, got '%v'", qiKONZSRes[1].Data)
	}

	// Get the entities using normal GetMulti to test local cache
	if err := n.GetMulti(qiKONZSRes); err != nil {
		t.Fatalf("GetMulti NZSoS: unexpected error: %v", err)
	} else if len(qiKONZSRes) != 2 {
		t.Fatalf("GetMulti NZSoS: expected slice len to be 2, got %v", len(qiKONZSRes))
	} else if qiKONZSRes[0].Id != 1 {
		t.Fatalf("GetMulti NZSoS: expected entity id to be 1, got %v", qiKONZSRes[0].Id)
	} else if qiKONZSRes[0].Data != "one" {
		t.Fatalf("GetMulti NZSoS: expected entity data to be 'one', got '%v'", qiKONZSRes[0].Data)
	} else if qiKONZSRes[1].Id != 2 {
		t.Fatalf("GetMulti NZSoS: expected entity id to be 2, got %v", qiKONZSRes[1].Id)
	} else if qiKONZSRes[1].Data != "two" {
		t.Fatalf("GetMulti NZSoS: expected entity data to be 'two', got '%v'", qiKONZSRes[1].Data)
	}

	n.FlushLocalCache()

	// Test the keys-only query with non-zero slice of pointers to struct
	qiKONZPRes := []*QueryItem{{Id: 1, Data: "invalid cache"}}
	if dskeys, err := n.GetAll(datastore.NewQuery("QueryItem").Filter("data=", "two").KeysOnly(), &qiKONZPRes); err != nil {
		t.Fatalf("GetAll KeysOnly NZSoPtS: unexpected error: %v", err)
	} else if len(dskeys) != 1 {
		t.Fatalf("GetAll KeysOnly NZSoPtS: expected 1 key, got %v", len(dskeys))
	} else if dskeys[0].IntID() != 2 {
		t.Fatalf("GetAll KeysOnly NZSoPtS: expected key IntID to be 2, got %v", dskeys[0].IntID())
	} else if len(qiKONZPRes) != 2 {
		t.Fatalf("GetAll KeysOnly NZSoPtS: expected slice len to be 2, got %v", len(qiKONZPRes))
	} else if qiKONZPRes[0].Id != 1 {
		t.Fatalf("GetAll KeysOnly NZSoPtS: expected entity id to be 1, got %v", qiKONZPRes[0].Id)
	} else if qiKONZPRes[0].Data != "invalid cache" {
		t.Fatalf("GetAll KeysOnly NZSoPtS: expected entity data to be 'invalid cache', got '%v'", qiKONZPRes[0].Data)
	} else if k := reflect.TypeOf(qiKONZPRes[1]).Kind(); k != reflect.Ptr {
		t.Fatalf("GetAll KeysOnly NZSoPtS: expected pointer, got %v", k)
	} else if qiKONZPRes[1].Id != 2 {
		t.Fatalf("GetAll KeysOnly NZSoPtS: expected entity id to be 2, got %v", qiKONZPRes[1].Id)
	} else if qiKONZPRes[1].Data != "" {
		t.Fatalf("GetAll KeysOnly NZSoPtS: expected entity data to be empty, got '%v'", qiKONZPRes[1].Data)
	}

	// Get the entities using normal GetMulti to test local cache
	if err := n.GetMulti(qiKONZPRes); err != nil {
		t.Fatalf("GetMulti NZSoPtS: unexpected error: %v", err)
	} else if len(qiKONZPRes) != 2 {
		t.Fatalf("GetMulti NZSoPtS: expected slice len to be 2, got %v", len(qiKONZPRes))
	} else if qiKONZPRes[0].Id != 1 {
		t.Fatalf("GetMulti NZSoPtS: expected entity id to be 1, got %v", qiKONZPRes[0].Id)
	} else if qiKONZPRes[0].Data != "one" {
		t.Fatalf("GetMulti NZSoPtS: expected entity data to be 'one', got '%v'", qiKONZPRes[0].Data)
	} else if qiKONZPRes[1].Id != 2 {
		t.Fatalf("GetMulti NZSoPtS: expected entity id to be 2, got %v", qiKONZPRes[1].Id)
	} else if qiKONZPRes[1].Data != "two" {
		t.Fatalf("GetMulti NZSoPtS: expected entity data to be 'two', got '%v'", qiKONZPRes[1].Data)
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
	Id      int64  `datastore:"-" goon:"id"`
	Data    string `datastore:"data"`
	Garbage string `datastore:"-"`
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

type HasData struct {
	Id   int64 `datastore:"-" goon:"id"`
	Data []byte
}

// This test won't fail but if run with -race flag, it will show known race conditions
// Using multiple goroutines per http request is recommended here:
// http://talks.golang.org/2013/highperf.slide#22
func TestRace(t *testing.T) {
	c, done, err := aetest.NewContext()
	if err != nil {
		t.Fatalf("Could not start aetest - %v", err)
	}
	defer done()
	g := FromContext(c)

	var hasIdSlice []*HasId
	for x := 1; x <= 4000; x++ {
		hasIdSlice = append(hasIdSlice, &HasId{Id: int64(x), Name: "Race"})
	}
	_, err = g.PutMulti(hasIdSlice)
	if err != nil {
		t.Fatalf("Could not put Race entities - %v", err)
	}
	hasIdSlice = hasIdSlice[:0]
	for x := 1; x <= 4000; x++ {
		hasIdSlice = append(hasIdSlice, &HasId{Id: int64(x)})
	}
	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		err := g.Get(hasIdSlice[0])
		if err != nil {
			t.Errorf("Error fetching id #0 - %v", err)
		}
		wg.Done()
	}()
	go func() {
		err := g.GetMulti(hasIdSlice[1:1500])
		if err != nil {
			t.Errorf("Error fetching ids 1 through 1499 - %v", err)
		}
		wg.Done()
	}()
	go func() {
		err := g.GetMulti(hasIdSlice[1500:])
		if err != nil {
			t.Errorf("Error fetching id #1500 through 4000 - %v", err)
		}
		wg.Done()
	}()
	wg.Wait()
	for x, hi := range hasIdSlice {
		if hi.Name != "Race" {
			t.Fatalf("Object #%d not fetched properly, fetched instead - %v", x, hi)
		}
	}

	// in case of datastore failure
	errInternalCall := errors.New("internal call error")
	withErrorContext := func(ctx context.Context, multiLimit int) context.Context {
		return appengine.WithAPICallFunc(ctx, func(ctx context.Context, service, method string, in, out proto.Message) error {
			if service != "datastore_v3" {
				return nil
			}
			if method != "Put" && method != "Get" && method != "Delete" {
				return nil
			}
			errs := make(appengine.MultiError, multiLimit)
			for x := 0; x < multiLimit; x++ {
				errs[x] = errInternalCall
			}
			return errs
		})
	}

	g.Context = withErrorContext(g.Context, datastorePutMultiMaxItems)
	_, err = g.PutMulti(hasIdSlice)
	if err != errInternalCall {
		t.Fatalf("Expected %v, got %v", errInternalCall, err)
	}

	g.FlushLocalCache()
	g.Context = withErrorContext(g.Context, datastoreGetMultiMaxItems)
	err = g.GetMulti(hasIdSlice)
	if err != errInternalCall {
		t.Fatalf("Expected %v, got %v", errInternalCall, err)
	}

	keys := make([]*datastore.Key, len(hasIdSlice))
	for x, hasId := range hasIdSlice {
		keys[x] = g.Key(hasId)
	}
	g.Context = withErrorContext(g.Context, datastoreDeleteMultiMaxItems)
	err = g.DeleteMulti(keys)
	if err != errInternalCall {
		t.Fatalf("Expected %v, got %v", errInternalCall, err)
	}
}

func TestPutGet(t *testing.T) {
	c, done, err := aetest.NewContext()
	if err != nil {
		t.Fatalf("Could not start aetest - %v", err)
	}
	defer done()
	g := FromContext(c)

	key, err := g.Put(&PutGet{ID: 12, Value: 15})
	if err != nil {
		t.Fatal(err)
	}
	if key.IntID() != 12 {
		t.Fatal("ID should be 12 but is", key.IntID())
	}

	// Datastore Get
	dsPutGet := &PutGet{}
	err = datastore.Get(c,
		datastore.NewKey(c, "PutGet", "", 12, nil), dsPutGet)
	if err != nil {
		t.Fatal(err)
	}
	if dsPutGet.Value != 15 {
		t.Fatal("dsPutGet.Value should be 15 but is",
			dsPutGet.Value)
	}

	// Goon Get
	goonPutGet := &PutGet{ID: 12}
	err = g.Get(goonPutGet)
	if err != nil {
		t.Fatal(err)
	}
	if goonPutGet.ID != 12 {
		t.Fatal("goonPutGet.ID should be 12 but is", goonPutGet.ID)
	}
	if goonPutGet.Value != 15 {
		t.Fatal("goonPutGet.Value should be 15 but is",
			goonPutGet.Value)
	}
}

func prefixKindName(src interface{}) string {
	return "prefix." + DefaultKindName(src)
}

func TestCustomKindName(t *testing.T) {
	c, done, err := aetest.NewContext()
	if err != nil {
		t.Fatalf("Could not start aetest - %v", err)
	}
	defer done()
	g := FromContext(c)

	hi := HasId{Name: "Foo"}

	//gate
	if kind := g.Kind(hi); kind != "HasId" {
		t.Fatal("HasId King should not have a prefix, but instead is, ", kind)
	}

	g.KindNameResolver = prefixKindName

	if kind := g.Kind(hi); kind != "prefix.HasId" {
		t.Fatal("HasId King should have a prefix, but instead is, ", kind)
	}

	_, err = g.Put(&hi)

	if err != nil {
		t.Fatal("Should be able to put a record: ", err)
	}

	// Due to eventual consistency, we need to wait a bit. The old aetest package
	// had an option to enable strong consistency that has been removed. This
	// is currently the best way I'm aware of to do this.
	time.Sleep(time.Second)
	reget1 := []HasId{}
	query := datastore.NewQuery("prefix.HasId")
	query.GetAll(c, &reget1)
	if len(reget1) != 1 {
		t.Fatal("Should have 1 record stored in datastore ", reget1)
	}
	if reget1[0].Name != "Foo" {
		t.Fatal("Name should be Foo ", reget1[0].Name)
	}
}

func TestMultis(t *testing.T) {
	c, done, err := aetest.NewContext()
	if err != nil {
		t.Fatalf("Could not start aetest - %v", err)
	}
	defer done()
	n := FromContext(c)

	testAmounts := []int{1, 999, 1000, 1001, 1999, 2000, 2001, 2510}
	for _, x := range testAmounts {
		memcache.Flush(c)
		objects := make([]*HasId, x)
		for y := 0; y < x; y++ {
			objects[y] = &HasId{Id: int64(y + 1)}
		}
		if keys, err := n.PutMulti(objects); err != nil {
			t.Fatalf("Error in PutMulti for %d objects - %v", x, err)
		} else if len(keys) != len(objects) {
			t.Fatalf("Expected %v keys, got %v", len(objects), len(keys))
		} else {
			for i, key := range keys {
				if key.IntID() != int64(i+1) {
					t.Fatalf("Expected object #%v key to be %v, got %v", i, (i + 1), key.IntID())
				}
			}
		}
		n.FlushLocalCache() // Put just put them in the local cache, get rid of it before doing the Get
		if err := n.GetMulti(objects); err != nil {
			t.Fatalf("Error in GetMulti - %v", err)
		}
	}

	// check if the returned keys match the struct keys for autogenerated keys
	for _, x := range testAmounts {
		memcache.Flush(c)
		objects := make([]*HasId, x)
		for y := 0; y < x; y++ {
			objects[y] = &HasId{}
		}
		if keys, err := n.PutMulti(objects); err != nil {
			t.Fatalf("Error in PutMulti for %d objects - %v", x, err)
		} else if len(keys) != len(objects) {
			t.Fatalf("Expected %v keys, got %v", len(objects), len(keys))
		} else {
			for i, key := range keys {
				if key.IntID() != objects[i].Id {
					t.Errorf("Expected object #%v key to be %v, got %v", i, objects[i].Id, key.IntID())
				}
			}
		}
		n.FlushLocalCache()
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
			t.Fatalf("Should be receiving a multiError on %d objects, but got no errors", x)
			continue
		}

		merr, ok := err.(appengine.MultiError)
		if ok {
			if len(merr) != len(getobjects) {
				t.Fatalf("Should have received a MultiError object of length %d but got length %d instead", len(getobjects), len(merr))
			}
			for x := range merr {
				switch { // record good conditions, fail in other conditions
				case merr[x] == nil && x%100 == 0:
				case merr[x] != nil && x%100 != 0:
				default:
					t.Fatalf("Found bad condition on object[%d] and error %v", x+1, merr[x])
				}
			}
		} else if x != 1 {
			t.Fatalf("Did not return a multierror on fetch but when fetching %d objects, received - %v", x, merr)
		}
	}
}

type root struct {
	Id   int64 `datastore:"-" goon:"id"`
	Data int
}

type normalChild struct {
	Id     int64          `datastore:"-" goon:"id"`
	Parent *datastore.Key `datastore:"-" goon:"parent"`
	Data   int
}

type coolKey *datastore.Key

type derivedChild struct {
	Id     int64   `datastore:"-" goon:"id"`
	Parent coolKey `datastore:"-" goon:"parent"`
	Data   int
}

func TestParents(t *testing.T) {
	c, done, err := aetest.NewContext()
	if err != nil {
		t.Fatalf("Could not start aetest - %v", err)
	}
	defer done()
	n := FromContext(c)

	r := &root{1, 10}
	rootKey, err := n.Put(r)
	if err != nil {
		t.Fatalf("couldn't Put(%+v)", r)
	}

	// Put exercises both get and set, since Id is uninitialized
	nc := &normalChild{0, rootKey, 20}
	nk, err := n.Put(nc)
	if err != nil {
		t.Fatalf("couldn't Put(%+v)", nc)
	}
	if nc.Parent == rootKey {
		t.Fatalf("derived parent key pointer value didn't change")
	}
	if !(*datastore.Key)(nc.Parent).Equal(rootKey) {
		t.Fatalf("parent of key not equal '%s' v '%s'! ", (*datastore.Key)(nc.Parent), rootKey)
	}
	if !nk.Parent().Equal(rootKey) {
		t.Fatalf("parent of key not equal '%s' v '%s'! ", nk, rootKey)
	}

	dc := &derivedChild{0, (coolKey)(rootKey), 12}
	dk, err := n.Put(dc)
	if err != nil {
		t.Fatalf("couldn't Put(%+v)", dc)
	}
	if dc.Parent == rootKey {
		t.Fatalf("derived parent key pointer value didn't change")
	}
	if !(*datastore.Key)(dc.Parent).Equal(rootKey) {
		t.Fatalf("parent of key not equal '%s' v '%s'! ", (*datastore.Key)(dc.Parent), rootKey)
	}
	if !dk.Parent().Equal(rootKey) {
		t.Fatalf("parent of key not equal '%s' v '%s'! ", dk, rootKey)
	}
}

type ContainerStruct struct {
	Id string `datastore:"-" goon:"id"`
	embeddedStructA
	embeddedStructB `datastore:"w"`
}

type embeddedStructA struct {
	X int
	y int
}

type embeddedStructB struct {
	Z1 int
	Z2 int `datastore:"z2fancy"`
}

func TestEmbeddedStruct(t *testing.T) {
	c, done, err := aetest.NewContext()
	if err != nil {
		t.Fatalf("Could not start aetest - %v", err)
	}
	defer done()
	g := FromContext(c)

	// Store some data with an embedded unexported struct
	pcs := &ContainerStruct{Id: "foo"}
	pcs.X, pcs.y, pcs.Z1, pcs.Z2 = 1, 2, 3, 4
	_, err = g.Put(pcs)
	if err != nil {
		t.Fatalf("Unexpected error on put - %v", err)
	}

	// First run fetches from the datastore (as Put() only caches to the local cache)
	// Second run fetches from memcache (as our first run here called Get() which caches into memcache)
	for i := 1; i <= 2; i++ {
		// Clear the local cache
		g.FlushLocalCache()

		// Fetch it and confirm the values
		gcs := &ContainerStruct{Id: pcs.Id}
		err = g.Get(gcs)
		if err != nil {
			t.Fatalf("#%v - Unexpected error on get - %v", i, err)
		}
		// The exported field must have the correct value
		if gcs.X != pcs.X {
			t.Fatalf("#%v - Expected - %v, got %v", i, pcs.X, gcs.X)
		}
		if gcs.Z1 != pcs.Z1 {
			t.Fatalf("#%v - Expected - %v, got %v", i, pcs.Z1, gcs.Z1)
		}
		if gcs.Z2 != pcs.Z2 {
			t.Fatalf("#%v - Expected - %v, got %v", i, pcs.Z2, gcs.Z2)
		}
		// The unexported field must be zero-valued
		if gcs.y != 0 {
			t.Fatalf("#%v - Expected - %v, got %v", i, 0, gcs.y)
		}
	}
}

func TestMemcachePutTimeout(t *testing.T) {
	c, done, err := aetest.NewContext()
	if err != nil {
		t.Fatalf("Could not start aetest - %v", err)
	}
	defer done()

	origMPTS := MemcachePutTimeoutSmall
	origMPTL := MemcachePutTimeoutLarge
	origMPTT := MemcachePutTimeoutThreshold
	origMGT := MemcacheGetTimeout
	origPMPE := propagateMemcachePutError
	defer func() {
		MemcachePutTimeoutSmall = origMPTS
		MemcachePutTimeoutLarge = origMPTL
		MemcachePutTimeoutThreshold = origMPTT
		MemcacheGetTimeout = origMGT
		propagateMemcachePutError = origPMPE
	}()

	propagateMemcachePutError = false

	g := FromContext(c)
	MemcachePutTimeoutSmall = time.Second
	// put a HasId resource, then test pulling it from memory, memcache, and datastore
	hi := &HasId{Name: "hasid"} // no id given, should be automatically created by the datastore
	if _, err := g.Put(hi); err != nil {
		t.Fatalf("put: unexpected error - %v", err)
	}

	// Generate the cache entry
	data, err := serializeStruct(hi)
	if err != nil {
		t.Fatalf("Unexpected error on serialize: %v", err)
	}
	ci := &cacheItem{
		key:   cacheKey(g.Key(hi)),
		value: data,
	}
	cis := []*cacheItem{ci}

	MemcachePutTimeoutSmall = 0
	MemcachePutTimeoutLarge = 0
	MemcachePutTimeoutThreshold = 1
	if err := g.putMemcache(cis); !appengine.IsTimeoutError(err) {
		t.Fatalf("Request should timeout - err = %v", err)
	}

	MemcachePutTimeoutLarge = time.Second
	if err := g.putMemcache(cis); err != nil {
		t.Fatalf("putMemcache: unexpected error - %v", err)
	}

	g.FlushLocalCache()
	memcache.Flush(c)
	// time out Get
	MemcacheGetTimeout = 0
	// time out Put too
	MemcachePutTimeoutSmall = 0
	MemcachePutTimeoutThreshold = 1
	MemcachePutTimeoutLarge = 0
	hiResult := &HasId{Id: hi.Id}
	if err := g.Get(hiResult); err != nil {
		t.Fatalf("Request should not timeout cause we'll fetch from the datastore but got error  %v", err)
		// Put timing out should also error, but it won't be returned here, just logged
	}
	if !reflect.DeepEqual(hi, hiResult) {
		t.Fatalf("Fetched object isn't accurate\n%s", getDiff(hi, hiResult, "hi", "hiResult"))
	}

	hiResult = &HasId{Id: hi.Id}
	g.FlushLocalCache()
	MemcacheGetTimeout = time.Second
	if err := g.Get(hiResult); err != nil {
		t.Fatalf("Request should not timeout cause we'll fetch from memcache successfully but got error %v", err)
	}
	if !reflect.DeepEqual(hi, hiResult) {
		t.Fatalf("Fetched object isn't accurate\n%s", getDiff(hi, hiResult, "hi", "hiResult"))
	}
}

func TestChangeMemcacheKey(t *testing.T) {
	c, done, err := aetest.NewContext()
	if err != nil {
		t.Fatalf("Could not start aetest - %v", err)
	}
	defer done()

	originalMemcacheKey := MemcacheKey
	defer func() {
		MemcacheKey = originalMemcacheKey
	}()
	verID := appengine.VersionID(c)
	MemcacheKey = func(k *datastore.Key) string {
		return "custom:" + verID + ":" + k.Encode()
	}

	g := FromContext(c)

	key, err := g.Put(&PutGet{ID: 12, Value: 15})
	if err != nil {
		t.Fatal(err)
	}
	g.FlushLocalCache()
	err = g.Get(&PutGet{ID: 12})
	if err != nil {
		t.Fatal(err)
	}

	ckey := cacheKey(key)

	_, err = memcache.Get(c, ckey)
	if err != nil {
		t.Fatal(err)
	}

	if !strings.HasSuffix(ckey, "custom:"+verID+":"+key.Encode()) {
		t.Fatal("cache key should have 'custom:`versionID`:`encodedKey` suffix", err)
	}
}

func TestMemcacheLimits(t *testing.T) {
	c, done, err := aetest.NewContext()
	if err != nil {
		t.Fatalf("Could not start aetest - %v", err)
	}
	defer done()

	// Confirm that the cacheKey function respects the output limit
	pk := datastore.NewKey(c, "Parent", strings.Repeat("p", memcacheMaxKeySize/2), 0, nil)
	ck := datastore.NewKey(c, "Child", strings.Repeat("c", memcacheMaxKeySize/2), 0, pk)
	if mk := cacheKey(ck); len(mk) > memcacheMaxKeySize {
		t.Fatalf("cacheKey returned key with a length of %d exceeding the maximum of %d", len(mk), memcacheMaxKeySize)
	}

	// Confirm that single maximum size fits
	maxItem := &memcache.Item{
		Key:        strings.Repeat("k", memcacheMaxKeySize),
		Value:      make([]byte, memcacheMaxValueSize),
		Flags:      1<<32 - 1,
		Expiration: time.Duration(1<<63 - 1),
	}
	if err := memcache.Set(c, maxItem); err != nil {
		t.Fatalf("Expected maximum sized item to pass! Got error: %v", err)
	}
	if _, err := memcache.Get(c, maxItem.Key); err != nil {
		t.Fatalf("Unexpected error on Get: %v", err)
	}

	// Helper function to test RPC batch size limit
	testRPCBatchSize := func(maxValueSize int, readBack bool) {
		var items []*memcache.Item
		itemSize := memcacheOverhead + memcacheMaxKeySize + maxValueSize
		extra := memcacheOverhead + memcacheMaxKeySize + 1 // at least 1 byte for value
		count := (memcacheMaxRPCSize - extra) / itemSize
		extra += (memcacheMaxRPCSize - extra) % itemSize
		for i := 0; i < count; i++ {
			items = append(items, &memcache.Item{
				Key:        strings.Repeat("k", memcacheMaxKeySize-30) + fmt.Sprintf("%030d", i),
				Value:      make([]byte, maxValueSize),
				Flags:      1<<32 - 1,
				Expiration: time.Duration(1<<63 - 1),
			})
		}
		extraValueSize := extra - memcacheOverhead - memcacheMaxKeySize
		items = append(items, &memcache.Item{
			Key:        strings.Repeat("x", memcacheMaxKeySize),
			Value:      make([]byte, extraValueSize),
			Flags:      1<<32 - 1,
			Expiration: time.Duration(1<<63 - 1),
		})
		if err := memcache.SetMulti(c, items); err != nil {
			t.Fatalf("Expected maximum sized batch to pass with maxValueSize %d! Got error: %v", maxValueSize, err)
		}
		if readBack {
			var keys []string
			for _, item := range items {
				keys = append(keys, item.Key)
			}
			if results, err := memcache.GetMulti(c, keys); err != nil {
				t.Fatalf("Unexpected error on GetMulti with maxValueSize %d: %v", maxValueSize, err)
			} else if len(keys) != len(results) {
				t.Fatalf("Expected %d results but got %d with maxValueSize %d", len(keys), len(results), maxValueSize)
			}
		}
	}

	// Confirm that large entities filling up RPC works
	testRPCBatchSize(memcacheMaxValueSize, true)

	// Confirm with middle-sized entities
	testRPCBatchSize(100000, true)

	// Confirm overhead not exceeding our expectations by filling up RPC with tiny entities
	// NOTE: We don't read back because the memcache emulator is way too slow to do this
	testRPCBatchSize(1000, false)

	// Test RPC limit handling via goon
	g := FromContext(c)

	// Aim for roughly 80% of max entity size
	data := bytes.Repeat([]byte{1, 2, 3, 4}, memcacheMaxItemSize/5)
	// Aim for the maximum RPC size that we can get with datastore GetMulti,
	// which should still be larger than the maximum memcache RPC size
	count := int64(datastoreGetMultiMaxRPCSize / len(data))
	hds := make([]*HasData, 0, count)
	for i := int64(1); i <= count; i++ {
		hds = append(hds, &HasData{Id: i, Data: data})
	}

	// Save all of these
	// TODO: Use PutMulti once goon supports splitting up too large PutMulti requests
	// keys, err := g.PutMulti(hds)
	// if err != nil {
	// 	t.Fatalf("Unexpected error on PutMulti: %v", err)
	// }
	var keys []*datastore.Key
	for _, hd := range hds {
		if key, err := g.Put(hd); err != nil {
			t.Fatalf("Unexpected error on Put: %v", err)
		} else {
			keys = append(keys, key)
		}
	}

	// Flush all the caches
	g.FlushLocalCache()
	memcache.Flush(c)

	// Fetch them all back to confirm datastore works,
	// and to also populate memcache
	for _, hd := range hds {
		hd.Data = nil
	}
	if err := g.GetMulti(hds); err != nil {
		t.Fatalf("Unexpected error on GetMulti: %v", err)
	}
	for _, hd := range hds {
		if !bytes.Equal(hd.Data, data) {
			t.Fatalf("Invalid data! %v", hd.Id)
		}
		hd.Data = nil
	}

	// Flush local cache & datastore
	g.FlushLocalCache()
	if err := datastore.DeleteMulti(c, keys); err != nil {
		t.Fatalf("Unexpected error on DeleteMulti: %v", err)
	}

	// Fetch them all back again, this time from memcache
	if err := g.GetMulti(hds); err != nil {
		t.Fatalf("Unexpected error on GetMulti: %v", err)
	}
	for _, hd := range hds {
		if !bytes.Equal(hd.Data, data) {
			t.Fatalf("Invalid data! %v", hd.Id)
		}
		hd.Data = nil
	}
}
