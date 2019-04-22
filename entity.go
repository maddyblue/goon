/*
 * Copyright (c) 2012 Matt Jibson <matt.jibson@gmail.com>
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
	"encoding/binary"
	"fmt"
	"math"
	"reflect"
	"strings"
	"sync"
	"time"

	"google.golang.org/appengine"
	"google.golang.org/appengine/datastore"
)

// KindNameResolver takes an Entity and returns what the Kind should be for
// Datastore.
type KindNameResolver func(src interface{}) string

// ### Entity serialization ###

const serializationFormatVersion = 5 // Increase this whenever the format changes

// The entities are encoded to bytes with little endian ordering, as follows:
//
// [header][?prop1][..][?propN]
//
// header   | uint32 | Always present
//   The first 30 bits are used for propCount. The last 2 bits for flags.
//
// propX    | []byte | X <= propCount
//   Each property is serialized separately.
//
//
// A property gets serialized into bytes with little endian ordering as follows:
//
// [nameLen][?name][header][?valueLen][?value]
//
// nameLen  | uint16 | Always present
//   The length of the property name.
//
// name     | string | nameLen > 0
//   The property name.
//
// header   | byte   | Always present
//   The first 4 bits specify the property value type (propType..), the last 4 bits are used for flags.
//
// valueLen | uint24 | type == (string|BlobKey|ByteString|[]byte|*Key) && value != zeroValue
//   The length of the value bytes.
//
// value    | []byte | type != (none|bool) && value != zeroValue
//
//    None         N/A
//    Int64        8 bytes
//    Bool         N/A (value == propHasValue header flag)
//    String       valueLen bytes
//    Float64      8 bytes
//    ByteString   valueLen bytes
//    KeyPtr       valueLen bytes
//    Time         8 bytes
//    BlobKey      valueLen bytes
//    GeoPoint     16 bytes, Lat+Lng float64
//    ByteSlice    valueLen bytes

// Entity header flags
const (
	entityExists = 1 << 30
	//entityRESERVED = 1 << 31 // Unused flag
)

const entityHeaderMaskPropCount = 1<<30 - 1              // All the bits used for propCount
const entityHeaderMaskFlags = ^entityHeaderMaskPropCount // All the bits used for flags

func serializeEntityHeader(propCount, flags int) uint32 {
	return uint32((flags & entityHeaderMaskFlags) | (propCount & entityHeaderMaskPropCount))
}

func deserializeEntityHeader(header uint32) (propCount, flags int) {
	return int(header) & entityHeaderMaskPropCount, int(header) & entityHeaderMaskFlags
}

// The valid datastore.Property.Value types are:
//    - int64
//    - bool
//    - string
//    - float64
//    - datastore.ByteString
//    - *datastore.Key
//    - time.Time
//    - appengine.BlobKey
//    - appengine.GeoPoint
//    - []byte (up to 1 megabyte in length)
//    - *Entity (representing a nested struct)
const (
	propTypeNone       = 0
	propTypeInt64      = 1
	propTypeBool       = 2
	propTypeString     = 3
	propTypeFloat64    = 4
	propTypeByteString = 5
	propTypeKeyPtr     = 6
	propTypeTime       = 7
	propTypeBlobKey    = 8
	propTypeGeoPoint   = 9
	propTypeByteSlice  = 10
	//propTypeEntityPtr  = 11 // TODO: Implement this?
	// Space for 4 more types, as the propType value must fit in 4 bits (0-15)
)

// Property header flags
const (
	propHasValue = 1 << 4
	propMultiple = 1 << 5
	propNoIndex  = 1 << 6
	//propRESERVED = 1 << 7 // Unused flag
)

// We limit the maximum length of datastore.Property.Name,
// however this is our implementation specific and not datastore specific.
const propMaxNameLength = 1<<16 - 1

// Keep a pool of buffers around for more efficient allocation
var bufferPool = sync.Pool{
	New: func() interface{} {
		return bytes.NewBuffer(make([]byte, 0, 16384)) // 16 KiB initial capacity
	},
}

// getBuffer returns a reusable buffer from a pool.
// Every buffer acquired with this function must be later freed via freeBuffer.
func getBuffer() *bytes.Buffer {
	return bufferPool.Get().(*bytes.Buffer)
}

// freeBuffer returns the buffer to the pool, allowing for reuse.
func freeBuffer(buf *bytes.Buffer) {
	buf.Reset()
	bufferPool.Put(buf)
}

func writeInt16(buf *bytes.Buffer, i int) {
	buf.WriteByte(byte(i))
	buf.WriteByte(byte(i >> 8))
}

func writeInt24(buf *bytes.Buffer, i int) {
	buf.WriteByte(byte(i))
	buf.WriteByte(byte(i >> 8))
	buf.WriteByte(byte(i >> 16))
}

func toUnixMicro(t time.Time) int64 {
	// We cannot use t.UnixNano() / 1e3 because we want to handle times more than
	// 2^63 nanoseconds (which is about 292 years) away from 1970, and those cannot
	// be represented in the numerator of a single int64 divide.
	return t.Unix()*1e6 + int64(t.Nanosecond()/1e3)
}

func fromUnixMicro(t int64) time.Time {
	return time.Unix(t/1e6, (t%1e6)*1e3).UTC()
}

func serializeProperty(buf *bytes.Buffer, p *datastore.Property) error {
	nameLen := len(p.Name)
	if nameLen > propMaxNameLength {
		return fmt.Errorf("Maximum property name length is %d, but received %d", propMaxNameLength, nameLen)
	}
	writeInt16(buf, nameLen)
	if nameLen > 0 {
		buf.WriteString(p.Name)
	}

	var header byte
	if p.Multiple {
		header |= propMultiple
	}
	if p.NoIndex {
		header |= propNoIndex
	}

	v := reflect.ValueOf(p.Value)
	unsupported := false

	switch v.Kind() {
	case reflect.Invalid:
		// Has no type and no value, but is legal
		buf.WriteByte(header)
	case reflect.Int64:
		header |= propTypeInt64
		val := uint64(v.Int())
		if val == 0 {
			buf.WriteByte(header)
		} else {
			header |= propHasValue
			buf.WriteByte(header)
			data := make([]byte, 8)
			binary.LittleEndian.PutUint64(data, val)
			buf.Write(data)
		}
	case reflect.Bool:
		header |= propTypeBool
		if v.Bool() {
			header |= propHasValue
		}
		buf.WriteByte(header)
	case reflect.String:
		switch v.Interface().(type) {
		case appengine.BlobKey:
			header |= propTypeBlobKey
		default:
			header |= propTypeString
		}
		val := v.String()
		if valLen := len(val); valLen == 0 {
			buf.WriteByte(header)
		} else {
			header |= propHasValue
			buf.WriteByte(header)
			writeInt24(buf, valLen)
			buf.WriteString(val)
		}
	case reflect.Float64:
		header |= propTypeFloat64
		val := v.Float()
		if val == 0 {
			buf.WriteByte(header)
		} else {
			header |= propHasValue
			buf.WriteByte(header)
			data := make([]byte, 8)
			binary.LittleEndian.PutUint64(data, math.Float64bits(val))
			buf.Write(data)
		}
	case reflect.Ptr:
		if k, ok := v.Interface().(*datastore.Key); ok {
			header |= propTypeKeyPtr
			if k == nil {
				buf.WriteByte(header)
			} else {
				header |= propHasValue
				buf.WriteByte(header)
				val := k.Encode()
				writeInt24(buf, len(val))
				buf.WriteString(val)
			}
		} else {
			unsupported = true
		}
	case reflect.Struct:
		switch s := v.Interface().(type) {
		case time.Time:
			header |= propTypeTime
			if s.IsZero() {
				buf.WriteByte(header)
			} else {
				header |= propHasValue
				buf.WriteByte(header)
				data := make([]byte, 8)
				binary.LittleEndian.PutUint64(data, uint64(toUnixMicro(s)))
				buf.Write(data)
			}
		case appengine.GeoPoint:
			header |= propTypeGeoPoint
			if s.Lat == 0 && s.Lng == 0 {
				buf.WriteByte(header)
			} else {
				header |= propHasValue
				buf.WriteByte(header)
				data := make([]byte, 16)
				binary.LittleEndian.PutUint64(data[:8], math.Float64bits(s.Lat))
				binary.LittleEndian.PutUint64(data[8:], math.Float64bits(s.Lng))
				buf.Write(data)
			}
		default:
			unsupported = true
		}
	case reflect.Slice:
		switch b := v.Interface().(type) {
		case datastore.ByteString:
			header |= propTypeByteString
			if bLen := len(b); bLen == 0 {
				buf.WriteByte(header)
			} else {
				header |= propHasValue
				buf.WriteByte(header)
				writeInt24(buf, bLen)
				buf.Write(b)
			}
		case []byte:
			header |= propTypeByteSlice
			if bLen := len(b); bLen == 0 {
				buf.WriteByte(header)
			} else {
				header |= propHasValue
				buf.WriteByte(header)
				writeInt24(buf, bLen)
				buf.Write(b)
			}
		default:
			unsupported = true
		}
	default:
		unsupported = true
	}
	if unsupported {
		return fmt.Errorf("unsupported datastore.Property value type: " + v.Type().String())
	}
	return nil
}

func deserializeProperty(buf *bytes.Buffer, prop *datastore.Property) error {
	next := func(n int) ([]byte, error) {
		b := buf.Next(n)
		if bLen := len(b); bLen != n {
			return b, fmt.Errorf("Buffer EOF, expected %d bytes but got %v", n, bLen)
		}
		return b, nil
	}
	getSize := func() (int, error) {
		sizeBytes, err := next(3)
		if err != nil {
			return 0, err
		}
		return int(sizeBytes[0]) | int(sizeBytes[1])<<8 | int(sizeBytes[2])<<16, nil
	}

	// Read the name length
	nameLenBytes, err := next(2)
	if err != nil {
		return err
	}
	nameLen := int(nameLenBytes[0]) | int(nameLenBytes[1])<<8
	// If thehre's a name, read it
	if nameLen > 0 {
		nameBytes, err := next(nameLen)
		if err != nil {
			return err
		}
		prop.Name = string(nameBytes)
	}

	// Read the header
	header, err := buf.ReadByte()
	if err != nil {
		return err
	}

	// Apply the flags
	prop.Multiple = (header&propMultiple != 0)
	prop.NoIndex = (header&propNoIndex != 0)

	// Determine the value
	valueType := header & 0xF
	zeroValue := header&propHasValue == 0
	switch valueType {
	case propTypeNone:
		// nil interface, so nothing to do
	case propTypeInt64:
		if zeroValue {
			prop.Value = int64(0)
		} else {
			valBytes, err := next(8)
			if err != nil {
				return err
			}
			prop.Value = int64(binary.LittleEndian.Uint64(valBytes))
		}
	case propTypeBool:
		prop.Value = !zeroValue
	case propTypeString:
		if zeroValue {
			prop.Value = ""
		} else {
			size, err := getSize()
			if err != nil {
				return err
			}
			valBytes, err := next(size)
			if err != nil {
				return err
			}
			prop.Value = string(valBytes)
		}
	case propTypeBlobKey:
		if zeroValue {
			prop.Value = appengine.BlobKey("")
		} else {
			size, err := getSize()
			if err != nil {
				return err
			}
			valBytes, err := next(size)
			if err != nil {
				return err
			}
			prop.Value = appengine.BlobKey(valBytes)
		}
	case propTypeFloat64:
		if zeroValue {
			prop.Value = float64(0)
		} else {
			valBytes, err := next(8)
			if err != nil {
				return err
			}
			prop.Value = math.Float64frombits(binary.LittleEndian.Uint64(valBytes))
		}
	case propTypeKeyPtr:
		if zeroValue {
			var key *datastore.Key
			prop.Value = key
		} else {
			size, err := getSize()
			if err != nil {
				return err
			}
			valBytes, err := next(size)
			if err != nil {
				return err
			}
			prop.Value, err = datastore.DecodeKey(string(valBytes))
			if err != nil {
				return err
			}
		}
	case propTypeTime:
		if zeroValue {
			prop.Value = time.Time{}
		} else {
			valBytes, err := next(8)
			if err != nil {
				return err
			}
			prop.Value = fromUnixMicro(int64(binary.LittleEndian.Uint64(valBytes)))
		}
	case propTypeGeoPoint:
		if zeroValue {
			prop.Value = appengine.GeoPoint{}
		} else {
			valBytes, err := next(16)
			if err != nil {
				return err
			}
			prop.Value = appengine.GeoPoint{
				Lat: math.Float64frombits(binary.LittleEndian.Uint64(valBytes[:8])),
				Lng: math.Float64frombits(binary.LittleEndian.Uint64(valBytes[8:])),
			}
		}
	case propTypeByteString:
		if zeroValue {
			prop.Value = datastore.ByteString{}
		} else {
			size, err := getSize()
			if err != nil {
				return err
			}
			prop.Value = make(datastore.ByteString, size)
			if _, err := buf.Read(prop.Value.(datastore.ByteString)); err != nil {
				return err
			}
		}
	case propTypeByteSlice:
		if zeroValue {
			prop.Value = []byte{}
		} else {
			size, err := getSize()
			if err != nil {
				return err
			}
			prop.Value = make([]byte, size)
			if _, err := buf.Read(prop.Value.([]byte)); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("Unrecognized value type %d", valueType)
	}

	return nil
}

// serializeStruct takes a struct and serializes it to portable bytes.
func serializeStruct(src interface{}) ([]byte, error) {
	if src == nil {
		return []byte{0, 0, 0, 0}, nil
	}
	if k := reflect.Indirect(reflect.ValueOf(src)).Type().Kind(); k != reflect.Struct {
		return nil, fmt.Errorf("goon: Expected struct, got instead: %v", k)
	}

	buf := getBuffer()
	defer freeBuffer(buf)

	var err error
	var props []datastore.Property
	if pls, ok := src.(datastore.PropertyLoadSaver); ok {
		props, err = pls.Save()
	} else {
		props, err = datastore.SaveStruct(src)
	}
	if err != nil {
		return nil, err
	}

	// Serialize the entity header
	header := serializeEntityHeader(len(props), entityExists)
	headerBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(headerBytes, header)
	buf.Write(headerBytes)

	// Serialize the properties
	for i := range props {
		if err := serializeProperty(buf, &props[i]); err != nil {
			return nil, err
		}
	}

	output := make([]byte, buf.Len())
	copy(output, buf.Bytes())
	return output, nil
}

// deserializeStruct takes portable bytes b, generated by serializeStruct, and assigns correct values to struct dst.
func deserializeStruct(dst interface{}, b []byte) error {
	if len(b) == 0 {
		return fmt.Errorf("goon: Expected some data to deserialize, got none.")
	}
	if k := reflect.Indirect(reflect.ValueOf(dst)).Type().Kind(); k != reflect.Struct {
		return fmt.Errorf("goon: Expected struct, got instead: %v", k)
	}

	// Deserialize the header
	header := binary.LittleEndian.Uint32(b[:4])
	propCount, flags := deserializeEntityHeader(header)
	if flags&entityExists == 0 {
		return datastore.ErrNoSuchEntity
	}

	// Deserialize the properties
	if propCount > 0 {
		buf := bytes.NewBuffer(b[4:])
		props := make([]datastore.Property, propCount)
		for i := 0; i < propCount; i++ {
			if err := deserializeProperty(buf, &props[i]); err != nil {
				return err
			}
		}
		if pls, ok := dst.(datastore.PropertyLoadSaver); ok {
			if err := pls.Load(props); err != nil {
				return err
			}
		} else {
			if err := datastore.LoadStruct(dst, props); err != nil {
				return err
			}
		}
	}

	return nil
}

// getStructKey returns the key of the struct based in its reflected or
// specified kind and id. The second return parameter is true if src has a
// string id.
func (g *Goon) getStructKey(src interface{}) (key *datastore.Key, hasStringId bool, err error) {
	v := reflect.Indirect(reflect.ValueOf(src))
	t := v.Type()
	k := t.Kind()

	if k != reflect.Struct {
		err = fmt.Errorf("goon: Expected struct, got instead: %v", k)
		return
	}

	var parent *datastore.Key
	var stringID string
	var intID int64
	var kind string

	for i := 0; i < v.NumField(); i++ {
		tf := t.Field(i)
		vf := v.Field(i)

		tag := tf.Tag.Get("goon")
		tagValues := strings.Split(tag, ",")
		if len(tagValues) > 0 {
			tagValue := tagValues[0]
			if tagValue == "id" {
				switch vf.Kind() {
				case reflect.Int64:
					if intID != 0 || stringID != "" {
						err = fmt.Errorf("goon: Only one field may be marked id")
						return
					}
					intID = vf.Int()
				case reflect.String:
					if intID != 0 || stringID != "" {
						err = fmt.Errorf("goon: Only one field may be marked id")
						return
					}
					stringID = vf.String()
					hasStringId = true
				default:
					err = fmt.Errorf("goon: ID field must be int64 or string in %v", t.Name())
					return
				}
			} else if tagValue == "kind" {
				if vf.Kind() == reflect.String {
					if kind != "" {
						err = fmt.Errorf("goon: Only one field may be marked kind")
						return
					}
					kind = vf.String()
					if kind == "" && len(tagValues) > 1 && tagValues[1] != "" {
						kind = tagValues[1]
					}
				}
			} else if tagValue == "parent" {
				dskeyType := reflect.TypeOf(&datastore.Key{})
				if vf.Type().ConvertibleTo(dskeyType) {
					if parent != nil {
						err = fmt.Errorf("goon: Only one field may be marked parent")
						return
					}
					parent = vf.Convert(dskeyType).Interface().(*datastore.Key)
				}
			}
		}
	}

	// if kind has not been manually set, fetch it from src's type
	if kind == "" {
		kind = g.KindNameResolver(src)
	}
	key = datastore.NewKey(g.Context, kind, stringID, intID, parent)
	return
}

// DefaultKindName is the default implementation to determine the Kind
// an Entity has. Returns the basic Type of the src (no package name included).
func DefaultKindName(src interface{}) string {
	v := reflect.ValueOf(src)
	v = reflect.Indirect(v)
	t := v.Type()
	return t.Name()
}

func (g *Goon) setStructKey(src interface{}, key *datastore.Key) error {
	v := reflect.ValueOf(src)
	t := v.Type()
	k := t.Kind()

	if k != reflect.Ptr {
		return fmt.Errorf("goon: Expected pointer to struct, got instead: %v", k)
	}

	v = reflect.Indirect(v)
	t = v.Type()
	k = t.Kind()

	if k != reflect.Struct {
		return fmt.Errorf(fmt.Sprintf("goon: Expected struct, got instead: %v", k))
	}

	idSet := false
	kindSet := false
	parentSet := false
	for i := 0; i < v.NumField(); i++ {
		tf := t.Field(i)
		vf := v.Field(i)

		if !vf.CanSet() {
			continue
		}

		tag := tf.Tag.Get("goon")
		tagValues := strings.Split(tag, ",")
		if len(tagValues) > 0 {
			tagValue := tagValues[0]
			if tagValue == "id" {
				if idSet {
					return fmt.Errorf("goon: Only one field may be marked id")
				}

				switch vf.Kind() {
				case reflect.Int64:
					vf.SetInt(key.IntID())
					idSet = true
				case reflect.String:
					vf.SetString(key.StringID())
					idSet = true
				}
			} else if tagValue == "kind" {
				if kindSet {
					return fmt.Errorf("goon: Only one field may be marked kind")
				}
				if vf.Kind() == reflect.String {
					if (len(tagValues) <= 1 || key.Kind() != tagValues[1]) && g.KindNameResolver(src) != key.Kind() {
						vf.Set(reflect.ValueOf(key.Kind()))
					}
					kindSet = true
				}
			} else if tagValue == "parent" {
				if parentSet {
					return fmt.Errorf("goon: Only one field may be marked parent")
				}
				dskeyType := reflect.TypeOf(&datastore.Key{})
				vfType := vf.Type()
				if vfType.ConvertibleTo(dskeyType) {
					vf.Set(reflect.ValueOf(key.Parent()).Convert(vfType))
					parentSet = true
				}
			}
		}
	}

	if !idSet {
		return fmt.Errorf("goon: Could not set id field")
	}

	return nil
}
