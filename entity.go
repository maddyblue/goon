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
	"encoding/gob"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	"appengine/datastore"
)

type fieldInfo struct {
	sliceIndex []int
	fieldIndex []int
}

const (
	serializationStateEmpty  = 0x00
	serializationStateNormal = 0x01
)

var (
	timeType        = reflect.TypeOf(time.Time{})
	fieldInfos      = make(map[reflect.Type]map[string]*fieldInfo)
	fieldInfosInGen = make(map[reflect.Type]bool)
	fieldInfosLock  sync.RWMutex
)

func getFieldInfo(t reflect.Type) map[string]*fieldInfo {
	fieldInfosLock.RLock()
	// Attempt to get the field info map for this type under an efficient read lock
	fieldMap, ok := fieldInfos[t]
	fieldInfosLock.RUnlock()

	if !ok {
		fieldInfosLock.Lock()
		// Check again, the map could have appeared when we didn't have a lock
		fieldMap, ok = fieldInfos[t]
		if !ok {
			// Is some other goroutine already generating the map?
			if fieldInfosInGen[t] {
				fieldInfosLock.Unlock()
				for {
					// TODO: Improve this implementation by removing sleep
					// and replacing it with something more efficient
					time.Sleep(time.Microsecond * 10)

					fieldInfosLock.RLock()
					if fieldInfosInGen[t] {
						fieldInfosLock.RUnlock()
						continue
					}

					fieldMap = fieldInfos[t]
					fieldInfosLock.RUnlock()
					return fieldMap
				}
			} else {
				// We are going to generate the map for this type
				fieldInfosInGen[t] = true
				fieldInfosLock.Unlock()

				fieldMap = make(map[string]*fieldInfo, 16)
				generateStructFieldMap(t, "", make([]int, 0, 16), []int{}, fieldMap)

				fieldInfosLock.Lock()
				fieldInfos[t] = fieldMap
				delete(fieldInfosInGen, t)
			}
		}
		fieldInfosLock.Unlock()
	}

	return fieldMap
}

func generateStructFieldMap(t reflect.Type, namePrefix string, indexPrefix, sliceIndex []int, fieldMap map[string]*fieldInfo) {
	var fieldName string

	numFields := t.NumField()
	for i := 0; i < numFields; i++ {
		tf := t.Field(i)
		if tf.PkgPath != "" {
			continue
		}

		tag := tf.Tag.Get("datastore")
		if len(tag) > 0 {
			if commaPos := strings.Index(tag, ","); commaPos == -1 {
				fieldName = tag
			} else if commaPos == 0 {
				fieldName = tf.Name
			} else {
				fieldName = tag[:commaPos]
			}
			if fieldName == "-" {
				continue
			}
		} else {
			fieldName = tf.Name
		}
		if namePrefix != "" {
			fieldName = namePrefix + fieldName
		}

		if tf.Type.Kind() == reflect.Slice {
			elemType := tf.Type.Elem()
			if elemType.Kind() == reflect.Struct && elemType != timeType {
				generateStructFieldMap(elemType, fieldName+".", make([]int, 0, 8), append(indexPrefix, i), fieldMap)
				continue
			}
		} else if tf.Type.Kind() == reflect.Struct && tf.Type != timeType {
			generateStructFieldMap(tf.Type, fieldName+".", append(indexPrefix, i), sliceIndex, fieldMap)
			continue
		}

		finalIndex := append(indexPrefix, i)
		fi := &fieldInfo{sliceIndex: make([]int, len(sliceIndex)), fieldIndex: make([]int, len(finalIndex))}
		copy(fi.sliceIndex, sliceIndex)
		copy(fi.fieldIndex, finalIndex)
		fieldMap[fieldName] = fi
	}
}

func serializeStruct(src interface{}) ([]byte, error) {
	if src == nil {
		return []byte{serializationStateEmpty}, nil
	}

	v := reflect.Indirect(reflect.ValueOf(src))
	t := v.Type()
	k := t.Kind()

	if k != reflect.Struct {
		return nil, fmt.Errorf("goon: Expected struct, got instead: %v", k)
	}

	initialBufSize := 512 + t.Size() // Rough estimation for initial buffer size
	buf := bytes.NewBuffer(make([]byte, 0, initialBufSize))
	buf.WriteByte(serializationStateNormal) // Set the header
	enc := gob.NewEncoder(buf)

	if err := serializeStructInternal(enc, "", v, t); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func serializeStructInternal(enc *gob.Encoder, namePrefix string, v reflect.Value, t reflect.Type) error {
	var fieldName string
	var metaData string

	numFields := v.NumField()
	for i := 0; i < numFields; i++ {
		vf := v.Field(i)
		if !vf.CanSet() {
			continue
		}

		tf := t.Field(i)
		tag := tf.Tag.Get("datastore")
		if len(tag) > 0 {
			if commaPos := strings.Index(tag, ","); commaPos == -1 {
				fieldName = tag
			} else if commaPos == 0 {
				fieldName = tf.Name
			} else {
				fieldName = tag[:commaPos]
			}
			if fieldName == "-" {
				continue
			}
		} else {
			fieldName = tf.Name
		}
		if namePrefix != "" {
			fieldName = namePrefix + fieldName
		}

		if vf.Kind() == reflect.Slice {
			elemType := vf.Type().Elem()
			// Unroll slices of structs
			if elemType.Kind() == reflect.Struct && elemType != timeType {
				subPrefix := fieldName + "."
				vfLen := vf.Len()
				for j := 0; j < vfLen; j++ {
					vi := vf.Index(j)
					if err := serializeStructInternal(enc, subPrefix, vi, vi.Type()); err != nil {
						return err
					}
				}
				continue
			} else if elemType.Kind() == reflect.Ptr {
				// For a slice of pointers we need to check if any index is nil,
				// because Gob unfortunately fails at encoding nil values
				anyNil := false
				vfLen := vf.Len()
				for j := 0; j < vfLen; j++ {
					vi := vf.Index(j)
					if vi.IsNil() {
						anyNil = true
						break
					}
				}
				if anyNil {
					for j := 0; j < vfLen; j++ {
						vi := vf.Index(j)
						encodeValue := true
						if vi.IsNil() {
							// Gob unfortunately fails at encoding nil values
							metaData = "!" + fieldName
							encodeValue = false
						} else {
							metaData = fieldName
						}
						if err := serializeStructInternalEncode(enc, fieldName, metaData, encodeValue, vi); err != nil {
							return err
						}
					}
					continue
				}
			}
		} else if vf.Kind() == reflect.Struct && tf.Type != timeType {
			if err := serializeStructInternal(enc, fieldName+".", vf, vf.Type()); err != nil {
				return err
			}
			continue
		}

		encodeValue := true
		if vf.Kind() == reflect.Slice {
			// When decoding, if the target is a slice but metadata doesn't have the $ sign,
			// then we can properly create a single value slice instead of panicing
			metaData = "$" + fieldName
		} else if vf.Kind() == reflect.Ptr && vf.IsNil() {
			// Gob unfortunately fails at encoding nil values
			metaData = "!" + fieldName
			encodeValue = false
		} else {
			metaData = fieldName
		}

		if err := serializeStructInternalEncode(enc, fieldName, metaData, encodeValue, vf); err != nil {
			return err
		}
	}

	return nil
}

func serializeStructInternalEncode(enc *gob.Encoder, fieldName, metaData string, encodeValue bool, v reflect.Value) error {
	if err := enc.Encode(metaData); err != nil {
		return fmt.Errorf("goon: Failed to encode field metadata %v - %v", metaData, err)
	}
	if encodeValue {
		if err := enc.EncodeValue(v); err != nil {
			return fmt.Errorf("goon: Failed to encode field %v value %v - %v", fieldName, v.Interface(), err)
		}
	}
	return nil
}

func deserializeStruct(dst interface{}, b []byte) error {
	v := reflect.Indirect(reflect.ValueOf(dst))
	t := v.Type()
	k := t.Kind()

	if k != reflect.Struct {
		return fmt.Errorf("goon: Expected struct, got instead: %v", k)
	}

	buf := bytes.NewBuffer(b)
	if header, err := buf.ReadByte(); err != nil {
		return fmt.Errorf("goon: Unexpected error reading cache header: %v", err)
	} else if header == serializationStateEmpty {
		return datastore.ErrNoSuchEntity
	} else if header != serializationStateNormal {
		return fmt.Errorf("goon: Unrecognized cache header: %v", header)
	}

	var metaData string
	dec := gob.NewDecoder(buf)
	structHistory := make(map[string]map[string]bool, 8)
	fieldMap := getFieldInfo(t)

	for buf.Len() > 0 {
		if err := dec.Decode(&metaData); err != nil {
			return fmt.Errorf("goon: Failed to decode field metadata: %v", err)
		}

		fieldName, slice, zeroValue := metaData, false, false
		if metaData[0] == '$' {
			fieldName, slice = metaData[1:], true
		} else if metaData[0] == '!' {
			fieldName, zeroValue = metaData[1:], true
		}
		nameParts := strings.Split(fieldName, ".")

		fi, ok := fieldMap[fieldName]
		if !ok {
			return fmt.Errorf("goon: Could not find field %v", fieldName)
		}

		if err := deserializeStructInternal(dec, fi, fieldName, nameParts, slice, zeroValue, structHistory, v, t); err != nil {
			return err
		}
	}

	return nil
}

func deserializeStructInternal(dec *gob.Decoder, fi *fieldInfo, fieldName string, nameParts []string, slice, zeroValue bool, structHistory map[string]map[string]bool, v reflect.Value, t reflect.Type) error {
	if len(fi.sliceIndex) > 0 {
		v = v.FieldByIndex(fi.sliceIndex)
		t = v.Type()

		var sv reflect.Value
		createNew := false
		nameIdx := len(fi.sliceIndex)
		absName, childName := strings.Join(nameParts[:nameIdx], "."), strings.Join(nameParts[nameIdx:], ".")
		sh, ok := structHistory[absName]
		if !ok || sh[childName] {
			sh = make(map[string]bool, 8)
			structHistory[absName] = sh
			createNew = true
		} else if len(sh) == 0 {
			createNew = true
		}

		if createNew {
			structType := t.Elem()
			sv = reflect.New(structType).Elem()
			v.Set(reflect.Append(v, sv))
		}

		sv = v.Index(v.Len() - 1)
		sh[childName] = true

		v = sv
		t = v.Type()
	}

	vf := v.FieldByIndex(fi.fieldIndex)

	if vf.Kind() == reflect.Slice && !slice {
		elemType := vf.Type().Elem()
		ev := reflect.New(elemType).Elem()

		if !zeroValue {
			if err := dec.DecodeValue(ev); err != nil {
				return fmt.Errorf("goon: Failed to decode field %v - %v", fieldName, err)
			}
		}

		vf.Set(reflect.Append(vf, ev))
	} else if !zeroValue {
		if err := dec.DecodeValue(vf); err != nil {
			return fmt.Errorf("goon: Failed to decode field %v - %v", fieldName, err)
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
				if vf.Type() == reflect.TypeOf(&datastore.Key{}) {
					if parent != nil {
						err = fmt.Errorf("goon: Only one field may be marked parent")
						return
					}
					parent = vf.Interface().(*datastore.Key)
				}
			}
		}
	}

	// if kind has not been manually set, fetch it from src's type
	if kind == "" {
		kind = typeName(src)
	}
	key = datastore.NewKey(g.context, kind, stringID, intID, parent)
	return
}

func typeName(src interface{}) string {
	v := reflect.ValueOf(src)
	v = reflect.Indirect(v)
	t := v.Type()
	return t.Name()
}

func setStructKey(src interface{}, key *datastore.Key) error {
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
					if (len(tagValues) <= 1 || key.Kind() != tagValues[1]) && typeName(src) != key.Kind() {
						vf.Set(reflect.ValueOf(key.Kind()))
					}
					kindSet = true
				}
			} else if tagValue == "parent" {
				if parentSet {
					return fmt.Errorf("goon: Only one field may be marked parent")
				}
				if vf.Type() == reflect.TypeOf(&datastore.Key{}) {
					vf.Set(reflect.ValueOf(key.Parent()))
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
