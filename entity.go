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
	"container/list"
	"encoding/gob"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"
	"unsafe"

	"appengine"
	"appengine/datastore"
)

type fieldInfo struct {
	sliceIndex []int
	fieldIndex []int
}

type fieldMetadata struct {
	name  string
	index int
}

type structMetaData struct {
	metaDatas   []string
	totalLength int
}

// https://developers.google.com/appengine/docs/go/datastore/reference
type seBootstrap struct {
	v01 datastore.Key // Non-pointer on purpose
	v02 time.Time
	v03 appengine.BlobKey
	v04 []time.Time
	v05 []int
	v06 []int8
	v07 []int16
	v08 []int32
	v09 []int64
	v10 []float32
	v11 []float64
	v12 []bool
	v13 []string
	v14 [][]byte
}

type serializationEncoder struct {
	buf *bytes.Buffer
	enc *gob.Encoder
}

type serializationDecoder struct {
	sr  *serializationReader
	dec *gob.Decoder
}

type serializationReader struct {
	r *bytes.Reader
}

func (sr *serializationReader) Read(p []byte) (n int, err error) {
	return sr.r.Read(p)
}

func (sr *serializationReader) ReadByte() (c byte, err error) {
	return sr.r.ReadByte()
}

const (
	serializationStateEmpty  = 0x00
	serializationStateNormal = 0x01
)

var (
	timeType                  = reflect.TypeOf(time.Time{})
	fieldInfos                = make(map[reflect.Type]map[string]*fieldInfo)
	fieldMetadatas            = make(map[reflect.Type][]fieldMetadata)
	fieldIMLock               sync.RWMutex
	serializationEncoders     = list.New()
	serializationEncodersLock sync.Mutex
	serializationDecoders     = list.New()
	serializationDecodersLock sync.Mutex
	seBoot                    seBootstrap
	seBootBytes               []byte
	seBootBytesLock           sync.RWMutex
)

func getFieldInfoAndMetadata(t reflect.Type) (map[string]*fieldInfo, []fieldMetadata) {
	fieldIMLock.RLock()
	// Attempt to get the data for this type under an efficient read lock
	fieldMap, ok := fieldInfos[t]
	fms := fieldMetadatas[t]
	fieldIMLock.RUnlock()

	if !ok {
		fieldIMLock.Lock()
		// Check again, the data could have appeared when we didn't have a lock
		fieldMap, ok = fieldInfos[t]
		if !ok {
			// We are going to generate the data for this type
			fieldMap = make(map[string]*fieldInfo, 16)
			generateFieldInfoAndMetadata(t, "", make([]int, 0, 16), []int{}, fieldMap)
			fieldInfos[t] = fieldMap
		}
		fms = fieldMetadatas[t]
		fieldIMLock.Unlock()
	}

	return fieldMap, fms
}

func getFieldMetadata(t reflect.Type) []fieldMetadata {
	fieldIMLock.RLock()
	defer fieldIMLock.RUnlock()
	return fieldMetadatas[t]
}

// generateFieldInfoAndMetadata should only be called under a fieldIMLock write-lock
func generateFieldInfoAndMetadata(t reflect.Type, namePrefix string, indexPrefix, sliceIndex []int, fieldMap map[string]*fieldInfo) {
	var fieldName string
	fms, havefms := fieldMetadatas[t]

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
		if !havefms {
			fms = append(fms, fieldMetadata{name: fieldName, index: i})
		}
		if namePrefix != "" {
			fieldName = namePrefix + fieldName
		}

		if tf.Type.Kind() == reflect.Slice {
			elemType := tf.Type.Elem()
			if elemType.Kind() == reflect.Struct && elemType != timeType {
				generateFieldInfoAndMetadata(elemType, fieldName+".", make([]int, 0, 8), append(indexPrefix, i), fieldMap)
				continue
			}
		} else if tf.Type.Kind() == reflect.Struct && tf.Type != timeType {
			generateFieldInfoAndMetadata(tf.Type, fieldName+".", append(indexPrefix, i), sliceIndex, fieldMap)
			continue
		}

		finalIndex := append(indexPrefix, i)
		fi := &fieldInfo{sliceIndex: make([]int, len(sliceIndex)), fieldIndex: make([]int, len(finalIndex))}
		copy(fi.sliceIndex, sliceIndex)
		copy(fi.fieldIndex, finalIndex)
		fieldMap[fieldName] = fi
	}

	if !havefms {
		fieldMetadatas[t] = fms
	}
}

func serializeStructMetaData(buf []byte, smd *structMetaData) {
	pos := 0
	for _, metaData := range smd.metaDatas {
		copy(buf[pos:], metaData)
		pos += len(metaData)
		buf[pos] = '+'
		pos += 1
	}
	buf[pos-1] = '|'
}

func deserializeStructMetaData(buf []byte) *structMetaData {
	smd := &structMetaData{metaDatas: make([]string, 0, 16)}
	pos, bufLen := 0, len(buf)
	for i := 0; i < bufLen; i++ {
		if buf[i] == '+' || buf[i] == '|' {
			smd.metaDatas = append(smd.metaDatas, string(buf[pos:i]))
			smd.totalLength += i - pos
			pos = i + 1
			if buf[i] == '|' {
				break
			}
		}
	}
	return smd
}

// updateSEBootBytes should only be set true if called under seBootBytesLock write-lock
func bootstrapSerializationEncoder(se *serializationEncoder, updateSEBootBytes bool) {
	se.enc.Encode(seBoot.v01)
	se.enc.Encode(seBoot.v02)
	se.enc.Encode(seBoot.v03)
	se.enc.Encode(seBoot.v04)
	se.enc.Encode(seBoot.v05)
	se.enc.Encode(seBoot.v06)
	se.enc.Encode(seBoot.v07)
	se.enc.Encode(seBoot.v08)
	se.enc.Encode(seBoot.v09)
	se.enc.Encode(seBoot.v10)
	se.enc.Encode(seBoot.v11)
	se.enc.Encode(seBoot.v12)
	se.enc.Encode(seBoot.v13)
	se.enc.Encode(seBoot.v14)

	if updateSEBootBytes {
		seBootBytes = make([]byte, se.buf.Len())
		copy(seBootBytes, se.buf.Bytes())
	}

	se.buf.Reset()
}

func bootstrapSerializationDecoder(sd *serializationDecoder) {
	seBootBytesLock.RLock()
	ok := len(seBootBytes) > 0
	seBootBytesLock.RUnlock()
	if !ok {
		seBootBytesLock.Lock()
		ok = len(seBootBytes) > 0
		if !ok {
			buf := bytes.NewBuffer(make([]byte, 0, 4096))
			enc := gob.NewEncoder(buf)
			se := &serializationEncoder{buf: buf, enc: enc}
			bootstrapSerializationEncoder(se, true)
		}
		seBootBytesLock.Unlock()
	}

	sd.sr.r = bytes.NewReader(seBootBytes)
	for i := 0; i < 14; i++ {
		sd.dec.Decode(nil)
	}
}

func getSerializationEncoder() *serializationEncoder {
	serializationEncodersLock.Lock()
	// Use an existing one if possible
	if serializationEncoders.Len() > 0 {
		defer serializationEncodersLock.Unlock()
		return serializationEncoders.Remove(serializationEncoders.Front()).(*serializationEncoder)
	}
	serializationEncodersLock.Unlock()
	// Otherwise allocate a new one
	buf := bytes.NewBuffer(make([]byte, 0, 16384)) // 16 KiB initial capacity
	enc := gob.NewEncoder(buf)
	se := &serializationEncoder{buf: buf, enc: enc}
	bootstrapSerializationEncoder(se, false)
	return se
}

func freeSerializationEncoder(se *serializationEncoder) {
	se.buf.Reset()
	serializationEncodersLock.Lock()
	serializationEncoders.PushBack(se)
	// TODO: Perhaps some occasional clean-up is in order?
	// Running 50 goroutines concurrently can allocate a bunch of these
	// but they may not be needed again for quite some time
	serializationEncodersLock.Unlock()
}

func getSerializationDecoder(data []byte) *serializationDecoder {
	serializationDecodersLock.Lock()
	// Use an existing one if possible
	if serializationDecoders.Len() > 0 {
		sd := serializationDecoders.Remove(serializationDecoders.Front()).(*serializationDecoder)
		serializationDecodersLock.Unlock()
		sd.sr.r = bytes.NewReader(data)
		return sd
	}
	serializationDecodersLock.Unlock()
	// Otherwise allocate a new one
	sr := &serializationReader{}
	dec := gob.NewDecoder(sr)
	sd := &serializationDecoder{sr: sr, dec: dec}
	bootstrapSerializationDecoder(sd)
	sd.sr.r = bytes.NewReader(data)
	return sd
}

func freeSerializationDecoder(sd *serializationDecoder) {
	sd.sr.r = nil // Avoid memory leaks
	serializationDecodersLock.Lock()
	serializationDecoders.PushBack(sd)
	// TODO: Perhaps some occasional clean-up is in order?
	// Running 50 goroutines concurrently can allocate a bunch of these
	// but they may not be needed again for quite some time
	serializationDecodersLock.Unlock()
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

	se := getSerializationEncoder()
	defer freeSerializationEncoder(se)
	smd := &structMetaData{metaDatas: make([]string, 0, 16)}
	_, fms := getFieldInfoAndMetadata(t) // Use this function to force generation if needed

	if err := serializeStructInternal(se.enc, smd, fms, "", v); err != nil {
		return nil, err
	}

	bufSize := se.buf.Len()
	// final size = header + all metadatas + separators for metadata + data
	finalBufSize := 1 + smd.totalLength + len(smd.metaDatas) + bufSize
	finalBuf := make([]byte, finalBufSize)
	finalBuf[0] = byte(serializationStateNormal)          // Set the header
	serializeStructMetaData(finalBuf[1:], smd)            // Serialize the metadata
	copy(finalBuf[finalBufSize-bufSize:], se.buf.Bytes()) // Copy the actual data

	return finalBuf, nil
}

func serializeStructInternal(enc *gob.Encoder, smd *structMetaData, fms []fieldMetadata, namePrefix string, v reflect.Value) error {
	var fieldName string
	var metaData string
	var elemType reflect.Type

	for _, fm := range fms {
		vf := v.Field(fm.index)

		if namePrefix != "" {
			fieldName = namePrefix + fm.name
		} else {
			fieldName = fm.name
		}

		if vf.Kind() == reflect.Slice {
			elemType = vf.Type().Elem()
			if elemType != timeType {
				// Unroll slices of structs
				if elemType.Kind() == reflect.Struct {
					if vfLen := vf.Len(); vfLen > 0 {
						subFms := getFieldMetadata(elemType)
						subPrefix := fieldName + "."
						for j := 0; j < vfLen; j++ {
							vi := vf.Index(j)
							if err := serializeStructInternal(enc, smd, subFms, subPrefix, vi); err != nil {
								return err
							}
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
							if err := serializeStructInternalEncode(enc, smd, fieldName, metaData, encodeValue, vi); err != nil {
								return err
							}
						}
						continue
					}
				} else if elemType.Kind().String() != elemType.Name() {
					// For slices of custom non-struct types, encode them as slices of the underlying type.
					// This is required to be able to re-use a global gob encoding machine,
					// as custom types require the type info to be declared by gob for every encoded struct!
					switch elemType.Kind() {
					case reflect.String:
						vf = reflect.ValueOf(*(*[]string)(unsafe.Pointer(vf.UnsafeAddr())))
					case reflect.Bool:
						vf = reflect.ValueOf(*(*[]bool)(unsafe.Pointer(vf.UnsafeAddr())))
					case reflect.Int:
						vf = reflect.ValueOf(*(*[]int)(unsafe.Pointer(vf.UnsafeAddr())))
					case reflect.Int8:
						vf = reflect.ValueOf(*(*[]int8)(unsafe.Pointer(vf.UnsafeAddr())))
					case reflect.Int16:
						vf = reflect.ValueOf(*(*[]int16)(unsafe.Pointer(vf.UnsafeAddr())))
					case reflect.Int32:
						vf = reflect.ValueOf(*(*[]int32)(unsafe.Pointer(vf.UnsafeAddr())))
					case reflect.Int64:
						vf = reflect.ValueOf(*(*[]int64)(unsafe.Pointer(vf.UnsafeAddr())))
					case reflect.Float32:
						vf = reflect.ValueOf(*(*[]float32)(unsafe.Pointer(vf.UnsafeAddr())))
					case reflect.Float64:
						vf = reflect.ValueOf(*(*[]float64)(unsafe.Pointer(vf.UnsafeAddr())))
					}
				}
			}
		} else if vf.Kind() == reflect.Struct {
			if vfType := vf.Type(); vfType != timeType {
				if err := serializeStructInternal(enc, smd, getFieldMetadata(vfType), fieldName+".", vf); err != nil {
					return err
				}
				continue
			}
		}

		encodeValue := true
		if vf.Kind() == reflect.Slice && elemType.Kind() != reflect.Uint8 {
			// NB! []byte is a special case and not a slice
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

		if err := serializeStructInternalEncode(enc, smd, fieldName, metaData, encodeValue, vf); err != nil {
			return err
		}
	}
	return nil
}

func serializeStructInternalEncode(enc *gob.Encoder, smd *structMetaData, fieldName, metaData string, encodeValue bool, v reflect.Value) error {
	smd.metaDatas = append(smd.metaDatas, metaData)
	smd.totalLength += len(metaData)

	if encodeValue {
		if err := enc.EncodeValue(v); err != nil {
			return fmt.Errorf("goon: Failed to encode field %v value %v - %v", fieldName, v.Interface(), err)
		}
	}
	return nil
}

func deserializeStruct(dst interface{}, b []byte) error {
	if len(b) == 0 {
		return fmt.Errorf("goon: Expected some data to deserialize, got none.")
	}

	v := reflect.Indirect(reflect.ValueOf(dst))
	t := v.Type()
	k := t.Kind()

	if k != reflect.Struct {
		return fmt.Errorf("goon: Expected struct, got instead: %v", k)
	}

	if header := b[0]; header == serializationStateEmpty {
		return datastore.ErrNoSuchEntity
	} else if header != serializationStateNormal {
		return fmt.Errorf("goon: Unrecognized cache header: %v", header)
	}

	smd := deserializeStructMetaData(b[1:])
	dataPos := 1 + smd.totalLength + len(smd.metaDatas)
	sd := getSerializationDecoder(b[dataPos:])
	defer freeSerializationDecoder(sd)
	structHistory := make(map[string]map[string]bool, 8)
	fieldMap, _ := getFieldInfoAndMetadata(t)

	for _, metaData := range smd.metaDatas {
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

		if err := deserializeStructInternal(sd.dec, fi, fieldName, nameParts, slice, zeroValue, structHistory, v, t); err != nil {
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
		if elemType.Kind() == reflect.Uint8 {
			if !zeroValue {
				if err := dec.DecodeValue(vf); err != nil {
					return fmt.Errorf("goon: Failed to decode field %v - %v", fieldName, err)
				}
			}
		} else {
			ev := reflect.New(elemType).Elem()
			if !zeroValue {
				if err := dec.DecodeValue(ev); err != nil {
					return fmt.Errorf("goon: Failed to decode field %v - %v", fieldName, err)
				}
			}
			vf.Set(reflect.Append(vf, ev))
		}
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
