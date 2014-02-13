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
	"time"

	"appengine/datastore"
)

const (
	serializationStateEmpty  = 0x00
	serializationStateNormal = 0x01
)

var timeType = reflect.TypeOf(time.Time{})

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

	var buf bytes.Buffer
	buf.WriteByte(serializationStateNormal) // Set the header
	enc := gob.NewEncoder(&buf)

	if err := serializeStructInternal(enc, "", v, t); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func serializeStructInternal(enc *gob.Encoder, namePrefix string, v reflect.Value, t reflect.Type) error {
	for i := 0; i < v.NumField(); i++ {
		vf := v.Field(i)
		if !vf.CanSet() {
			continue
		}

		tf := t.Field(i)
		fieldName := tf.Name
		tag := tf.Tag.Get("datastore")
		if len(tag) > 0 {
			fieldName = strings.Split(tag, ",")[0]
			if fieldName == "-" {
				continue
			}
		}
		if namePrefix != "" {
			fieldName = namePrefix + fieldName
		}

		if vf.Kind() == reflect.Slice {
			elemType := vf.Type().Elem()
			if elemType.Kind() == reflect.Struct && elemType != timeType {
				for j := 0; j < vf.Len(); j++ {
					vi := vf.Index(j)
					if err := serializeStructInternal(enc, fieldName+".", vi, vi.Type()); err != nil {
						return err
					}
				}
				continue
			}
		}

		if vf.Kind() == reflect.Struct && tf.Type != timeType {
			if err := serializeStructInternal(enc, fieldName+".", vf, vf.Type()); err != nil {
				return err
			}
			continue
		}

		metaData := fieldName
		if vf.Kind() == reflect.Slice {
			// When decoding, if the target is a slice but metadata doesn't have the $ sign,
			// then we can properly create a single value slice instead of panicing
			metaData = "$" + fieldName
		}

		if err := enc.Encode(metaData); err != nil {
			return fmt.Errorf("goon: Failed to encode field metadata %v - %v", metaData, err)
		}
		if err := enc.EncodeValue(vf); err != nil {
			return fmt.Errorf("goon: Failed to encode field %v value %v - %v", fieldName, vf.Interface(), err)
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
	structHistory := make(map[string]map[string]bool)

	for buf.Len() > 0 {
		if err := dec.Decode(&metaData); err != nil {
			return fmt.Errorf("goon: Failed to decode field metadata: %v", err)
		}

		fieldName, slice := metaData, false
		if metaData[0] == '$' {
			fieldName, slice = metaData[1:], true
		}
		nameParts := strings.Split(fieldName, ".")

		if done, err := deserializeStructInternal(dec, nameParts, 0, slice, structHistory, v, t); err != nil {
			return err
		} else if !done {
			return fmt.Errorf("goon: Could not find field %v", fieldName)
		}
	}

	return nil
}

func deserializeStructInternal(dec *gob.Decoder, nameParts []string, nameIdx int, slice bool, structHistory map[string]map[string]bool, v reflect.Value, t reflect.Type) (bool, error) {
	relativeName := strings.Join(nameParts[nameIdx:], ".")

	for i := 0; i < v.NumField(); i++ {
		vf := v.Field(i)
		if !vf.CanSet() {
			continue
		}

		tf := t.Field(i)
		fieldName := tf.Name
		tag := tf.Tag.Get("datastore")
		if len(tag) > 0 {
			fieldName = strings.Split(tag, ",")[0]
			if fieldName == "-" {
				continue
			}
		}

		if fieldName == relativeName {
			if vf.Kind() == reflect.Slice && !slice {
				elemType := vf.Type().Elem()
				ev := reflect.New(elemType).Elem()

				if err := dec.DecodeValue(ev); err != nil {
					return true, fmt.Errorf("goon: Failed to decode field %v - %v", strings.Join(nameParts, "."), err)
				}

				vf.Set(reflect.Append(vf, ev))
			} else {
				if err := dec.DecodeValue(vf); err != nil {
					return true, fmt.Errorf("goon: Failed to decode field %v - %v", strings.Join(nameParts, "."), err)
				}
			}

			return true, nil
		}

		if fieldName == nameParts[nameIdx] && (vf.Kind() == reflect.Struct || (vf.Kind() == reflect.Slice && vf.Type().Elem().Kind() == reflect.Struct)) {
			if vf.Kind() == reflect.Slice {
				var sv reflect.Value
				createNew := false
				absName, childName := strings.Join(nameParts[:nameIdx+1], "."), strings.Join(nameParts[nameIdx+1:], ".")
				sh, ok := structHistory[absName]
				if !ok || sh[childName] {
					sh = make(map[string]bool)
					structHistory[absName] = sh
					createNew = true
				} else if len(sh) == 0 {
					createNew = true
				}

				if createNew {
					structType := vf.Type().Elem()
					sv = reflect.New(structType).Elem()
				} else {
					sv = vf.Index(vf.Len() - 1)
				}

				if found, err := deserializeStructInternal(dec, nameParts, nameIdx+1, slice, structHistory, sv, sv.Type()); err != nil {
					return found, err
				} else if found {
					if createNew {
						vf.Set(reflect.Append(vf, sv))
					}
					sh[childName] = true
					return true, nil
				}
			} else if found, err := deserializeStructInternal(dec, nameParts, nameIdx+1, slice, structHistory, vf, vf.Type()); found || err != nil {
				return found, err
			}
		}
	}

	return false, nil
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
