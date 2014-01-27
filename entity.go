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

	"appengine/datastore"
)

func toGob(src interface{}) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	gob.Register(reflect.Indirect(reflect.ValueOf(src)).Interface())
	if err := enc.Encode(src); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func fromGob(src interface{}, b []byte) error {
	buf := bytes.NewBuffer(b)
	gob.Register(reflect.Indirect(reflect.ValueOf(src)).Interface())
	dec := gob.NewDecoder(buf)
	if err := dec.Decode(src); err != nil {
		return err
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
