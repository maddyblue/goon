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
	"appengine/datastore"
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"reflect"
	"strings"
)

func toGob(src interface{}) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	gob.Register(src)
	err := enc.Encode(src)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func fromGob(src interface{}, b []byte) error {
	var buf bytes.Buffer
	_, _ = buf.Write(b)
	gob.Register(src)
	dec := gob.NewDecoder(&buf)
	err := dec.Decode(src)
	if err != nil {
		return err
	}
	return nil
}

func (g *Goon) getStructKey(src interface{}) (*datastore.Key, error) {
	v := reflect.Indirect(reflect.ValueOf(src))
	t := v.Type()
	k := t.Kind()

	if k != reflect.Struct {
		return nil, errors.New(fmt.Sprintf("goon: Expected struct, got instead: %v", k))
	}

	var parent *datastore.Key
	var stringID string
	var intID int64
	var kind string
	foundSelf := false

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
						return nil, errors.New("goon: Only one field may be marked id")
					}
					intID = vf.Int()
				case reflect.String:
					if stringID != "" || intID != 0 {
						return nil, errors.New("goon: Only one field may be marked id")
					}
					stringID = vf.String()
				}
			} else if tagValue == "kind" {
				if vf.Kind() == reflect.String {
					if kind != "" {
						return nil, errors.New("goon: Only one field may be marked kind")
					}
					kind = vf.String()
					if kind == "" && len(tagValues) > 1 && tagValues[1] != "" {
						kind = tagValues[1]
					}
				}
			} else if tagValue == "parent" {
				if vf.Type() == reflect.TypeOf(&datastore.Key{}) {
					if parent != nil {
						return nil, errors.New("goon: Only one field may be marked parent")
					}
					parent = vf.Interface().(*datastore.Key)
				}
			} else if tagValue == "self" {
				if foundSelf {
					return nil, errors.New("goon: Only one field may be marked as self")
				}
				key := vf.Interface().(*datastore.Key)
				if key != nil {
					return key, nil
				}
				foundSelf = true
			}
		}
	}

	// if kind has not been manually set, fetch it from src's type
	if kind == "" {
		kind = typeName(src)
	}
	return datastore.NewKey(g.context, kind, stringID, intID, parent), nil
	// can be an incomplete Key
}

func typeName(src interface{}) string {
	v := reflect.ValueOf(src)
	v = reflect.Indirect(v)
	t := v.Type()
	return t.Name()
}

func setStructKey(src interface{}, key *datastore.Key) error {
	v := reflect.Indirect(reflect.ValueOf(src))
	t := v.Type()
	k := t.Kind()

	if k != reflect.Struct {
		return errors.New(fmt.Sprintf("goon: Expected struct, got instead: %v", k))
	}

	idSet := false
	kindSet := false
	parentSet := false
	selfSet := false
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
					return errors.New("goon: Only one field may be marked id")
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
					return errors.New("goon: Only one field may be marked kind")
				}
				if vf.Kind() == reflect.String {
					if (len(tagValues) <= 1 || key.Kind() != tagValues[1]) && typeName(src) != key.Kind() {
						vf.Set(reflect.ValueOf(key.Kind()))
					}
					kindSet = true
				}
			} else if tagValue == "parent" {
				if parentSet {
					return errors.New("goon: Only one field may be marked parent")
				}
				if vf.Type() == reflect.TypeOf(&datastore.Key{}) {
					vf.Set(reflect.ValueOf(key.Parent()))
					parentSet = true
				}
			} else if tagValue == "self" {
				if selfSet {
					return errors.New("goon: Only one field may be marked self")
				}
				if vf.Type() == reflect.TypeOf(&datastore.Key{}) {
					vf.Set(reflect.ValueOf(key))
					selfSet = true
				}
			}
		}
	}

	if !idSet {
		return errors.New("goon: Could not set id field")
	}

	return nil
}
