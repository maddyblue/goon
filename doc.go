/*
Package goon provides an autocaching interface to the app engine datastore
similar to the python NDB package.

Goon differs from the datastore package in that it remembers the appengine
Context, which need only be specified once, at creation time. Goon has an
almost identical API, just designed to be simpler.

Keys in Goon are stored in the structs themselves. Below is an example struct
with a field to specify the id. (There are also optional kind and parent fields;
see docs on the KeyError() function.)
	type User struct {
		Id    string `datastore:"-" goon:"id"`
		Name  string
	}

Thus, to get a User with id 2:
	userid := 2
	g := goon.NewGoon(r)
	u := User{Id: userid}
	g.Get(&u)
*/
package goon
