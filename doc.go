/*
Package goon provides an autocaching interface to the app engine datastore
similar to the python NDB package.

Usage:

	type Group struct {
		Name string
	}

	func Test(w http.ResponseWriter, r *http.Request) {
		g := Group{
			Name: "test",
		}

		n := goon.NewGoon(r)

		// Create a new entity with an incomplete key and no parent.
		e, _ := n.NewEntity(nil, &g)
		fmt.Fprintln(w, "e with incomplete key:", e)

		// The kind name "Group" is fetched by reflecting on g.
		_ = n.Put(e)
		fmt.Fprintln(w, "e with key:", e)

		var g2 Group
		// Fetch it back.
		e2, _ := n.KeyGet(&g2, e.Key)
		fmt.Fprintln(w, "e2:", e2)

		_ = n.RunInTransaction(func(ng *goon.Goon) error {
			var g3 Group
			ne, _ := ng.KeyGet(&g3, e.Key)
			g3.Name = "new name"
			ng.Put(ne)
			return nil
		}, nil)

		e3, _ := n.KeyGet(&Group{}, e.Key)
		fmt.Fprintln(w, "e3:", e3)

		_ = n.Delete(e.Key)
		e4, _ := n.KeyGet(&Group{}, e.Key)
		fmt.Fprintln(w, "e4:", e4)
	}

Currently in early development: do not use in production.

Submit bugs and code to: https://github.com/mjibson/goon
*/
package goon
