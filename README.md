# goon

An autocaching interface to the app engine datastore for Go. Designed to be similar to the python NDB package.

## The different flavors of App Engine

You must choose the goon major version based on which App Engine library you are using.

| App Engine Go library            | Include in your project     | Correct goon version                                                   |
|----------------------------------|-----------------------------|------------------------------------------------------------------------|
| `google.golang.org/appengine/v2` |`github.com/mjibson/goon/v2` | [goon v2.0.2](https://github.com/mjibson/goon/releases/tag/v2.0.2)     |
| `google.golang.org/appengine`    |`github.com/mjibson/goon`    | [goon v1.1.0](https://github.com/mjibson/goon/releases/tag/v1.1.0)     |
| `appengine`                      |`github.com/mjibson/goon`    | [goon v0.9.0](https://github.com/mjibson/goon/releases/tag/v0.9.0)     |
| `cloud.google.com/go`            |N/A                          | Not supported ([issue #74](https://github.com/mjibson/goon/issues/74)) |

## Documentation

[https://pkg.go.dev/github.com/mjibson/goon](https://pkg.go.dev/github.com/mjibson/goon)
