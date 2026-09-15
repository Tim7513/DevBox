// Package migrations embeds the SQL schema migrations so that every binary
// (apiserver, scheduler, worker, devboxctl) ships with the schema it expects
// and can verify or apply it at boot without a separate migration tool or a
// file path that has to exist in the container image.
package migrations

import "embed"

//go:embed *.sql
var FS embed.FS
