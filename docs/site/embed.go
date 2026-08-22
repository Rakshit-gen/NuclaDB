// Package site embeds the CLI documentation page so nucladbd can serve it
// directly (see internal/api/gateway) — docs/site/index.html stays the
// single copy, readable standalone or served by the running binary.
package site

import _ "embed"

//go:embed index.html
var Index []byte
