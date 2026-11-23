This is the Go 1.26.4 `encoding/json` v1 encoder, copied under its BSD
license (see LICENSE).

`_upstream/` contains byte-for-byte copies of `encode.go`, `fold.go`,
`indent.go`, `scanner.go`, `tables.go`, and `tags.go` from:
https://github.com/golang/go/tree/go1.26.4/src/encoding/json

`sdk.patch` records all changes to those files. The corresponding files in
this directory are generated and checked in so ordinary Go module consumers
need neither a patch tool nor a generation step. Go ignores `_upstream/`
when discovering packages. SDK-specific entry points and helpers live in
`sdk.go`; regression tests live in `sdk_test.go`.

From the SDK module root, regenerate or verify the checked-in files with:

```sh
go generate ./internal/json
sh internal/json/generate.sh --check
```

The Nix build also runs `sh internal/json/generate.sh --check-upstream`,
which compares every vendored upstream file with `go env GOROOT` before
checking the generated files. Run this with the selected Go release when
updating the fork. The ordinary `--check` remains usable with older supported
Go toolchains; it verifies the patch output without requiring their standard
library sources to match the vendored release.

Generation requires `sh` and `patch`. To change the fork, edit `sdk.patch`
and regenerate; do not edit the generated files directly. When updating Go,
replace the six upstream files with untouched copies from the selected
release, update the version above, and rebase the patch. Run the consistency
check and the SDK tests before committing the result.

The SDK needs to change numeric emission while preserving struct field
selection, custom marshalers, and addressability. The standard v1 encoder
has no hook for this; an independent reflection walk duplicates those rules.

Local changes:

- Replace the standard-library build constraint with the SDK no-op constraint
  and remove the unused `unsafe` linkname import.
- Replace `reflect.TypeAssert[T](v)` with ordinary assertions on
  `v.Interface()` so the SDK still builds on Go 1.24.
- Alias `Number` to `encoding/json.Number` and add the SDK entry points.
- Replace non-finite floats and record typed numeric warnings in encoder
  state. Quoted integers and object keys do not produce numeric warnings.
- Render errors with `%+v`, after JSON marshalers and before text marshalers,
  including named byte elements that otherwise use the base64 fast path.
- Merge SDK-generated fields into the root object in the same traversal:
  root mapping fields they shadow are excluded before their values are
  visited, and the generated fields follow the remaining entries.

Custom `MarshalJSON` output is opaque: it retains the standard encoder's
validation and is not assigned typed-number diagnostics after type information
has been lost. Warning flags are discarded when serialization fails.

Regression tests cover field dominance, pointer-receiver methods,
`omitzero`, `,string`, and exclusion before callbacks.
