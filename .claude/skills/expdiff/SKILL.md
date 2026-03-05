---
name: expdiff
description: Regenerate .pb.go.expdiff files after proto-generated Go files have changed.
---

# Regenerate expdiff

Regenerate the expected-diff files for protobuf-generated Go code. Run:

```bash
make pkg/mimirpb/mimir.pb.go.expdiff
```

If `$ARGUMENTS` is provided, use it as the target instead (e.g., `pkg/other/foo.pb.go.expdiff`):

```bash
make $ARGUMENTS
```

After the command completes, stage the updated `.expdiff` file(s).
