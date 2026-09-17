---
name: split-file
description: Split a large Go file into smaller focused files while preserving git history. Use when a file is too large and needs to be broken into logical modules.
argument-hint: [filepath]
---

# Split a Go file into multiple files preserving git history

Split the file at `$ARGUMENTS` into multiple smaller files, grouped by logical concern, while preserving `git log --follow` history for each new file.

## Phase 1: Analyze

1. Read the file and catalog every function, method, type, const block, and var block with their line numbers.
2. Group them into logical categories (e.g., push path, query path, TSDB lifecycle, HTTP handlers, etc.).
3. Check existing files in the same package to understand naming conventions and avoid conflicts.
4. Propose the split to the user: list each new file, what goes in it, and approximate line count. Keep the original file for initialization, config, types, and lifecycle. Aim for at least 4 files.
5. Wait for user approval before proceeding.

## Phase 2: Split with history preservation

For each new file, repeat this sequence. Process files from largest to smallest.

### Git dance (creates the new file with full history)

```bash
# 1. Create temp branch from current branch
git checkout -b temp-split-SUFFIX $CURRENT_BRANCH

# 2. Rename original to new filename
git mv path/to/original.go path/to/new_file.go
git commit -m "temp: rename original.go to new_file.go"

# 3. Go back to working branch
git checkout $CURRENT_BRANCH

# 4. Rename original to a temp name (creates rename/rename conflict)
git mv path/to/original.go path/to/original_main_temp.go
git commit -m "temp: rename original.go to original_main_temp.go"

# 5. Merge — this triggers a CONFLICT (rename/rename), which is what we want
git merge temp-split-SUFFIX --no-commit
git checkout --ours path/to/original_main_temp.go
git checkout --theirs path/to/new_file.go
git add path/to/original_main_temp.go path/to/new_file.go
git rm --cached path/to/original.go 2>/dev/null
git commit -m "split: create new_file.go from original.go (preserving history)"

# 6. Rename temp back to original
git mv path/to/original_main_temp.go path/to/original.go
git commit -m "temp: rename original_main_temp.go back to original.go"

# 7. Cleanup
git branch -d temp-split-SUFFIX
```

### Content extraction

After the git dance, both files contain the full original content. Now trim them:

1. Write a python helper script that extracts specific line ranges from a file (for the new file) and removes those same ranges (for the original). This is much faster and less error-prone than manual editing.
2. Run `goimports -local <module_path> -w` on both files to fix imports.
3. If `goimports` resolves aliased imports incorrectly (e.g., `util_log`, `mimir_storage`, `promcfg`), fix them manually. This is common for packages with custom import aliases.
4. Run `go build ./path/to/package/...` to verify compilation.
5. Commit the trimmed files: `git commit -m "split: trim new_file.go to <description> code only"`

### Gotchas learned from experience

- **goimports resolves wrong packages**: When the codebase has aliased imports like `util_log "pkg/util/log"` or `mimir_storage "pkg/storage"`, `goimports` may resolve to a different package with the same base name (e.g., otel's `exemplar` instead of prometheus's `exemplar`). Always check the import block after running goimports.
- **Shared types across split boundaries**: Types like context keys or request state structs used by multiple split files should go in the file where they are primarily defined. They're accessible from other files in the same package.
- **The rename/rename conflict is essential**: A simple modify-on-one-side + rename-on-other-side does NOT create a conflict — git auto-merges it by following the rename, and you lose the original file. You MUST rename on BOTH sides to force the conflict.
- **Test files don't need splitting**: Tests in `_test.go` files work across all files in the same package.
- **Process largest extractions first**: This keeps the original file shrinking progressively and makes line number tracking easier.

## Phase 3: Verify

After all splits are done:

```bash
# Build
go build ./path/to/package/...

# Run tests
go test ./path/to/package/... -count=1 -short

# Verify history for each new file
for f in new_file1 new_file2 ...; do
  echo "=== $f ==="
  git log --follow --oneline "path/to/${f}.go" | tail -3
done
```

Each new file should show the full commit history from before the split.

### Per-declaration content hash verification

Use `tools/split-file-verify` to prove that every declaration was moved intact. The tool parses Go files using the AST, extracts every top-level declaration (functions, methods, types, var/const blocks), and outputs a sorted TSV of `name \t sha256_hash`.

```bash
# Build the tool
go build -o /tmp/split-file-verify ./tools/split-file-verify

# Hash every declaration in the original file (from the base commit)
git show <base-commit>:path/to/original.go > /tmp/original.go
/tmp/split-file-verify /tmp/original.go > /tmp/original_decls.tsv

# Hash every declaration across all split files
/tmp/split-file-verify path/to/file1.go path/to/file2.go ... > /tmp/split_decls.tsv

# Compare — should produce no output if everything matches
diff /tmp/original_decls.tsv /tmp/split_decls.tsv
```

If `diff` produces no output, every declaration has the exact same content in the split files as in the original. Post this as a PR comment so reviewers can verify without reading every line.
