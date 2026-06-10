// SPDX-License-Identifier: AGPL-3.0-only

package objtools

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
)

// tmpWriteDirName is the name of a directory under the bucket root used to hold
// partially written objects before they are renamed into place. It is reserved and
// excluded from bucket operations
const tmpWriteDirName = ".objtools-tmp" // nolint:gosec // false positive for G101: Potential hardcoded credentials

var (
	errFilesystemVersioningUnsupported = errors.New("the filesystem backend does not support versioning")
	errTmpWriteDirReserved             = fmt.Errorf("object names under %q are reserved by the filesystem backend", tmpWriteDirName)
	errObjectIsDirectory               = errors.New("object name refers to an existing directory")
	errNonCanonicalName                = errors.New("object name is not in canonical form: it must not contain empty, \".\", or \"..\" segments, repeated delimiters, or a trailing delimiter")
)

type FilesystemClientConfig struct {
	Directory string
}

func (c *FilesystemClientConfig) RegisterFlags(prefix string, f *flag.FlagSet) {
	f.StringVar(&c.Directory, prefix+"directory", "", "The directory that will be treated as if it were the root of an object storage bucket.")
}

func (c *FilesystemClientConfig) Validate(prefix string) error {
	if c.Directory == "" {
		return fmt.Errorf("the filesystem directory (%s) is required", prefix+"directory")
	}
	// A directory that doesn't exist yet is fine, but it can't be a file
	info, err := os.Stat(c.Directory)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("the filesystem directory (%s) is not a directory: %s", prefix+"directory", c.Directory)
	}
	return nil
}

func (c *FilesystemClientConfig) ToBucket() (Bucket, error) {
	return &fsBucket{
		directory: filepath.Clean(c.Directory),
	}, nil
}

// fsBucket is a Bucket implementation backed by a local filesystem. Object names are mapped to
// paths relative to the configured directory.
//
// Intended for limited use by tools/testing only.
//
// Known limitations:
//   - The filesystem has no notion of versioning, so versioned operations are not supported.
//   - Files within the temporary write directory and empty directories may leak.
//   - No fsync is performed.
//   - Object names that do not map to canonical paths are rejected.
//   - Containment is enforced lexically via the canonical name check, so a symlink
//     within the directory that points elsewhere can still escape the bucket.
type fsBucket struct {
	directory string
}

func (bkt *fsBucket) fsPath(objectName string) string {
	return filepath.Join(bkt.directory, filepath.FromSlash(objectName))
}

func (bkt *fsBucket) tempWriteDir() string {
	return filepath.Join(bkt.directory, tmpWriteDirName)
}

// isCanonicalName reports whether name is in canonical slash-separated form: no
// empty, ".", or ".." segments and no repeated delimiters. When allowTrailingDelim
// is true a single trailing delimiter is permitted, as list prefixes may carry one.
// This is the single source of truth for what makes a name well-formed, and because
// it rejects ".." segments it also guarantees a mapped path cannot escape the bucket.
func isCanonicalName(name string, allowTrailingDelim bool) bool {
	if allowTrailingDelim {
		name = strings.TrimSuffix(name, Delim)
	}
	// Anchor with a leading delimiter so path.Clean treats name as absolute; "."
	// and ".." segments then change the cleaned result and fail the comparison.
	return name != "" && path.Clean(Delim+name) == Delim+name
}

// isTemp reports whether fsPath is the temporary write directory or lies within it.
func (bkt *fsBucket) isTemp(fsPath string) bool {
	staging := bkt.tempWriteDir()
	return fsPath == staging || strings.HasPrefix(fsPath, staging+string(filepath.Separator))
}

// resolveObject validates objectName as an object key and maps it to its
// filesystem path. The name must be canonical and must not fall in the temporary write directory.
func (bkt *fsBucket) resolveObject(objectName string) (string, error) {
	if !isCanonicalName(objectName, false) {
		return "", errNonCanonicalName
	}
	fsPath := bkt.fsPath(objectName)
	if bkt.isTemp(fsPath) {
		return "", errTmpWriteDirReserved
	}
	return fsPath, nil
}

// resolvePrefix validates a list prefix and maps it to the filesystem path of the
// directory to list. An empty prefix maps to the bucket root; a non-empty prefix
// must be canonical and may carry a trailing delimiter.
func (bkt *fsBucket) resolvePrefix(prefix string) (string, error) {
	if prefix != "" && !isCanonicalName(prefix, true) {
		return "", errNonCanonicalName
	}
	fsPath := bkt.fsPath(ensureDelimiterSuffix(prefix))
	if bkt.isTemp(fsPath) {
		return "", errTmpWriteDirReserved
	}
	return fsPath, nil
}

func (bkt *fsBucket) ensureNotDirectory(fsPath string) error {
	info, err := os.Stat(fsPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	if info.IsDir() {
		return errObjectIsDirectory
	}
	return nil
}

func (bkt *fsBucket) Get(_ context.Context, objectName string, options GetOptions) (io.ReadCloser, error) {
	if options.VersionID != "" {
		return nil, errFilesystemVersioningUnsupported
	}
	fsPath, err := bkt.resolveObject(objectName)
	if err != nil {
		return nil, err
	}
	if err := bkt.ensureNotDirectory(fsPath); err != nil {
		return nil, err
	}
	return os.Open(fsPath)
}

func (bkt *fsBucket) Exists(_ context.Context, objectName string, options ExistsOptions) (bool, error) {
	if options.VersionID != "" {
		return false, errFilesystemVersioningUnsupported
	}
	fsPath, err := bkt.resolveObject(objectName)
	if err != nil {
		return false, err
	}
	info, err := os.Stat(fsPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, err
	}
	return !info.IsDir(), nil
}

func (bkt *fsBucket) ServerSideCopy(_ context.Context, objectName string, dstBucket Bucket, options CopyOptions) error {
	if options.SourceVersionID != "" {
		return errFilesystemVersioningUnsupported
	}
	d, ok := dstBucket.(*fsBucket)
	if !ok {
		return errors.New("destination Bucket wasn't a filesystem Bucket")
	}
	src, err := bkt.resolveObject(objectName)
	if err != nil {
		return err
	}
	dst, err := d.resolveObject(options.destinationObjectName(objectName))
	if err != nil {
		return err
	}
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	// Stage through the destination bucket: dst lives under d's directory, so the
	// temporary write directory and rename must happen there too.
	return d.stagedWrite(dst, func(w io.Writer) error {
		_, err := io.Copy(w, in)
		return err
	})
}

func (bkt *fsBucket) ClientSideCopy(ctx context.Context, objectName string, dstBucket Bucket, options CopyOptions) error {
	if options.SourceVersionID != "" {
		return errFilesystemVersioningUnsupported
	}
	src, err := bkt.resolveObject(objectName)
	if err != nil {
		return err
	}
	info, err := os.Stat(src)
	if err != nil {
		return fmt.Errorf("failed to stat filesystem source object: %w", err)
	}
	reader, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("failed to open filesystem source object: %w", err)
	}
	if err := dstBucket.Upload(ctx, options.destinationObjectName(objectName), reader, info.Size()); err != nil {
		_ = reader.Close()
		return fmt.Errorf("failed to upload filesystem source object to destination: %w", err)
	}
	if err := reader.Close(); err != nil {
		return fmt.Errorf("failed closing filesystem source object reader: %w", err)
	}
	return nil
}

func (bkt *fsBucket) List(_ context.Context, options ListOptions) (*ListResult, error) {
	root, err := bkt.resolvePrefix(options.Prefix)
	if err != nil {
		return nil, err
	}

	if options.Recursive {
		return bkt.listRecursive(root)
	}
	return bkt.listShallow(ensureDelimiterSuffix(options.Prefix), root)
}

// listShallow lists the immediate children of dir, returning files as objects and
// subdirectories as prefixes
func (bkt *fsBucket) listShallow(prefix string, dir string) (*ListResult, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return &ListResult{Objects: []ObjectAttributes{}, Prefixes: []string{}}, nil
		}
		return nil, err
	}

	objects := make([]ObjectAttributes, 0, len(entries))
	prefixes := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			// Never surface the reserved staging directory as a prefix
			if bkt.isTemp(filepath.Join(dir, entry.Name())) {
				continue
			}
			prefixes = append(prefixes, prefix+entry.Name()+Delim)
			continue
		}
		info, err := entry.Info()
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return nil, err
		}
		objects = append(objects, ObjectAttributes{
			Name:         prefix + entry.Name(),
			Size:         info.Size(),
			LastModified: info.ModTime(),
		})
	}
	return &ListResult{Objects: objects, Prefixes: prefixes}, nil
}

// listRecursive lists every file in the subtree of dir
func (bkt *fsBucket) listRecursive(dir string) (*ListResult, error) {
	objects := make([]ObjectAttributes, 0, 10)
	err := filepath.WalkDir(dir, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			return err
		}
		if entry.IsDir() {
			// Ignore the staging directory
			if bkt.isTemp(path) {
				return filepath.SkipDir
			}
			return nil
		}
		rel, err := filepath.Rel(bkt.directory, path)
		if err != nil {
			return err
		}
		info, err := entry.Info()
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			return err
		}
		objects = append(objects, ObjectAttributes{
			Name:         filepath.ToSlash(rel),
			Size:         info.Size(),
			LastModified: info.ModTime(),
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &ListResult{Objects: objects, Prefixes: []string{}}, nil
}

func (bkt *fsBucket) RestoreVersion(context.Context, string, VersionInfo) error {
	return errFilesystemVersioningUnsupported
}

func (bkt *fsBucket) Upload(_ context.Context, objectName string, reader io.Reader, _ int64) error {
	fsPath, err := bkt.resolveObject(objectName)
	if err != nil {
		return err
	}
	return bkt.stagedWrite(fsPath, func(w io.Writer) error {
		if _, err := io.Copy(w, reader); err != nil {
			return fmt.Errorf("failed writing filesystem object: %w", err)
		}
		return nil
	})
}

func (bkt *fsBucket) Delete(_ context.Context, objectName string, options DeleteOptions) error {
	if options.VersionID != "" {
		return errFilesystemVersioningUnsupported
	}
	fsPath, err := bkt.resolveObject(objectName)
	if err != nil {
		return err
	}
	if err := bkt.ensureNotDirectory(fsPath); err != nil {
		return err
	}
	if err := os.Remove(fsPath); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	bkt.pruneEmptyParents(filepath.Dir(fsPath))
	return nil
}

func (bkt *fsBucket) pruneEmptyParents(dir string) {
	for dir != bkt.directory && len(dir) > len(bkt.directory) {
		// os.Remove only deletes a directory when it is empty
		if err := os.Remove(dir); err != nil {
			return
		}
		dir = filepath.Dir(dir)
	}
}

func (bkt *fsBucket) Name() string {
	return "filesystem"
}

// stagedWrite writes content to dst by staging it under bkt's temporary write directory and
// renaming it into place to prevent partial write appearing in dst. dst must already
// be resolved via resolveObject and belong to bkt.
func (bkt *fsBucket) stagedWrite(dst string, write func(io.Writer) error) (retErr error) {
	if err := bkt.ensureNotDirectory(dst); err != nil {
		return err
	}
	tmpDir := bkt.tempWriteDir()
	if err := os.MkdirAll(tmpDir, 0o755); err != nil {
		return err
	}
	tmpPath, err := writeToTemp(tmpDir, write)
	if err != nil {
		return err
	}
	defer func() {
		if retErr != nil {
			_ = os.Remove(tmpPath)
			bkt.pruneEmptyParents(filepath.Dir(dst))
		}
	}()
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	return os.Rename(tmpPath, dst)
}

func writeToTemp(dir string, write func(io.Writer) error) (string, error) {
	tmp, err := os.CreateTemp(dir, "tmp-*")
	if err != nil {
		return "", err
	}
	tmpPath := tmp.Name()
	if err := write(tmp); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
		return "", err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return "", err
	}
	return tmpPath, nil
}
