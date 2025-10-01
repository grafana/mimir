package parquetconverter

import (
	"context"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	s3mgr "github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	s3svc "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/efficientgo/core/logerrcapture"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid/v2"
	"github.com/pkg/errors"
	"github.com/thanos-io/objstore"
	"golang.org/x/sync/errgroup"

	"github.com/grafana/mimir/pkg/storage/bucket/s3"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
)

func downloaderFromConfig(ctx context.Context, cfg s3.Config) (downloader, error) {
	awsConfig, err := config.LoadDefaultConfig(ctx, config.WithRegion(cfg.Region))
	if err != nil {
		return nil, err
	}

	// Override credentials if provided
	if cfg.AccessKeyID != "" && cfg.SecretAccessKey.String() != "" {
		awsConfig.Credentials = credentials.NewStaticCredentialsProvider(
			cfg.AccessKeyID,
			cfg.SecretAccessKey.String(),
			cfg.SessionToken.String(),
		)
	}

	s3Client := s3svc.NewFromConfig(awsConfig, func(o *s3svc.Options) {
		o.UsePathStyle = true
	})

	downloader := s3mgr.NewDownloader(s3Client, func(d *s3mgr.Downloader) {
		if cfg.PartSize > 0 {
			d.PartSize = int64(cfg.PartSize)
		} else {
			d.PartSize = 128 * 1024 * 1024 // 128MB default
		}

		// Set concurrency for parallel downloads
		d.Concurrency = 10
	})

	return &fastDownloader{downloader: downloader, bucketName: cfg.BucketName}, nil
}

type fastDownloader struct {
	downloader *s3mgr.Downloader
	bucketName string
}

type downloader interface {
	download(context.Context, log.Logger, objstore.Bucket, string, ulid.ULID, string, ...downloadOption) error
}

var _ downloader = &fastDownloader{}

// Download downloads a directory meant to be a block directory. If any one of the files
// has a hash calculated in the meta file and it matches with what is in the destination path then
// we do not download it. We always re-download the meta file.
func (d *fastDownloader) download(ctx context.Context, logger log.Logger, bucket objstore.Bucket, prefix string, id ulid.ULID, dst string, options ...downloadOption) error {
	if err := os.MkdirAll(dst, 0750); err != nil {
		return errors.Wrap(err, "create dir")
	}

	if err := d.downloadFile(ctx, logger, bucket, path.Join(prefix, id.String(), block.MetaFilename), filepath.Join(dst, block.MetaFilename)); err != nil {
		return err
	}

	ignoredPaths := []string{block.MetaFilename}
	src := path.Join(prefix, id.String())
	if err := d.downloadDir(ctx, logger, bucket, src, src, dst, append(options, withDownloadIgnoredPaths(ignoredPaths...))...); err != nil {
		return err
	}

	chunksDir := filepath.Join(dst, block.ChunksDirname)
	_, err := os.Stat(chunksDir)
	if os.IsNotExist(err) {
		// This can happen if block is empty. We cannot easily upload empty directory, so create one here.
		return os.Mkdir(chunksDir, os.ModePerm)
	}
	if err != nil {
		return errors.Wrapf(err, "stat %s", chunksDir)
	}

	return nil
}

// DownloadFile downloads the src file from the bucket to dst. If dst is an existing
// directory, a file with the same name as the source is created in dst.
// If destination file is already existing, download file will overwrite it.
func (d *fastDownloader) downloadFile(ctx context.Context, logger log.Logger, bkt objstore.BucketReader, src, dst string) (err error) {
	if fi, err := os.Stat(dst); err == nil {
		if fi.IsDir() {
			dst = filepath.Join(dst, filepath.Base(src))
		}
	} else if !os.IsNotExist(err) {
		return err
	}

	f, err := os.Create(dst)
	if err != nil {
		return errors.Wrapf(err, "create file %s", dst)
	}
	defer func() {
		if err != nil {
			if rerr := os.Remove(dst); rerr != nil {
				level.Warn(logger).Log("msg", "failed to remove partially downloaded file", "file", dst, "err", rerr)
			}
		}
	}()
	defer logerrcapture.Do(logger, f.Close, "close block's output file")

	_, err = d.downloader.Download(ctx, f, &s3svc.GetObjectInput{
		Bucket: aws.String(d.bucketName),
		Key:    aws.String(src),
	})
	if err != nil {
		return errors.Wrapf(err, "download file %s -> %s", d.bucketName, src)
	}
	return nil
}

// DownloadOption configures the provided params.
type downloadOption func(params *downloadParams)

func withDownloadIgnoredPaths(ignoredPaths ...string) downloadOption {
	return func(params *downloadParams) {
		params.ignoredPaths = ignoredPaths
	}
}

// downloadParams holds the DownloadDir() parameters and is used by objstore clients implementations.
type downloadParams struct {
	concurrency  int
	ignoredPaths []string
}

func applyDownloadOptions(options ...downloadOption) downloadParams {
	out := downloadParams{
		concurrency: 1,
	}
	for _, opt := range options {
		opt(&out)
	}
	return out
}

// DownloadDir downloads all object found in the directory into the local directory.
func (d *fastDownloader) downloadDir(ctx context.Context, logger log.Logger, bkt objstore.BucketReader, originalSrc, src, dst string, options ...downloadOption) error {
	if err := os.MkdirAll(dst, 0750); err != nil {
		return errors.Wrap(err, "create dir")
	}
	opts := applyDownloadOptions(options...)

	// The derived Context is canceled the first time a function passed to Go returns a non-nil error or the first
	// time Wait returns, whichever occurs first.
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(opts.concurrency)

	var downloadedFiles []string
	var m sync.Mutex

	err := bkt.Iter(ctx, src, func(name string) error {
		g.Go(func() error {
			dst := filepath.Join(dst, filepath.Base(name))
			if strings.HasSuffix(name, objstore.DirDelim) {
				if err := d.downloadDir(ctx, logger, bkt, originalSrc, name, dst, options...); err != nil {
					return err
				}
				m.Lock()
				downloadedFiles = append(downloadedFiles, dst)
				m.Unlock()
				return nil
			}
			for _, ignoredPath := range opts.ignoredPaths {
				if ignoredPath == strings.TrimPrefix(name, string(originalSrc)+objstore.DirDelim) {
					level.Debug(logger).Log("msg", "not downloading again because a provided path matches this one", "file", name)
					return nil
				}
			}
			if err := d.downloadFile(ctx, logger, bkt, name, dst); err != nil {
				return err
			}

			m.Lock()
			downloadedFiles = append(downloadedFiles, dst)
			m.Unlock()
			return nil
		})
		return nil
	})

	if err == nil {
		err = g.Wait()
	}

	if err != nil {
		downloadedFiles = append(downloadedFiles, dst) // Last, clean up the root dst directory.
		// Best-effort cleanup if the download failed.
		for _, f := range downloadedFiles {
			if rerr := os.RemoveAll(f); rerr != nil {
				level.Warn(logger).Log("msg", "failed to remove file on partial dir download error", "file", f, "err", rerr)
			}
		}
		return err
	}

	return nil
}
