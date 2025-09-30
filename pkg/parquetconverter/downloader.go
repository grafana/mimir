package parquetconverter

import (
	"context"
	"crypto/tls"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	s3mgr "github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	s3svc "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/grafana/mimir/pkg/storage/bucket/s3"
)

func downloaderFromConfig(ctx context.Context, cfg s3.Config) (*s3mgr.Downloader, error) {

	// Create AWS config
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

	// Configure HTTP client with settings from your config
	httpClient := &http.Client{
		Timeout: 30 * time.Second,
	}

	// Apply HTTP configuration
	if cfg.HTTP.InsecureSkipVerify {
		httpClient.Transport = &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		}
	}

	// Create S3 client
	s3Client := s3svc.NewFromConfig(awsConfig, func(o *s3svc.Options) {
		o.HTTPClient = httpClient
		o.UsePathStyle = true
	})

	// Create downloader with optimized settings
	downloader := s3mgr.NewDownloader(s3Client, func(d *s3mgr.Downloader) {
		// Use configured part size or default to 64MB
		if cfg.PartSize > 0 {
			d.PartSize = int64(cfg.PartSize)
		} else {
			d.PartSize = 64 * 1024 * 1024 // 64MB default
		}

		// Set concurrency for parallel downloads
		d.Concurrency = 10

		// Set buffer size for better performance
		//d.BufferProvider = s3mgr.NewBufferedReadSeekerWriteToPool(25 * 1024 * 1024) // 25MB buffer
	})

	return downloader, nil
}
