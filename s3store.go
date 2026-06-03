package loge

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// S3Config configures an S3Store. Credentials come from the standard AWS chain
// (environment, shared config, IAM role) — never from flags.
type S3Config struct {
	Bucket         string
	Prefix         string
	Endpoint       string // custom endpoint for S3-compatible stores (MinIO); empty = AWS
	Region         string
	ForcePathStyle bool
	ReadURLBase    string // public/CDN base for reads; empty = derived
	ACL            string // optional canned ACL, e.g. "public-read"
}

// S3Store is an ObjectStore backed by S3 (or any S3-compatible endpoint) via
// aws-sdk-go-v2. Reads happen out-of-band as plain public HTTP GETs (see
// ReadURLBase), so the store is only used for the write/verify path.
type S3Store struct {
	client      *s3.Client
	uploader    *manager.Uploader
	bucket      string
	readURLBase string
	acl         string
}

// NewS3Store builds an S3Store from cfg.
func NewS3Store(ctx context.Context, cfg S3Config) (*S3Store, error) {
	var loadOpts []func(*awsconfig.LoadOptions) error
	if cfg.Region != "" {
		loadOpts = append(loadOpts, awsconfig.WithRegion(cfg.Region))
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return nil, fmt.Errorf("could not load aws config: %w", err)
	}

	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		}

		o.UsePathStyle = cfg.ForcePathStyle
	})

	readURLBase := strings.TrimRight(cfg.ReadURLBase, "/")
	if readURLBase == "" {
		readURLBase = deriveReadURLBase(cfg)
	}

	return &S3Store{
		client:      client,
		uploader:    manager.NewUploader(client),
		bucket:      cfg.Bucket,
		readURLBase: readURLBase,
		acl:         cfg.ACL,
	}, nil
}

// deriveReadURLBase builds a path-based public read base when one is not given.
// Reads must be path-based (no query params) because go-sqlite3 strips the DSN
// query string before the VFS sees it.
func deriveReadURLBase(cfg S3Config) string {
	if cfg.Endpoint != "" {
		return strings.TrimRight(cfg.Endpoint, "/") + "/" + cfg.Bucket
	}

	region := cfg.Region
	if region == "" {
		region = "us-east-1"
	}

	return fmt.Sprintf("https://%s.s3.%s.amazonaws.com", cfg.Bucket, region)
}

// Put uploads localPath under key (multipart) and returns its public read URL.
func (s *S3Store) Put(ctx context.Context, key, localPath string) (string, error) {
	file, err := os.Open(localPath) //nolint: gosec
	if err != nil {
		return "", fmt.Errorf("could not open segment: %w", err)
	}
	defer func() {
		_ = file.Close()
	}()

	input := &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Body:   file,
	}

	if s.acl != "" {
		input.ACL = types.ObjectCannedACL(s.acl)
	}

	if _, err := s.uploader.Upload(ctx, input); err != nil {
		return "", fmt.Errorf("could not upload segment: %w", err)
	}

	return s.ReadURL(key), nil
}

// ReadURL returns the public, path-based HTTP URL a key is read back from.
func (s *S3Store) ReadURL(key string) string {
	return s.readURLBase + "/" + key
}

// List returns every object key stored under prefix, paginating through the
// bucket listing.
func (s *S3Store) List(ctx context.Context, prefix string) ([]string, error) {
	var keys []string

	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(prefix),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("could not list objects: %w", err)
		}

		for _, object := range page.Contents {
			if object.Key != nil {
				keys = append(keys, *object.Key)
			}
		}
	}

	return keys, nil
}

// Size returns the stored object's size via HeadObject.
func (s *S3Store) Size(ctx context.Context, key string) (int64, error) {
	out, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return 0, fmt.Errorf("could not head object: %w", err)
	}

	if out.ContentLength == nil {
		return 0, nil
	}

	return *out.ContentLength, nil
}
