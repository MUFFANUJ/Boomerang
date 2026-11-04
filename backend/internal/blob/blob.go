// Package blob offloads large payloads to S3-compatible object storage (MinIO). Kafka
// messages then carry only a reference.
package blob

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// Store is an object store bucket.
type Store struct {
	cl     *minio.Client
	bucket string
}

// New connects to MinIO/S3.
func New(endpoint, access, secret, bucket string, useSSL bool) (*Store, error) {
	cl, err := minio.New(endpoint, &minio.Options{Creds: credentials.NewStaticV4(access, secret, ""), Secure: useSSL})
	if err != nil {
		return nil, err
	}
	return &Store{cl: cl, bucket: bucket}, nil
}

// EnsureBucket creates the bucket if missing.
func (s *Store) EnsureBucket(ctx context.Context) error {
	ok, err := s.cl.BucketExists(ctx, s.bucket)
	if err != nil {
		return err
	}
	if ok {
		return nil
	}
	return s.cl.MakeBucket(ctx, s.bucket, minio.MakeBucketOptions{})
}

// Put stores a payload and returns its reference.
func (s *Store) Put(ctx context.Context, key string, b []byte) (string, error) {
	_, err := s.cl.PutObject(ctx, s.bucket, key, bytes.NewReader(b), int64(len(b)), minio.PutObjectOptions{ContentType: "application/json"})
	if err != nil {
		return "", err
	}
	return "s3://" + s.bucket + "/" + key, nil
}

// Get fetches a payload by reference.
func (s *Store) Get(ctx context.Context, ref string) ([]byte, error) {
	prefix := "s3://" + s.bucket + "/"
	if len(ref) <= len(prefix) || ref[:len(prefix)] != prefix {
		return nil, fmt.Errorf("unknown payload ref %q", ref)
	}
	obj, err := s.cl.GetObject(ctx, s.bucket, ref[len(prefix):], minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	defer obj.Close()
	return io.ReadAll(obj)
}

// Ping checks connectivity.
func (s *Store) Ping(ctx context.Context) error {
	_, err := s.cl.BucketExists(ctx, s.bucket)
	return err
}
