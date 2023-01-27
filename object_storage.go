package hafs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"

	"github.com/minio/minio-go/v7"
	miniocredentials "github.com/minio/minio-go/v7/pkg/credentials"
)

type ObjectStorageEngine interface {
	Read(fs string, inode uint64, offset uint64, buf []byte) (uint64, error)
	Write(fs string, inode uint64, data *os.File) (int64, error)
	Remove(fs string, inode uint64) error
	Close() error
}

var ErrStorageEngineNotConfigured error = errors.New("storage engine not configured")

type unconfiguredStorageEngine struct{}

func (s *unconfiguredStorageEngine) Read(fs string, inode uint64, offset uint64, buf []byte) (uint64, error) {
	return 0, ErrStorageEngineNotConfigured
}

func (s *unconfiguredStorageEngine) Write(fs string, inode uint64, data *os.File) (int64, error) {
	return 0, ErrStorageEngineNotConfigured
}

func (s *unconfiguredStorageEngine) Remove(fs string, inode uint64) error {
	return ErrStorageEngineNotConfigured
}

func (s *unconfiguredStorageEngine) Close() error {
	return ErrStorageEngineNotConfigured
}

type fileStorageEngine struct {
	path string
}

func (s *fileStorageEngine) Write(fs string, inode uint64, data *os.File) (int64, error) {
	f, err := os.Create(fmt.Sprintf("%s/%016x.%s", s.path, inode, fs))
	if err != nil {
		return 0, err
	}
	defer f.Close()
	n, err := io.Copy(f, data)
	if err != nil {
		return n, err
	}
	return n, f.Sync()
}

func (s *fileStorageEngine) Read(fs string, inode uint64, offset uint64, buf []byte) (uint64, error) {
	f, err := os.Open(fmt.Sprintf("%s/%016x.%s", s.path, inode, fs))
	if err != nil {
		if os.IsNotExist(err) {
			for i := 0; i < len(buf); i += 1 {
				buf[i] = 0
			}
			return uint64(len(buf)), nil
		}
		return 0, err
	}
	defer f.Close()
	n, err := f.ReadAt(buf, int64(offset))
	return uint64(n), err
}

func (s *fileStorageEngine) Remove(fs string, inode uint64) error {
	err := os.Remove(fmt.Sprintf("%s/%016x.%s", s.path, inode, fs))
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	return nil
}

func (s *fileStorageEngine) Close() error {
	return nil
}

type s3StorageEngine struct {
	path   string
	bucket string
	client *minio.Client
}

func (s *s3StorageEngine) Read(fs string, inode uint64, offset uint64, buf []byte) (uint64, error) {
	obj, err := s.client.GetObject(
		context.Background(),
		s.bucket,
		fmt.Sprintf("%s%016x.%s", s.path, inode, fs),
		minio.GetObjectOptions{},
	)
	defer obj.Close()
	if err != nil {
		if minio.ToErrorResponse(err).StatusCode == 404 {
			for i := 0; i < len(buf); i += 1 {
				buf[i] = 0
			}
			return uint64(len(buf)), nil
		}
		return 0, err
	}
	n, err := obj.ReadAt(buf, int64(offset))
	return uint64(n), err
}

func (s *s3StorageEngine) Write(fs string, inode uint64, data *os.File) (int64, error) {
	stat, err := data.Stat()
	if err != nil {
		return 0, err
	}
	obj, err := s.client.PutObject(
		context.Background(),
		s.bucket,
		fmt.Sprintf("%s%016x.%s", s.path, inode, fs),
		data,
		stat.Size(),
		minio.PutObjectOptions{},
	)
	if err != nil {
		if minio.ToErrorResponse(err).StatusCode == 403 {
			return 0, ErrPermission
		}
		return 0, err
	}
	return obj.Size, nil
}

func (s *s3StorageEngine) Remove(fs string, inode uint64) error {
	err := s.client.RemoveObject(
		context.Background(),
		s.bucket,
		fmt.Sprintf("%s%016x.%s", s.path, inode, fs),
		minio.RemoveObjectOptions{},
	)
	if err != nil {
		if minio.ToErrorResponse(err).StatusCode == 404 {
			return nil
		}
		return err
	}
	return nil
}

func (s *s3StorageEngine) Close() error {
	return nil
}

func NewObjectStorageEngine(storageSpec string) (ObjectStorageEngine, error) {

	if strings.HasPrefix(storageSpec, "file:") {
		return &fileStorageEngine{
			path: storageSpec[5:],
		}, nil
	}

	if strings.HasPrefix(storageSpec, "s3:") {
		var creds *miniocredentials.Credentials

		u, err := url.Parse(storageSpec)
		if err != nil {
			return nil, err
		}

		q := u.Query()

		if u.User != nil {
			accessKeyID := u.User.Username()
			secretAccessKey, _ := u.User.Password()
			creds = miniocredentials.NewStaticV4(accessKeyID, secretAccessKey, "")
		} else {
			creds = miniocredentials.NewEnvAWS()
		}

		bucket, ok := q["bucket"]
		if !ok {
			return nil, fmt.Errorf("s3 storage url %q must contain bucket parameter", u.Redacted())
		}

		isSecure := true
		if secureParam, ok := q["secure"]; ok {
			isSecure = secureParam[0] != "false"
		}

		endpoint := u.Hostname()
		if u.Port() != "" {
			endpoint = endpoint + ":" + u.Port()
		}

		client, err := minio.New(endpoint, &minio.Options{
			Creds:  creds,
			Secure: isSecure,
		})
		if err != nil {
			return nil, err
		}

		return &s3StorageEngine{
			bucket: bucket[0],
			path:   u.Path,
			client: client,
		}, nil
	}

	if storageSpec == "" {
		return &unconfiguredStorageEngine{}, nil
	}

	return nil, errors.New("unknown/invalid storage specification")
}
