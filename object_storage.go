package hafs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"
	"sync"

	"github.com/minio/minio-go/v7"
	miniocredentials "github.com/minio/minio-go/v7/pkg/credentials"
)

type ReaderAtCloser interface {
	io.ReaderAt
	io.Closer
}

type ObjectStorageEngine interface {
	Open(fs string, inode uint64) (ReaderAtCloser, bool, error)
	ReadAll(fs string, inode uint64, w io.Writer) (bool, error)
	Write(fs string, inode uint64, data *os.File) (int64, error)
	Remove(fs string, inode uint64) error
	Close() error
}

var ErrStorageEngineNotConfigured error = errors.New("storage engine not configured")

type unconfiguredStorageEngine struct{}

func (s *unconfiguredStorageEngine) Open(fs string, inode uint64) (ReaderAtCloser, bool, error) {
	return nil, false, ErrStorageEngineNotConfigured
}

func (s *unconfiguredStorageEngine) ReadAll(fs string, inode uint64, w io.Writer) (bool, error) {
	return false, ErrStorageEngineNotConfigured
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

func (s *fileStorageEngine) Open(fs string, inode uint64) (ReaderAtCloser, bool, error) {
	f, err := os.Open(fmt.Sprintf("%s/%016x.%s", s.path, inode, fs))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return f, true, nil
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

func (s *fileStorageEngine) ReadAll(fs string, inode uint64, w io.Writer) (bool, error) {
	f, err := os.Open(fmt.Sprintf("%s/%016x.%s", s.path, inode, fs))
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	defer f.Close()
	_, err = io.Copy(w, f)
	if err != nil {
		return false, err
	}
	return true, nil
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

// Wrapper around a minio object that implements ReaderAt in a nicer way than
// the minio client does when it comes to sequential reading.
type s3Reader struct {
	lock          sync.Mutex
	obj           *minio.Object
	currentOffset int64
}

func (r *s3Reader) ReadAt(buf []byte, offset int64) (int, error) {
	r.lock.Lock()
	defer r.lock.Unlock()
	if offset != r.currentOffset {
		newOffset, err := r.obj.Seek(offset, io.SeekStart)
		if err != nil {
			r.currentOffset = ^0
			return 0, err
		}
		r.currentOffset = newOffset
	}
	n, err := io.ReadFull(r.obj, buf)
	r.currentOffset += int64(n)
	if err == io.ErrUnexpectedEOF {
		err = io.EOF
	}
	return n, err
}

func (r *s3Reader) Close() error {
	return r.obj.Close()
}

func (s *s3StorageEngine) Open(fs string, inode uint64) (ReaderAtCloser, bool, error) {
	obj, err := s.client.GetObject(
		context.Background(),
		s.bucket,
		fmt.Sprintf("%s%016x.%s", s.path, inode, fs),
		minio.GetObjectOptions{},
	)
	if err != nil {
		if minio.ToErrorResponse(err).StatusCode == 404 {
			return nil, false, nil
		}
		return nil, false, err
	}
	return &s3Reader{
		obj:           obj,
		currentOffset: 0,
	}, true, nil
}

func (s *s3StorageEngine) ReadAll(fs string, inode uint64, w io.Writer) (bool, error) {
	obj, err := s.client.GetObject(
		context.Background(),
		s.bucket,
		fmt.Sprintf("%s%016x.%s", s.path, inode, fs),
		minio.GetObjectOptions{},
	)
	defer obj.Close()
	if err != nil {
		if minio.ToErrorResponse(err).StatusCode == 404 {
			return false, nil
		}
		return false, err
	}
	_, err = io.Copy(w, obj)
	if err != nil {
		return false, err
	}
	return true, nil
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
