package hafs

import (
	"context"
	"errors"
	"fmt"
	"io"
	iofs "io/fs"
	"net/url"
	"os"
	"strings"
	"sync"

	"github.com/minio/minio-go/v7"
	miniocredentials "github.com/minio/minio-go/v7/pkg/credentials"
)

type readerAtCloser interface {
	io.ReaderAt
	io.Closer
}

type storageEngine interface {
	Open(inode uint64) (readerAtCloser, error)
	Write(inode uint64, data *os.File) (int64, error)
	Remove(inode uint64) error
	Validate() error
	Close() error
}

type fileStorageEngine struct {
	path string
}

func (s *fileStorageEngine) Open(inode uint64) (readerAtCloser, error) {
	f, err := os.Open(fmt.Sprintf("%s/%d", s.path, inode))
	return f, err
}

func (s *fileStorageEngine) Write(inode uint64, data *os.File) (int64, error) {
	f, err := os.Create(fmt.Sprintf("%s/%d", s.path, inode))
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

func (s *fileStorageEngine) Remove(inode uint64) error {
	err := os.Remove(fmt.Sprintf("%s/%d", s.path, inode))
	if err != nil {
		if err == iofs.ErrNotExist {
			return nil
		}
		return err
	}
	return nil
}

func (s *fileStorageEngine) Validate() error {
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
			r.currentOffset = newOffset
		}
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

func (s *s3StorageEngine) Open(inode uint64) (readerAtCloser, error) {
	obj, err := s.client.GetObject(
		context.Background(),
		s.bucket,
		fmt.Sprintf("%s/%d", s.path, inode),
		minio.GetObjectOptions{},
	)
	if err != nil {
		return nil, err
	}
	return &s3Reader{
		obj:           obj,
		currentOffset: 0,
	}, nil
}

func (s *s3StorageEngine) Write(inode uint64, data *os.File) (int64, error) {
	stat, err := data.Stat()
	if err != nil {
		return 0, err
	}
	obj, err := s.client.PutObject(
		context.Background(),
		s.bucket,
		fmt.Sprintf("%s/%d", s.path, inode),
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

func (s *s3StorageEngine) Remove(inode uint64) error {
	err := s.client.RemoveObject(
		context.Background(),
		s.bucket,
		fmt.Sprintf("%s/%d", s.path, inode),
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

func (s *s3StorageEngine) Validate() error {
	return nil
}

func (s *s3StorageEngine) Close() error {
	return nil
}

func newStorageEngine(storage string) (storageEngine, error) {
	if strings.HasPrefix(storage, "file://") {
		return &fileStorageEngine{
			path: storage[7:],
		}, nil
	}

	if strings.HasPrefix(storage, "s3://") {
		var creds *miniocredentials.Credentials

		u, err := url.Parse(storage)
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

	return nil, errors.New("unknown/invalid storage specification")
}

// This cache only ever grows - which is a good use for sync.Map.
// it doesn't seem like the infinite growth ever be a practical problem,
// but we can address that if it ever does.
var storageEngineCache sync.Map

func getStorageEngine(storage string) (storageEngine, error) {
	cached, ok := storageEngineCache.Load(storage)
	if !ok {
		engine, err := newStorageEngine(storage)
		if err != nil {
			return nil, err
		}
		storageEngineCache.Store(storage, engine)
		return engine, nil
	}
	return cached.(storageEngine), nil
}

func storageValidate(storage string) error {
	s, err := newStorageEngine(storage)
	if err != nil {
		return err
	}
	defer s.Close()
	return s.Validate()
}

func storageOpen(storage string, inode uint64) (readerAtCloser, error) {
	s, err := getStorageEngine(storage)
	if err != nil {
		return nil, err
	}
	return s.Open(inode)
}

func storageWrite(storage string, inode uint64, data *os.File) (int64, error) {
	s, err := getStorageEngine(storage)
	if err != nil {
		return 0, err
	}
	return s.Write(inode, data)
}

func storageRemove(storage string, inode uint64) error {
	s, err := getStorageEngine(storage)
	if err != nil {
		return err
	}
	return s.Remove(inode)
}
