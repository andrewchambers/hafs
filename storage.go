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

	crushstore "github.com/andrewchambers/crushstore/client"
	"github.com/minio/minio-go/v7"
	miniocredentials "github.com/minio/minio-go/v7/pkg/credentials"
)

type ReaderAtCloser interface {
	io.ReaderAt
	io.Closer
}

type StorageEngine interface {
	Open(fs string, inode uint64) (ReaderAtCloser, error)
	Write(fs string, inode uint64, data *os.File) (int64, error)
	Remove(fs string, inode uint64) error
	Validate() error
	Close() error
}

type fileStorageEngine struct {
	path string
}

func (s *fileStorageEngine) Open(fs string, inode uint64) (ReaderAtCloser, error) {
	f, err := os.Open(fmt.Sprintf("%s/%d.%s", s.path, inode, fs))
	return f, err
}

func (s *fileStorageEngine) Write(fs string, inode uint64, data *os.File) (int64, error) {
	f, err := os.Create(fmt.Sprintf("%s/%d.%s", s.path, inode, fs))
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

func (s *fileStorageEngine) Remove(fs string, inode uint64) error {
	err := os.Remove(fmt.Sprintf("%s/%d.%s", s.path, inode, fs))
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

func (s *s3StorageEngine) Open(fs string, inode uint64) (ReaderAtCloser, error) {
	obj, err := s.client.GetObject(
		context.Background(),
		s.bucket,
		fmt.Sprintf("%s/%d.%s", s.path, inode, fs),
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

func (s *s3StorageEngine) Write(fs string, inode uint64, data *os.File) (int64, error) {
	stat, err := data.Stat()
	if err != nil {
		return 0, err
	}
	obj, err := s.client.PutObject(
		context.Background(),
		s.bucket,
		fmt.Sprintf("%s/%d.%s", s.path, inode, fs),
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
		fmt.Sprintf("%s/%d.%s", s.path, inode, fs),
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

type crushStoreStorageEngine struct {
	client *crushstore.Client
}

type crushStoreObjectReader struct {
	lock          sync.Mutex
	obj           *crushstore.ObjectReader
	currentOffset int64
}

func (r *crushStoreObjectReader) ReadAt(buf []byte, offset int64) (int, error) {
	r.lock.Lock()
	defer r.lock.Unlock()
	if offset != r.currentOffset {
		return 0, ErrNotSupported
	}
	n, err := io.ReadFull(r.obj, buf)
	r.currentOffset += int64(n)
	if err == io.ErrUnexpectedEOF {
		err = io.EOF
	}
	return n, err
}

func (r *crushStoreObjectReader) Close() error {
	return r.obj.Close()
}

func (s *crushStoreStorageEngine) Open(fs string, inode uint64) (ReaderAtCloser, error) {
	obj, ok, err := s.client.Get(
		fmt.Sprintf("%d.%s", inode, fs),
		crushstore.GetOptions{},
	)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, ErrNotExist
	}
	return &crushStoreObjectReader{
		obj:           obj,
		currentOffset: 0,
	}, nil
}

func (s *crushStoreStorageEngine) Write(fs string, inode uint64, data *os.File) (int64, error) {
	stat, err := data.Stat()
	if err != nil {
		return 0, err
	}
	err = s.client.Put(
		fmt.Sprintf("%d.%s", inode, fs),
		data,
		crushstore.PutOptions{},
	)
	return stat.Size(), err
}

func (s *crushStoreStorageEngine) Remove(fs string, inode uint64) error {
	err := s.client.Delete(
		fmt.Sprintf("%d.%s", inode, fs),
		crushstore.DeleteOptions{},
	)
	if err != nil {
		return err
	}
	return nil
}

func (s *crushStoreStorageEngine) Validate() error {
	_, _, err := s.client.Head("test-key", crushstore.HeadOptions{})
	if err != nil {
		return fmt.Errorf("unable to check test key: %w", err)
	}
	return nil
}

func (s *crushStoreStorageEngine) Close() error {
	return s.client.Close()
}

func newStorageEngine(storage string) (StorageEngine, error) {
	if strings.HasPrefix(storage, "crushstore:") {
		u, err := url.Parse(storage)
		if err != nil {
			return nil, err
		}
		cfg := u.Query().Get("cluster_config")
		if cfg == "" {
			cfg = os.Getenv("CRUSHSTORE_CLUSTER_CONFIG")
			if cfg == "" {
				_, err := os.Stat("./crushstore-cluster.conf")
				if err == nil {
					cfg = "./crushstore-cluster.conf"
				} else {
					cfg = "/etc/crushstore/crushstore-cluster.conf"
				}
			}
			client, err := crushstore.New(cfg, crushstore.ClientOptions{})
			if err != nil {
				return nil, err
			}
			return &crushStoreStorageEngine{
				client: client,
			}, nil
		}
	}

	if strings.HasPrefix(storage, "file:") {
		return &fileStorageEngine{
			path: storage[5:],
		}, nil
	}

	if strings.HasPrefix(storage, "s3:") {
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

var DefaultStorageEngineCache StorageEngineCache

type StorageEngineCache struct {
	// This cache only ever grows - which is a good use for sync.Map.
	// it doesn't seem like the infinite growth ever be a practical problem,
	// but we can address that if it ever does.
	cache sync.Map
}

func (c *StorageEngineCache) Get(storage string) (StorageEngine, error) {
	cached, ok := c.cache.Load(storage)
	if !ok {
		engine, err := newStorageEngine(storage)
		if err != nil {
			return nil, err
		}
		c.cache.Store(storage, engine)
		return engine, nil
	}
	return cached.(StorageEngine), nil
}

func (c *StorageEngineCache) Validate(storage string) error {
	s, err := newStorageEngine(storage)
	if err != nil {
		return err
	}
	defer s.Close()
	return s.Validate()
}
