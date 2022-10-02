package hafs

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"
	"sync"

	"github.com/minio/minio-go/v7"
	miniocredentials "github.com/minio/minio-go/v7/pkg/credentials"
)

// This cache only grows which is a good use for sync.Map - it doesn't seem like it will
// ever be a practical problem that the cache never shrinks.
var s3StorageCache sync.Map

type cachedS3Storage struct {
	url    *url.URL
	client *minio.Client
}

func getS3Client(storage string) (*cachedS3Storage, error) {
	cached, ok := s3StorageCache.Load(storage)
	if !ok {
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

		endpoint, ok := q["endpoint"]
		if !ok {
			return nil, fmt.Errorf("s3 storage url %q must contain an endpoint parameter", u.Redacted())
		}

		isSecure := true
		secureParam, ok := q["secure"]
		if ok {
			isSecure = secureParam[0] != "false"
		}

		client, err := minio.New(endpoint[0], &minio.Options{
			Creds:  creds,
			Secure: isSecure,
		})

		if err != nil {
			return nil, err
		}

		newCached := &cachedS3Storage{
			url:    u,
			client: client,
		}
		s3StorageCache.Store(storage, newCached)
		cached = newCached
	}
	return cached.(*cachedS3Storage), nil
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

type readerAtCloser interface {
	io.ReaderAt
	io.Closer
}

func storageOpen(storage string, inode uint64) (readerAtCloser, error) {

	if strings.HasPrefix(storage, "s3://") {
		storage, err := getS3Client(storage)
		if err != nil {
			return nil, err
		}
		obj, err := storage.client.GetObject(
			context.Background(),
			storage.url.Hostname(),
			fmt.Sprintf("%s/%d", storage.url.Path, inode),
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

	if strings.HasPrefix(storage, "file://") {
		f, err := os.Open(fmt.Sprintf("%s/%d", storage[7:], inode))
		return f, err
	}

	return nil, fmt.Errorf("unknown storage scheme: %s", storage)
}

func storageWrite(storage string, inode uint64, data *os.File) (int64, error) {
	if strings.HasPrefix(storage, "s3://") {
		storage, err := getS3Client(storage)
		if err != nil {
			return 0, err
		}
		stat, err := data.Stat()
		if err != nil {
			return 0, err
		}
		obj, err := storage.client.PutObject(
			context.Background(),
			storage.url.Hostname(),
			fmt.Sprintf("%s/%d", storage.url.Path, inode),
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

	if strings.HasPrefix(storage, "file://") {
		f, err := os.Create(fmt.Sprintf("%s/%d", storage[7:], inode))
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

	return 0, fmt.Errorf("unknown storage scheme: %s", storage)
}
