package hafs

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"sync"

	"github.com/minio/minio-go/v7"
	miniocredentials "github.com/minio/minio-go/v7/pkg/credentials"
)

type StorageObject interface {
	io.ReaderAt
	io.Closer
}

func newS3Client(u *url.URL) (*minio.Client, error) {

	var creds *miniocredentials.Credentials

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

	return client, err
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

func StorageOpen(storage string, inode uint64) (StorageObject, error) {
	u, err := url.Parse(storage)
	if err != nil {
		return nil, err
	}
	switch u.Scheme {
	case "file":
		f, err := os.Open(fmt.Sprintf("%s/%d", u.Path, inode))
		return f, err
	case "s3":
		client, err := newS3Client(u)
		if err != nil {
			return nil, err
		}
		obj, err := client.GetObject(context.Background(), u.Hostname(), fmt.Sprintf("%s/%d", u.Path, inode), minio.GetObjectOptions{})
		if err != nil {
			return nil, err
		}
		return &s3Reader{
			obj:           obj,
			currentOffset: 0,
		}, nil
	default:
		return nil, fmt.Errorf("unknown storage scheme: %s", u.Scheme)
	}
}

func StorageWrite(storage string, inode uint64, data *os.File) (int64, error) {
	u, err := url.Parse(storage)
	if err != nil {
		return 0, err
	}
	switch u.Scheme {
	case "file":
		f, err := os.Create(fmt.Sprintf("%s/%d", u.Path, inode))
		if err != nil {
			return 0, err
		}
		defer f.Close()
		n, err := io.Copy(f, data)
		if err != nil {
			return n, err
		}
		return n, f.Sync()
	case "s3":
		client, err := newS3Client(u)
		if err != nil {
			return 0, err
		}
		stat, err := data.Stat()
		if err != nil {
			return 0, err
		}
		obj, err := client.PutObject(
			context.Background(),
			u.Hostname(),
			fmt.Sprintf("%s/%d", u.Path, inode),
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
	default:
		return 0, fmt.Errorf("unknown storage scheme: %s", u.Scheme)
	}
}
