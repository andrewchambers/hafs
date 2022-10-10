package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/shlex"
	"golang.org/x/sync/errgroup"
	"gopkg.in/yaml.v3"
	"lukechampine.com/blake3"
)

const (
	TOMBSTONE_EXPIRY = 120 * time.Second // TODO a real/configurable value.
)

var (
	DataDir      string
	ThisLocation Location
)

func replicateObj(server string, k string, f *os.File) error {
	r, w := io.Pipe()
	mpw := multipart.NewWriter(w)
	errg, _ := errgroup.WithContext(context.Background())
	errg.Go(func() error {
		var part io.Writer
		defer w.Close()
		defer f.Close()
		part, err := mpw.CreateFormFile("data", "data")
		if err != nil {
			return err
		}
		_, err = io.Copy(part, f)
		if err != nil {
			return err
		}
		err = mpw.Close()
		if err != nil {
			return err
		}
		return nil
	})

	endpoint := fmt.Sprintf("%s/put?key=%s&type=replicate", server, k)
	resp, err := http.Post(endpoint, mpw.FormDataContentType(), r)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("unable to read response: %s", endpoint, err)
	}

	if resp.StatusCode != 200 {
		return fmt.Errorf("post object %q to %s failed: %s, body=%q", k, endpoint, resp.Status, body)
	}

	uploadErr := errg.Wait()
	if err != nil {
		return uploadErr
	}

	return nil
}

func checkObj(server string, k string) (ObjMeta, bool, error) {
	endpoint := fmt.Sprintf("%s/check?key=%s", server, k)
	resp, err := http.Get(endpoint)
	if err != nil {
		return ObjMeta{}, false, fmt.Errorf("unable to check %q@%s", k, server)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return ObjMeta{}, false, fmt.Errorf("unable to read check body for %q@%s", k, server)
	}

	if resp.StatusCode == 404 {
		return ObjMeta{}, false, nil
	}

	if resp.StatusCode != 200 {
		return ObjMeta{}, false, fmt.Errorf("unable to check %q@%s: %s", k, server, err)
	}

	stat := ObjMeta{}
	err = json.Unmarshal(body, &stat)
	return stat, true, err
}

type ObjStamp struct {
	Tombstone          bool
	CreatedAtUnixMicro uint64
}

func ObjStampFromBytes(b []byte) ObjStamp {
	stamp := ObjStamp{}
	stamp.FieldsFromBytes(b[:])
	return stamp
}

func (s *ObjStamp) IsExpired(now time.Time, timeout time.Duration) bool {
	return s.Tombstone && time.UnixMicro(int64(s.CreatedAtUnixMicro)).Add(timeout).Before(now)
}

func (s *ObjStamp) FieldsFromBytes(b []byte) {
	stamp := binary.BigEndian.Uint64(b)
	s.Tombstone = (stamp >> 63) != 0
	s.CreatedAtUnixMicro = (stamp << 1) >> 1
}

func (s *ObjStamp) ToBytes() [8]byte {
	stamp := s.CreatedAtUnixMicro
	if s.Tombstone {
		stamp |= 1 << 63
	}
	b := [8]byte{}
	binary.BigEndian.PutUint64(b[:], stamp)
	return b
}

type ObjMeta struct {
	Size               uint64
	Tombstone          bool
	CreatedAtUnixMicro uint64
}

func internalError(w http.ResponseWriter, format string, a ...interface{}) {
	log.Printf(format, a...)
	w.WriteHeader(http.StatusInternalServerError)
	w.Write([]byte("internal server error"))
}

func checkHandler(w http.ResponseWriter, req *http.Request) {
	if req.Method != "GET" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	q := req.URL.Query()
	k := url.QueryEscape(q.Get("key"))
	if k == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	objPath := filepath.Join(DataDir, k)
	f, err := os.Open(objPath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		internalError(w, "io error opening %q: %s", objPath, err)
		return
	}
	defer f.Close()
	stat, err := f.Stat()
	if err != nil {
		internalError(w, "io error stating %q: %s", objPath, err)
		return
	}

	stampBytes := [8]byte{}
	_, err = f.ReadAt(stampBytes[:], 32)
	if err != nil {
		internalError(w, "io error reading %q: %s", objPath, err)
		return
	}
	stamp := ObjStampFromBytes(stampBytes[:])
	buf, err := json.Marshal(ObjMeta{
		Size:               uint64(stat.Size()) - 40,
		Tombstone:          stamp.Tombstone,
		CreatedAtUnixMicro: stamp.CreatedAtUnixMicro,
	})
	if err != nil {
		internalError(w, "error marshalling response: %s", err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(buf)
}

type objectContentReadSeeker struct {
	f *os.File
}

func (of *objectContentReadSeeker) Read(buf []byte) (int, error) {
	return of.f.Read(buf)
}

func (of *objectContentReadSeeker) Seek(offset int64, whence int) (int64, error) {
	return of.f.Seek(offset+40, whence)
}

func flushDir(dirPath string) error {
	// XXX possible cache opens?
	d, err := os.Open(dirPath)
	if err != nil {
		return err
	}
	defer d.Close()
	// XXX possible to batch syncs across goroutines?
	return d.Sync()
}

func getHandler(w http.ResponseWriter, req *http.Request) {
	if req.Method != "GET" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	q := req.URL.Query()
	k := url.QueryEscape(q.Get("key"))
	if k == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	objPath := filepath.Join(DataDir, k)
	f, err := os.Open(filepath.Join(DataDir, k))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			locs, err := GetClusterConfig().Crush(k)
			if err != nil {
				internalError(w, "error placing %q: %s", k, err)
				return
			}
			primaryLoc := locs[0]
			if ThisLocation.Equals(primaryLoc) {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			endpoint := fmt.Sprintf("%s/get?key=%s", primaryLoc[len(primaryLoc)-1], k)
			log.Printf("redirecting get %q to %s", k, endpoint)
			http.Redirect(w, req, endpoint, http.StatusTemporaryRedirect)
			return
		}
		internalError(w, "io error opening %q: %s", objPath, err)
		return
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		internalError(w, "io error statting %q: %s", objPath, err)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", fmt.Sprintf("%d", stat.Size()-40))
	http.ServeContent(w, req, k, stat.ModTime(), &objectContentReadSeeker{f})
}

func putHandler(w http.ResponseWriter, req *http.Request) {
	if req.Method != "POST" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	q := req.URL.Query()
	k := url.QueryEscape(q.Get("key"))
	if k == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	objDir := DataDir
	objPath := filepath.Join(objDir, k)

	locs, err := GetClusterConfig().Crush(k)
	if err != nil {
		internalError(w, "error placing %q: %s", k, err)
		return
	}
	primaryLoc := locs[0]
	isPrimary := primaryLoc.Equals(ThisLocation)
	isReplication := q.Get("type") == "replicate"

	// Only the primary supports non replication writes.
	if !isReplication && !isPrimary {
		endpoint := fmt.Sprintf("%s/put?key=%s", primaryLoc[len(primaryLoc)-1], k)
		log.Printf("redirecting put %q to %s", k, endpoint)
		http.Redirect(w, req, endpoint, http.StatusTemporaryRedirect)
		return
	}

	err = req.ParseMultipartForm(16 * 1024 * 1024)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	dataFile, _, err := req.FormFile("data")
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("missing data field"))
		return
	}
	defer dataFile.Close()

	// Write object.
	tmpF, err := os.CreateTemp(DataDir, "obj.*.tmp")
	if err != nil {
		internalError(w, "io error creating temporary file: %s", err)
		return
	}

	removeTmp := true
	defer func() {
		if removeTmp {
			err := os.Remove(tmpF.Name())
			if err != nil {
				log.Printf("io error removing %q", tmpF.Name())
			}
		}
	}()

	hash := [32]byte{}
	stamp := ObjStamp{}
	hasher := blake3.New(32, nil)

	if isReplication {
		header := [40]byte{}
		_, err := io.ReadFull(dataFile, header[:])
		if err != nil {
			if errors.Is(err, io.EOF) {
				w.WriteHeader(400)
			} else {
				log.Printf("unable to read put object: %s", err)
				w.WriteHeader(500)
			}
			return
		}
		copy(hash[:], header[:32])
		stamp.FieldsFromBytes(header[32:40])
		_, err = hasher.Write(header[32:40])
		if err != nil {
			internalError(w, "error hashing: %s", err)
			return
		}
		_, err = tmpF.Write(header[:])
		if err != nil {
			internalError(w, "io error writing %q: %s", tmpF.Name(), err)
			return
		}

		if stamp.IsExpired(time.Now(), TOMBSTONE_EXPIRY) {
			return
		}
	} else {
		header := [40]byte{}
		stamp.Tombstone = false
		stamp.CreatedAtUnixMicro = uint64(time.Now().UnixMicro())
		stampBytes := stamp.ToBytes()
		_, err = hasher.Write(stampBytes[:])
		if err != nil {
			internalError(w, "error hashing: %s", err)
			return
		}
		copy(header[32:], stampBytes[:])
		_, err := tmpF.Write(header[:])
		if err != nil {
			internalError(w, "io error writing %q: %s", tmpF.Name(), err)
			return
		}
	}

	_, err = io.Copy(io.MultiWriter(tmpF, hasher), dataFile)
	if err != nil {
		internalError(w, "io error writing %q: %s", tmpF.Name(), err)
		return
	}

	if isReplication {
		actualHash := [32]byte{}
		copy(actualHash[:], hasher.Sum(nil))
		if hash != actualHash {
			w.WriteHeader(http.StatusBadRequest)
			io.WriteString(w,
				fmt.Sprintf(
					"sent hash %s did not equal computed hash %s",
					hex.EncodeToString(hash[:]),
					hex.EncodeToString(actualHash[:]),
				),
			)
			return

		}
	} else {
		copy(hash[:], hasher.Sum(nil))
		_, err = tmpF.WriteAt(hash[:], 0)
		if err != nil {
			internalError(w, "io error writing %q: %s", tmpF.Name(), err)
			return
		}
	}

	existingF, err := os.Open(objPath)
	if err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			internalError(w, "io error opening %q: %s", objPath, err)
			return
		}
	}
	if existingF != nil {
		existingHeader := [40]byte{}
		_, err := existingF.ReadAt(existingHeader[:], 0)
		if err != nil {
			internalError(w, "io error reading %q: %s", objPath, err)
			return
		}

		existingStamp := ObjStampFromBytes(existingHeader[32:])
		if existingStamp.Tombstone && stamp.Tombstone {
			// Nothing to do, already deleted.
			w.WriteHeader(200)
			return
		}

		// Only accept the put if it is a delete or reupload of the existing object.
		if !stamp.Tombstone {
			if !bytes.Equal(hash[:], existingHeader[:32]) {
				w.WriteHeader(400)
				w.Write([]byte("conflicting put"))
				return
			}
		}
	}

	err = tmpF.Sync()
	if err != nil {
		internalError(w, "io error syncing %q: %s", tmpF.Name(), err)
		return
	}

	err = tmpF.Close()
	if err != nil {
		internalError(w, "io error closing %q: %s", tmpF.Name(), err)
		return
	}

	err = os.Rename(tmpF.Name(), objPath)
	if err != nil {
		internalError(w, "io overwriting %q: %s", objPath, err)
		return
	}
	removeTmp = false

	if !isPrimary {
		err := flushDir(objDir)
		if err != nil {
			internalError(w, "io error flushing %q: %s", objDir, err)
			return
		}
		return
	}

	// We are the primary, we must spread the
	// data to all the other nodes in the placement.
	wg := &sync.WaitGroup{}
	successfulReplications := new(uint64)
	for i := 1; i < len(locs); i++ {
		loc := locs[i]
		wg.Add(1)
		go func() {
			wg.Done()
			server := loc[len(loc)-1]
			if isReplication {
				meta, ok, err := checkObj(server, k)
				if err != nil {
					log.Printf("error checking %q@%s: %s", k, server, err)
					return
				}
				if ok && (stamp.Tombstone == meta.Tombstone || meta.Tombstone) {
					// We don't need to replicate if the remote has matching objects and
					// the tombstones match or the remote node has already deleted this key.
					atomic.AddUint64(successfulReplications, 1)
					return
				}
			}

			objF, err := os.Open(objPath)
			if err != nil {
				log.Printf("io error opening %q: %s", objPath, err)
				return
			}
			defer objF.Close()
			log.Printf("replicating %q to %s", k, server)
			err = replicateObj(server, k, objF)
			if err != nil {
				log.Printf("error replicating %q: %s", objPath, err)
				return
			}

			atomic.AddUint64(successfulReplications, 1)
		}()
	}

	err = flushDir(objDir)
	if err == nil {
		atomic.AddUint64(successfulReplications, 1)
	} else {
		log.Printf("io error flushing %q: %s", objDir, err)
		return
	}

	wg.Wait()

	minReplicas := uint64(len(locs))
	if *successfulReplications < minReplicas { // XXX we could add a 'min replication param'
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}

func deleteHandler(w http.ResponseWriter, req *http.Request) {
	if req.Method != "POST" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	q := req.URL.Query()
	k := url.QueryEscape(q.Get("key"))
	if k == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	objDir := DataDir
	objPath := filepath.Join(objDir, k)

	locs, err := GetClusterConfig().Crush(k)
	if err != nil {
		internalError(w, "error placing %q: %s", k, err)
		return
	}
	primaryLoc := locs[0]

	if !primaryLoc.Equals(ThisLocation) {
		endpoint := fmt.Sprintf("%s/delete?key=%s", primaryLoc[len(primaryLoc)-1], k)
		log.Printf("redirecting delete %q to %s", k, endpoint)
		http.Redirect(w, req, endpoint, http.StatusTemporaryRedirect)
		return
	}

	objStamp := ObjStamp{
		Tombstone:          true,
		CreatedAtUnixMicro: uint64(time.Now().UnixMicro()),
	}
	objStampBytes := objStamp.ToBytes()
	objHash := blake3.Sum256(objStampBytes[:])
	obj := [40]byte{}
	copy(obj[0:32], objHash[:])
	copy(obj[32:40], objStampBytes[:])

	// Write object.
	tmpF, err := os.CreateTemp(DataDir, "obj*$tmp") // Use a suffix that our key escape handles.
	if err != nil {
		internalError(w, "io error creating temporary file: %s", err)
		return
	}
	defer tmpF.Close()
	removeTmp := true
	defer func() {
		if removeTmp {
			err := os.Remove(tmpF.Name())
			if err != nil {
				log.Printf("io removing %q: %s", tmpF.Name(), err)
			}
		}
	}()

	_, err = tmpF.Write(obj[:])
	if err != nil {
		internalError(w, "io error writing %q: %s", tmpF.Name(), err)
		return
	}

	err = tmpF.Sync()
	if err != nil {
		internalError(w, "io error syncing %q: %s", tmpF.Name(), err)
		return
	}

	err = tmpF.Close()
	if err != nil {
		internalError(w, "io error closing %q: %s", tmpF.Name(), err)
		return
	}

	err = os.Rename(tmpF.Name(), objPath)
	if err != nil {
		internalError(w, "io overwriting %q: %s", objPath, err)
		return
	}
	removeTmp = false

	wg := &sync.WaitGroup{}
	successfulReplications := new(uint64)

	for i := 1; i < len(locs); i++ {
		loc := locs[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			server := loc[len(loc)-1]
			objF, err := os.Open(objPath)
			if err != nil {
				log.Printf("io error opening %q: %s", objPath, err)
				return
			}
			defer objF.Close()
			log.Printf("replicating deletion of %q to %s", k, server)
			err = replicateObj(server, k, objF)
			if err != nil {
				log.Printf("error replicating %q: %s", objPath, err)
				return
			}
			atomic.AddUint64(successfulReplications, 1)
		}()
	}

	err = flushDir(objDir)
	if err == nil {
		atomic.AddUint64(successfulReplications, 1)
	} else {
		log.Printf("io error flushing %q: %s", objDir, err)
		return
	}

	wg.Wait()

	minReplicas := uint64(len(locs))
	if *successfulReplications < minReplicas { // XXX we could add a 'min replication param'
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

}

func ScrubObject(objPath string, opts ScrubOpts) {
	log.Printf("scrubbing object stored at %q", objPath)

	k, err := url.QueryUnescape(filepath.Base(objPath))
	if err != nil {
		log.Printf("scrubber removing %q, not a valid object")
		err = os.Remove(objPath)
		if err != nil {
			logScrubError(SCRUB_ECORRUPT, "io error removing %q: %s", objPath, err)
		}
		return
	}

	locs, err := GetClusterConfig().Crush(k)
	if err != nil {
		logScrubError(SCRUB_EOTHER, "scrubber unable to place %q: %s", objPath, err)
		return
	}
	primaryLoc := locs[0]

	objF, err := os.Open(objPath)
	if err != nil {
		logScrubError(SCRUB_EOTHER, "scrubber unable to open %q: %s", objPath, err)
		return
	}
	defer objF.Close()

	stat, err := objF.Stat()
	if err != nil {
		logScrubError(SCRUB_EOTHER, "scrubber unable to stat %q: %s", objPath, err)
	}
	if err == nil {
		atomic.AddUint64(&_totalScrubbedBytes, uint64(stat.Size()))
	}

	stampBytes := [8]byte{}
	_, err = objF.ReadAt(stampBytes[:], 32)
	if err != nil {
		logScrubError(SCRUB_EOTHER, "scrubber unable to read %q: %s", objPath, err)
	}

	stamp := ObjStampFromBytes(stampBytes[:])

	if opts.Full {
		expectedHash := [32]byte{}
		actualHash := [32]byte{}
		_, err := io.ReadFull(objF, expectedHash[:])
		if err != nil && !errors.Is(err, io.EOF) {
			logScrubError(SCRUB_EOTHER, "io error scrubbing %q: %s", objPath, err)
			return
		}
		hasher := blake3.New(32, nil)
		_, err = io.Copy(hasher, objF)
		if err != nil {
			logScrubError(SCRUB_EOTHER, "io error scrubbing %q: %s", objPath, err)
			return
		}
		copy(actualHash[:], hasher.Sum(nil))
		if expectedHash != actualHash {
			log.Printf("scrub detected corrupt file at %q, removing it", objPath)
			err = os.Remove(objPath)
			if err != nil {
				logScrubError(SCRUB_ECORRUPT, "io error removing %q: %s", objPath, err)
			}
			return
		}

		_, err = objF.Seek(0, io.SeekStart)
		if err != nil {
			logScrubError(SCRUB_EOTHER, "io error seeking %q", objPath)
			return
		}

		// We only trust a tombstone after it has been fully scrubbed.
		if stamp.IsExpired(time.Now(), TOMBSTONE_EXPIRY) {
			log.Printf("scrubber removing %q, it has expired", objPath)
			err := os.Remove(objPath)
			if err != nil {
				logScrubError(SCRUB_EOTHER, "unable to remove %q: %s", objPath, err)
			}
			return
		}

	}

	if ThisLocation.Equals(primaryLoc) {
		for i := 1; i < len(locs); i++ {
			server := locs[i][len(locs[i])-1]
			meta, ok, err := checkObj(server, k)
			if err != nil {
				logScrubError(SCRUB_EREPL, "scrubber check failed: %s", err)
				continue
			}
			if ok {
				if stamp.Tombstone && !meta.Tombstone {
					// Both have the data, but they disagree about the deletion state.
					log.Printf("scrubber replicating tombstone of %q to %s", k, server)
					err := replicateObj(server, k, objF)
					if err != nil {
						logScrubError(SCRUB_EREPL, "scrubber replication of %q failed: %s", k, err)
					}
				}
			} else {
				log.Printf("scrubber replicating %q to %s", k, server)
				err := replicateObj(server, k, objF)
				if err != nil {
					logScrubError(SCRUB_EREPL, "scrubber replication of %q failed: %s", k, err)
				}
			}
		}
	} else {
		primaryServer := primaryLoc[len(primaryLoc)-1]
		meta, ok, err := checkObj(primaryServer, k)
		if err != nil {
			logScrubError(SCRUB_EREPL, "scrubber was unable to verify primary placement of %q: %s", k, err)
			return
		}
		if !ok || (stamp.Tombstone && !meta.Tombstone) {
			if !ok {
				log.Printf("restoring %q to primary server %s", k, primaryServer)
			} else {
				log.Printf("scrubber replicating tombstone of %q to %s", k, primaryServer)
			}
			err := replicateObj(primaryServer, k, objF)
			if err != nil {
				logScrubError(SCRUB_EREPL, "scrubber replication of %q failed: %s", k, err)
				return
			}
		}
		keepObject := false
		for i := 0; i < len(locs); i++ {
			keepObject = keepObject || ThisLocation.Equals(locs[i])
		}
		if !keepObject {
			log.Printf("scrubber removing %q, it has been moved", k)
			err = os.Remove(objPath)
			if err != nil {
				logScrubError(SCRUB_EOTHER, "unable to remove %q: %s", objPath, err)
			}
		}
	}
}

type ScrubOpts struct {
	Full bool
}

func Scrub(opts ScrubOpts) {
	log.Printf("scrub started, full=%v", opts.Full)
	atomic.StoreUint64(&_scrubInProgress, 1)

	startTotalScrubbedObjects := atomic.LoadUint64(&_totalScrubbedObjects)
	startTotalScrubbedBytes := atomic.LoadUint64(&_totalScrubbedBytes)
	startTotalReplicationErrorCount := atomic.LoadUint64(&_totalScrubReplicationErrorCount)
	startTotalCorruptionErrorCount := atomic.LoadUint64(&_totalScrubCorruptionErrorCount)
	startTotalOtherErrorCount := atomic.LoadUint64(&_totalScrubOtherErrorCount)

	defer func() {
		scrubbedObjects := atomic.LoadUint64(&_totalScrubbedObjects) - startTotalScrubbedObjects
		scrubbedBytes := atomic.LoadUint64(&_totalScrubbedBytes) - startTotalScrubbedBytes
		replicationErrorCount := atomic.LoadUint64(&_totalScrubReplicationErrorCount) - startTotalReplicationErrorCount
		corruptionErrorCount := atomic.LoadUint64(&_totalScrubCorruptionErrorCount) - startTotalCorruptionErrorCount
		otherErrorCount := atomic.LoadUint64(&_totalScrubOtherErrorCount) - startTotalOtherErrorCount
		errorCount := replicationErrorCount + corruptionErrorCount + otherErrorCount
		log.Printf("scrubbed %d object(s), %d byte(s) with %d error(s)", scrubbedObjects, scrubbedBytes, errorCount)
		atomic.StoreUint64(&_lastScrubReplicationErrorCount, replicationErrorCount)
		atomic.StoreUint64(&_lastScrubCorruptionErrorCount, corruptionErrorCount)
		atomic.StoreUint64(&_lastScrubOtherErrorCount, otherErrorCount)
		atomic.StoreUint64(&_lastScrubbedBytes, scrubbedBytes)
		atomic.StoreUint64(&_scrubInProgress, 0)
	}()

	dispatch := make(chan string)

	errg, _ := errgroup.WithContext(context.Background())
	const N_SCRUB_WORKERS = 4
	for i := 0; i < N_SCRUB_WORKERS; i++ {
		errg.Go(func() error {
			for {
				path, ok := <-dispatch
				if !ok {
					return nil
				}
				ScrubObject(path, opts)
				atomic.AddUint64(&_totalScrubbedObjects, 1)
			}
		})
	}

	objectCount := uint64(0)
	err := filepath.WalkDir(DataDir, func(path string, e fs.DirEntry, err error) error {
		if e.IsDir() {
			return nil
		}
		if strings.HasSuffix(path, "$tmp") {
			stat, err := os.Stat(path)
			if err != nil {
				if errors.Is(err, os.ErrNotExist) {
					return nil
				}
				logScrubError(SCRUB_EOTHER, "error stating temporary file: %s", err)
				return nil
			}
			// Cleanup interrupted puts after a long delay.
			if stat.ModTime().Add(24 * 90 * time.Hour).Before(time.Now()) {
				log.Printf("scrubber removing expired temporary file %q", path)
				err := os.Remove(path)
				if err != nil {
					logScrubError(SCRUB_EOTHER, "error removing %q: %s", path, err)
				}
			}
			return nil
		}
		dispatch <- path
		objectCount += 1
		return nil
	})
	if err != nil {
		logScrubError(SCRUB_EOTHER, "scrub walk had an error: %s", err)
	}
	atomic.StoreUint64(&_lastScrubbedObjects, objectCount)

	close(dispatch)
	err = errg.Wait()
	if err != nil {
		logScrubError(SCRUB_EOTHER, "scrub worker had an error: %s", err)
	}

	atomic.AddUint64(&_scrubsCompleted, 1)
}

var (
	rebalanceTrigger chan struct{} = make(chan struct{})

	_lastScrubbedBytes               uint64
	_lastScrubbedObjects             uint64
	_lastScrubCorruptionErrorCount   uint64
	_lastScrubOtherErrorCount        uint64
	_lastScrubReplicationErrorCount  uint64
	_totalScrubbedBytes              uint64
	_totalScrubbedObjects            uint64
	_totalScrubCorruptionErrorCount  uint64
	_totalScrubOtherErrorCount       uint64
	_totalScrubReplicationErrorCount uint64
	_scrubInProgress                 uint64
	_scrubsCompleted                 uint64
)

const (
	SCRUB_EOTHER = iota
	SCRUB_EREPL
	SCRUB_ECORRUPT
)

func logScrubError(class int, format string, a ...interface{}) {
	switch class {
	case SCRUB_EREPL:
		atomic.AddUint64(&_totalScrubReplicationErrorCount, 1)
	case SCRUB_ECORRUPT:
		atomic.AddUint64(&_totalScrubCorruptionErrorCount, 1)
	default:
		atomic.AddUint64(&_totalScrubOtherErrorCount, 1)
	}
	log.Printf(format, a...)
}

func ScrubForever() {
	full := true // XXX store the state somewhere?
	// XXX config/better/intervals.
	fullScrubTicker := time.NewTicker(5 * time.Minute)
	fastScrubTicker := time.NewTicker(30 * time.Second)
	for {
		startCfg := GetClusterConfig()
		Scrub(ScrubOpts{Full: full}) // XXX config full and not.
		endCfg := GetClusterConfig()
		if startCfg != endCfg {
			// The config changed while scrubbing, we must scrub again with
			// the new config to handle any placement changes.
			full = false
			continue
		}
		select {
		case <-fullScrubTicker.C:
			full = true
		case <-fastScrubTicker.C:
			full = false
		case <-rebalanceTrigger:
			full = false
		}
	}
}

func nodeInfoHandler(w http.ResponseWriter, req *http.Request) {

	counters := struct {
		LastScrubCorruptionErrorCount  uint64
		LastScrubOtherErrorCount       uint64
		LastScrubReplicationErrorCount uint64
		LastScrubbedBytes              uint64
		LastScrubbedObjects            uint64

		TotalScrubCorruptionErrorCount  uint64
		TotalScrubOtherErrorCount       uint64
		TotalScrubReplicationErrorCount uint64
		TotalScrubbedBytes              uint64
		TotalScrubbedObjects            uint64
		ScrubInProgress                 uint64
		ScrubsCompleted                 uint64
	}{
		LastScrubCorruptionErrorCount:   atomic.LoadUint64(&_lastScrubCorruptionErrorCount),
		LastScrubOtherErrorCount:        atomic.LoadUint64(&_lastScrubOtherErrorCount),
		LastScrubReplicationErrorCount:  atomic.LoadUint64(&_lastScrubReplicationErrorCount),
		LastScrubbedBytes:               atomic.LoadUint64(&_lastScrubbedBytes),
		LastScrubbedObjects:             atomic.LoadUint64(&_lastScrubbedObjects),
		TotalScrubCorruptionErrorCount:  atomic.LoadUint64(&_totalScrubCorruptionErrorCount),
		TotalScrubOtherErrorCount:       atomic.LoadUint64(&_totalScrubOtherErrorCount),
		TotalScrubReplicationErrorCount: atomic.LoadUint64(&_totalScrubReplicationErrorCount),
		TotalScrubbedBytes:              atomic.LoadUint64(&_totalScrubbedBytes),
		TotalScrubbedObjects:            atomic.LoadUint64(&_totalScrubbedObjects),
		ScrubInProgress:                 uint64(atomic.LoadUint64(&_scrubInProgress)),
		ScrubsCompleted:                 atomic.LoadUint64(&_scrubsCompleted),
	}

	buf, err := json.Marshal(&counters)
	if err != nil {
		internalError(w, "unable to marshal counters: %s", err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(buf)
}

type ClusterConfig struct {
	ConfigBytes      []byte
	ClusterSecret    string
	PlacementRules   []CrushSelection
	StorageHierarchy *StorageHierarchy
}

func (cfg *ClusterConfig) Crush(k string) ([]Location, error) {
	return cfg.StorageHierarchy.Crush(k, cfg.PlacementRules)
}

var _clusterConfig atomic.Value

func SetClusterConfig(cfg *ClusterConfig) {
	if !cfg.StorageHierarchy.ContainsStorageNodeAtLocation(ThisLocation) {
		log.Printf("WARNING - config storage hierarchy does not contain the current node at %s.", ThisLocation)
	}
	_clusterConfig.Store(cfg)
	select {
	case rebalanceTrigger <- struct{}{}:
	default:
	}
}

func GetClusterConfig() *ClusterConfig {
	config, _ := _clusterConfig.Load().(*ClusterConfig)
	return config
}

func ParseClusterConfig(configYamlBytes []byte) (*ClusterConfig, error) {

	newConfig := &ClusterConfig{
		ConfigBytes: configYamlBytes,
	}

	rawConfig := struct {
		ClusterSecret  string   `yaml:"cluster-secret"`
		StorageSchema  string   `yaml:"storage-schema"`
		PlacementRules []string `yaml:"placement-rules"`
		StorageNodes   []string `yaml:"storage-nodes"`
	}{}

	decoder := yaml.NewDecoder(bytes.NewReader(configYamlBytes))
	decoder.KnownFields(true)
	err := decoder.Decode(&rawConfig)
	if err != nil {
		return nil, fmt.Errorf("unable to load yaml config: %w", err)
	}

	newConfig.ClusterSecret = rawConfig.ClusterSecret

	newConfig.StorageHierarchy, err = NewStorageHierarchyFromSchema(rawConfig.StorageSchema)
	if err != nil {
		return nil, fmt.Errorf("unable parse storage-schema %q: %w", rawConfig.StorageSchema, err)
	}

	parseNodeInfo := func(s string) (*StorageNodeInfo, error) {
		parts, err := shlex.Split(s)
		if err != nil {
			return nil, fmt.Errorf("unable to split storage node spec %q into components: %w", err)
		}
		if len(parts) < 3 {
			return nil, fmt.Errorf("storage node needs at least 3 components")
		}

		weight, err := strconv.ParseInt(parts[0], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("error parsing weight %q: %w", parts[0], err)
		}

		var defunct bool
		switch parts[1] {
		case "healthy":
			defunct = false
		case "defunct":
			defunct = true
		default:
			return nil, fmt.Errorf("unknown node status %q, expected 'healthy' or 'defunct'", parts[1])
		}

		return &StorageNodeInfo{
			TotalSpace: weight, // XXX rename to weight.
			Failed:     defunct,
			Location:   Location(parts[2:]),
		}, nil
	}

	// TODO rename CrushSelection to PlacementRule
	parsePlacementRule := func(s string) (CrushSelection, error) {
		parts, err := shlex.Split(s)
		if err != nil {
			return CrushSelection{}, fmt.Errorf("unable to split placement rule %q into components: %w", err)
		}
		if len(parts) < 1 {
			return CrushSelection{}, fmt.Errorf("unexpected empty placement rule")
		}
		switch parts[0] {
		case "select":
			if len(parts) != 3 {
				return CrushSelection{}, fmt.Errorf("select placement rules require 2 arguments")
			}
			typeName := parts[1]
			count, err := strconv.Atoi(parts[2])
			if err != nil {
				return CrushSelection{}, fmt.Errorf("unable to parse select count %q: %w", err)
			}
			return CrushSelection{
				Type:  typeName,
				Count: count,
			}, nil
		default:
			return CrushSelection{}, fmt.Errorf("unexpected placement operator %q", parts[0])
		}
	}

	for _, placementRuleString := range rawConfig.PlacementRules {
		placementRule, err := parsePlacementRule(placementRuleString)
		if err != nil {
			return nil, fmt.Errorf("unable parse placement rule %q: %w", placementRuleString, err)
		}
		newConfig.PlacementRules = append(newConfig.PlacementRules, placementRule)
	}

	for _, storageNodeString := range rawConfig.StorageNodes {
		nodeInfo, err := parseNodeInfo(storageNodeString)
		if err != nil {
			return nil, fmt.Errorf("unable parse %q storage-schema: %w", storageNodeString, err)
		}
		err = newConfig.StorageHierarchy.AddStorageNode(nodeInfo)
		if err != nil {
			return nil, fmt.Errorf("unable add %q to storage hierarchy: %w", storageNodeString, err)
		}
	}
	newConfig.StorageHierarchy.Finish()

	return newConfig, nil
}

func ReloadClusterConfigFromFile(configPath string) error {
	configBytes, err := os.ReadFile(configPath)
	if err != nil {
		return err
	}
	currentConfig := GetClusterConfig()
	if currentConfig != nil {
		if bytes.Equal(configBytes, currentConfig.ConfigBytes) {
			return nil
		}
	}
	newConfig, err := ParseClusterConfig(configBytes)
	if err != nil {
		return err
	}
	SetClusterConfig(newConfig)
	return nil
}

func WatchClusterConfigForever(configPath string) {
	lastUpdate := time.Now()
	for {
		stat, err := os.Stat(configPath)
		if err != nil {
			log.Printf("unable to stat config: %s", err)
			time.Sleep(1 * time.Second)
			continue
		}
		if stat.ModTime().After(lastUpdate) {
			log.Printf("detected config change, reloading.")
			err = ReloadClusterConfigFromFile(configPath)
			if err != nil {
				log.Printf("error reloading config: %s", err)
				time.Sleep(1 * time.Second)
				continue
			}
			lastUpdate = stat.ModTime()
		}

		// Check the config on fixed unix time boundaries, this
		// means our cluster is more likely to reload their configs
		// in sync when polling a network config.
		const RELOAD_BOUNDARY = 60
		nowUnix := time.Now().Unix()
		delaySecs := int64(RELOAD_BOUNDARY / 2)
		// XXX loop is dumb (but works).
		for {
			if (nowUnix+delaySecs)%RELOAD_BOUNDARY == 0 {
				break
			}
			delaySecs += 1
		}
		time.Sleep(time.Duration(delaySecs) * time.Second)
	}
}

func main() {

	listenAddress := flag.String("listen-address", "", "Address to listen on.")
	location := flag.String("location", "", "Storage location specification, defaults to http://${listen-address}.")
	dataDir := flag.String("data-dir", "", "Directory to store objects under.")
	clusterConfigFile := flag.String("cluster-config", "./crushstore-cluster.conf", "Directory to store objects under.")

	flag.Parse()

	if *dataDir == "" {
		log.Fatalf("-data-dir not specified.")
	}

	_, err := os.Stat(*dataDir)
	if err != nil {
		log.Fatalf("error checking -data-dir: %s", err)
	}
	DataDir = *dataDir

	if *listenAddress == "" {
		log.Fatalf("-listen-address not specified.")
	}

	if *location == "" {
		*location = fmt.Sprintf("http://%s", *listenAddress)
	}
	parsedLocation, err := shlex.Split(*location)
	if err != nil {
		log.Fatalf("error parsing -location: %s", err)
	}
	ThisLocation = Location(parsedLocation)

	log.Printf("serving location %v", ThisLocation)

	err = ReloadClusterConfigFromFile(*clusterConfigFile)
	if err != nil {
		log.Fatalf("error loading initial config: %s", err)
	}
	log.Printf("serving hierarchy:\n%s\n", GetClusterConfig().StorageHierarchy.AsciiTree())

	go WatchClusterConfigForever(*clusterConfigFile)

	http.HandleFunc("/put", putHandler)
	http.HandleFunc("/get", getHandler)
	http.HandleFunc("/check", checkHandler)
	http.HandleFunc("/delete", deleteHandler)
	http.HandleFunc("/node_info", nodeInfoHandler)

	log.Printf("serving on %s", *listenAddress)

	go ScrubForever()

	http.ListenAndServe(*listenAddress, nil)
}
