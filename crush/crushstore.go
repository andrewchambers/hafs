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
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"
	"lukechampine.com/blake3"
)

var (
	ListenAddress = flag.String("listen-address", "", "Address to listen on.")
	DataDir       = flag.String("data-dir", "./data", "data to store objects")
)

var (
	ThisLocation Location
)

type PlacementConfig struct {
	hierarchy *StorageHierarchy
	selectors []CrushSelection
}

func (cfg *PlacementConfig) Crush(k string) ([]Location, error) {
	return cfg.hierarchy.Crush(k, cfg.selectors)
}

var _placementConfig atomic.Value

func SetPlacementConfig(cfg *PlacementConfig) {
	_placementConfig.Store(cfg)
	select {
	case scrubStartTrigger <- struct{}{}:
	default:
	}
}

func GetPlacementConfig() *PlacementConfig {
	return _placementConfig.Load().(*PlacementConfig)
}

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
		return fmt.Errorf("post object %q to %s failed with status=%s: %s", k, endpoint, resp.Status, body)
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

func checkHandler(w http.ResponseWriter, req *http.Request) {
	if req.Method != "GET" {
		panic("TODO")
	}
	q := req.URL.Query()
	k := q.Get("key")
	if k == "" {
		panic("TODO")
	}
	f, err := os.Open(filepath.Join(*DataDir, k))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		// XXX
		panic(err)
	}
	defer f.Close()
	stat, err := f.Stat()
	if err != nil {
		panic(err)
	}

	stampBytes := [8]byte{}
	_, err = f.ReadAt(stampBytes[:], 32)
	if err != nil {
		panic(err)
	}
	stamp := ObjStampFromBytes(stampBytes[:])
	buf, err := json.Marshal(ObjMeta{
		Size:               uint64(stat.Size()) - 40,
		Tombstone:          stamp.Tombstone,
		CreatedAtUnixMicro: stamp.CreatedAtUnixMicro,
	})
	if err != nil {
		panic(err)
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(buf)
}

func getHandler(w http.ResponseWriter, req *http.Request) {
	if req.Method != "GET" {
		panic("TODO")
	}
	q := req.URL.Query()
	k := q.Get("key")
	if k == "" {
		panic("TODO")
	}
	f, err := os.Open(filepath.Join(*DataDir, k))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		// XXX
		panic(err)
	}
	defer f.Close()

	_, err = f.Seek(32, io.SeekStart)
	if err != nil {
		panic(err)
	}

	stat, err := f.Stat()
	if err != nil {
		// XXX
		panic(err)
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", fmt.Sprintf("%d", stat.Size()-32))
	http.ServeContent(w, req, k, stat.ModTime(), f)
}

func putHandler(w http.ResponseWriter, req *http.Request) {
	if req.Method != "POST" {
		panic("XXX")
	}

	q := req.URL.Query()
	k := q.Get("key")
	if k == "" {
		panic("XXX")
	}
	objPath := filepath.Join(*DataDir, k)

	locs, err := GetPlacementConfig().Crush(k)
	if err != nil {
		panic(err)
	}
	primaryLoc := locs[0]
	isPrimary := primaryLoc.Equals(ThisLocation)
	isReplication := q.Get("type") == "replicate"

	// Only the primary supports non replication writes.
	if !isReplication && !isPrimary {
		endpoint := fmt.Sprintf("%s/put?key=%s", primaryLoc[len(primaryLoc)-1][1], k)
		log.Printf("not primary, redirecting put %q to primary@%s", k, endpoint)
		http.Redirect(w, req, endpoint, http.StatusTemporaryRedirect)
		return
	}

	err = req.ParseMultipartForm(16 * 1024 * 1024)
	if err != nil {
		panic(err)
	}

	dataFile, _, err := req.FormFile("data")
	if err != nil {
		panic(err)
	}
	defer dataFile.Close()

	// Write object.
	tmpF, err := os.CreateTemp(*DataDir, "obj.*.tmp")
	if err != nil {
		panic(err)
	}

	removeTmp := true
	defer func() {
		if removeTmp {
			_ = os.Remove(tmpF.Name())
		}
	}()

	hash := [32]byte{}
	stamp := ObjStamp{}

	hasher := blake3.New(32, nil)

	if isReplication {
		header := [40]byte{}
		_, err := io.ReadFull(dataFile, header[:])
		if err != nil {
			panic(err)
		}
		copy(hash[:], header[:32])
		stamp.FieldsFromBytes(header[32:40])
		_, err = hasher.Write(header[32:40])
		if err != nil {
			panic(err)
		}
		_, err = tmpF.Write(header[:])
		if err != nil {
			panic(err)
		}
	} else {
		header := [40]byte{}
		stamp.Tombstone = false
		stamp.CreatedAtUnixMicro = uint64(time.Now().UnixMicro())
		stampBytes := stamp.ToBytes()
		_, err = hasher.Write(stampBytes[:])
		if err != nil {
			panic(err)
		}
		copy(header[32:], stampBytes[:])
		_, err := tmpF.Write(header[:])
		if err != nil {
			panic(err)
		}
	}

	_, err = io.Copy(io.MultiWriter(tmpF, hasher), dataFile)
	if err != nil {
		panic(err)
	}

	if isReplication {
		actualHash := [32]byte{}
		copy(actualHash[:], hasher.Sum(nil))
		if hash != actualHash {
			panic(fmt.Errorf(
				"sent hash %s did not equal computed hash %s",
				hex.EncodeToString(hash[:]),
				hex.EncodeToString(actualHash[:])))
		}
	} else {
		copy(hash[:], hasher.Sum(nil))
		_, err = tmpF.WriteAt(hash[:], 0)
		if err != nil {
			panic(err)
		}
	}

	existingF, err := os.Open(objPath)
	if err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			panic(err)
		}
	}
	if existingF != nil {
		existingHeader := [40]byte{}
		_, err := existingF.ReadAt(existingHeader[:], 0)
		if err != nil {
			panic(err)
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
		panic(err)
	}

	err = tmpF.Close()
	if err != nil {
		panic(err)
	}

	err = os.Rename(tmpF.Name(), objPath)
	if err != nil {
		panic(err)
	}
	// XXX TODO
	// FlushDirectory()
	// Flush directory - can we batch these together with other requests?
	removeTmp = false

	if isPrimary {
		// If this is primary, we must spread the
		// data to all the other nodes in the placement.
		// XXX do in parallel.
		for i := 0; i < len(locs); i++ {
			loc := locs[i]
			if loc.Equals(ThisLocation) {
				continue
			}

			server := loc[len(loc)-1][1]
			if isReplication {
				meta, ok, err := checkObj(server, k)
				if err != nil {
					panic(err)
				}
				if ok && stamp.Tombstone == meta.Tombstone {
					// Don't need to replicate, the remote is up to date.
					continue
				}
			}

			objF, err := os.Open(objPath)
			if err != nil {
				panic(err)
			}
			defer objF.Close()
			log.Printf("replicating %q to %s", k, server)
			err = replicateObj(server, k, objF)
			if err != nil {
				panic(err)
			}
		}
	}
}

func deleteHandler(w http.ResponseWriter, req *http.Request) {
	if req.Method != "POST" {
		panic("XXX")
	}

	q := req.URL.Query()
	k := q.Get("key")
	if k == "" {
		panic("XXX")
	}
	objPath := filepath.Join(*DataDir, k)

	locs, err := GetPlacementConfig().Crush(k)
	if err != nil {
		panic(err)
	}
	primaryLoc := locs[0]

	if !primaryLoc.Equals(ThisLocation) {
		endpoint := fmt.Sprintf("%s/delete?key=%s", primaryLoc[len(primaryLoc)-1][1], k)
		log.Printf("not primary, redirecting delete %q to primary@%s", k, endpoint)
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
	tmpF, err := os.CreateTemp(*DataDir, "obj.*.tmp")
	if err != nil {
		panic(err)
	}
	defer tmpF.Close()
	removeTmp := true
	defer func() {
		if removeTmp {
			_ = os.Remove(tmpF.Name())
		}
	}()

	_, err = tmpF.Write(obj[:])
	if err != nil {
		panic(err)
	}

	err = tmpF.Sync()
	if err != nil {
		panic(err)
	}

	err = tmpF.Close()
	if err != nil {
		panic(err)
	}

	err = os.Rename(tmpF.Name(), objPath)
	if err != nil {
		panic(err)
	}
	removeTmp = false

	// XXX TODO
	// FlushDirectory()
	// Flush directory - can we batch these together with other requests?

	// Replicate the delete.
	// XXX do in parallel.
	for i := 1; i < len(locs); i++ {
		loc := locs[i]
		server := loc[len(loc)-1][1]
		objF, err := os.Open(objPath)
		if err != nil {
			panic(err)
		}
		defer objF.Close()
		log.Printf("replicating deletion of %q to %s", k, server)
		err = replicateObj(server, k, objF)
		if err != nil {
			panic(err)
		}
	}
}

type ScrubOpts struct {
	Full bool
}

func Scrub(opts ScrubOpts) {
	log.Printf("scrub started")
	defer log.Printf("scrub finished")

	filepath.WalkDir(*DataDir, func(objPath string, e fs.DirEntry, err error) error {
		if e.IsDir() {
			return nil
		}
		k := filepath.Base(objPath)

		if strings.HasSuffix(k, ".tmp") {
			// TODO cleanup old tmp files.
			return nil
		}

		locs, err := GetPlacementConfig().Crush(k)
		if err != nil {
			log.Printf("scrubber unable to place %q: %s", k, err)
			return nil
		}
		primaryLoc := locs[0]

		objF, err := os.Open(objPath)
		if err != nil {
			log.Printf("scrubber unable to open %q: %s", k, err)
			return nil
		}
		defer objF.Close()

		stampBytes := [8]byte{}
		_, err = objF.ReadAt(stampBytes[:], 32)
		if err != nil {
			panic(err)
		}
		stamp := ObjStampFromBytes(stampBytes[:])

		const TOMBSTONE_EXPIRY = 120 * time.Second // TODO a real/configurable value.
		if stamp.IsExpired(time.Now(), TOMBSTONE_EXPIRY) {
			log.Printf("scrubber removing %q, it has expired", k)
			err := os.Remove(objPath)
			if err != nil {
				log.Printf("unable to remove %q: %s", k, err)
			}
			return nil
		}

		if opts.Full {
			expectedHash := [32]byte{}
			actualHash := [32]byte{}
			_, err := io.ReadFull(objF, expectedHash[:])
			if err != nil {
				log.Printf("io error scrubbing %q: %s", k, err)
			}
			hasher := blake3.New(32, nil)
			_, err = io.Copy(hasher, objF)
			if err != nil {
				log.Printf("io error scrubbing %q: %s", k, err)
			}
			copy(actualHash[:], hasher.Sum(nil))
			if expectedHash != actualHash {
				log.Printf("scrub detected corrupt %q", k)
				err = os.Remove(objPath)
				if err != nil {
					log.Printf("io error removing %q: %s", k, err)
				}
				// Nothing more to do for this entry.
				return nil
			}

			_, err = objF.Seek(0, io.SeekStart)
			if err != nil {
				log.Printf("io error seeking %q", k)
				return nil
			}
		}

		if ThisLocation.Equals(primaryLoc) {
			for i := 1; i < len(locs); i++ {
				server := locs[i][len(locs[i])-1][1]
				meta, ok, err := checkObj(server, k)
				if err != nil {
					log.Printf("scrubber stat failed: %s", err)
					continue
				}
				needsReplication := false
				if ok {
					if stamp.Tombstone && !meta.Tombstone {
						log.Printf("scrubber replicating tombstone of %q to %s", k, server)
						needsReplication = true
					}
				} else {
					log.Printf("scrubber replicating missing %q to %s", k, server)
					needsReplication = true
				}
				if needsReplication {
					err := replicateObj(server, k, objF)
					if err != nil {
						log.Printf("scrubber replication of %q failed: %s", k, err)
					}
				}
			}
		} else {
			primaryServer := primaryLoc[len(primaryLoc)-1][1]
			meta, ok, err := checkObj(primaryServer, k)
			if err != nil {
				log.Printf("scrubber was unable to verify primary placement of %q: %s", k, err)
				return nil
			}
			if !ok || (stamp.Tombstone && !meta.Tombstone) {
				log.Printf("restoring %q to primary server %s", k, primaryServer)
				err := replicateObj(primaryServer, k, objF)
				if err != nil {
					log.Printf("scrubber replication of %q failed: %s", k, err)
					return nil
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
					log.Printf("unable to remove %q: %s", k, err)
				}
			}
		}

		return nil
	})
}

var scrubStartTrigger chan struct{} = make(chan struct{})

func scrubberForever() {
	full := false
	fullScrub := time.NewTicker(5 * time.Minute)
	fastScrub := time.NewTicker(30 * time.Second)
	for {
		startCfg := GetPlacementConfig()
		Scrub(ScrubOpts{Full: full}) // XXX config full and not.
		if GetPlacementConfig() != startCfg {
			// The config changed while scrubbing
			// we must scrub again to shift chunks to their desired placement
			// in a timely way.
			continue
		}
		select {
		case <-fullScrub.C: // XXX config/better/interval.
			full = true
		case <-fastScrub.C:
			full = false
		case <-scrubStartTrigger:
		}
	}
}

func refreshHandler(w http.ResponseWriter, req *http.Request) {
	if req.Method != "POST" {
		panic("TODO")
	}
	panic("TODO")
}

func startScrubHandler(w http.ResponseWriter, req *http.Request) {
	if req.Method != "POST" {
		panic("TODO")
	}
	panic("TODO")
}

func main() {

	flag.Parse()

	if *ListenAddress == "" {
		log.Fatalf("-listen-address not specified")
	}

	hostName, err := os.Hostname()
	if err != nil {
		panic(err)
	}

	_, err = os.Stat(*DataDir)
	if err != nil {
		log.Fatalf("error checking -data-dir: %s", err)
	}

	ThisLocation = Location{
		{"host", hostName},
		{"server", fmt.Sprintf("http://%s", *ListenAddress)},
	}

	storageHierarchy, err := NewStorageHierarchyFromSpec("host server")
	if err != nil {
		panic(err)
	}

	nodes := []*StorageNodeInfo{
		&StorageNodeInfo{
			Location: Location{
				{"host", "black"},
				{"server", "http://127.0.0.1:5000"},
			},
			TotalSpace: 100,
		},
		&StorageNodeInfo{
			Location: Location{
				{"host", "black"},
				{"server", "http://127.0.0.1:5001"},
			},
			TotalSpace: 100,
		},
	}

	for _, ni := range nodes {
		err := storageHierarchy.AddStorageNode(ni)
		if err != nil {
			log.Fatalf("unable to configure storage hierarchy: %s", err)
		}
	}

	storageHierarchy.Finish()

	log.Printf("serving hierarchy:\n%s\n", storageHierarchy.AsciiTree())

	SetPlacementConfig(&PlacementConfig{
		hierarchy: storageHierarchy,
		selectors: []CrushSelection{
			CrushSelection{
				Type:  "host",
				Count: 1,
			},
			CrushSelection{
				Type:  "server",
				Count: 2,
			},
		},
	})

	http.HandleFunc("/put", putHandler)
	http.HandleFunc("/get", getHandler)
	http.HandleFunc("/check", checkHandler)
	http.HandleFunc("/delete", deleteHandler)
	http.HandleFunc("/refresh", refreshHandler)
	http.HandleFunc("/start_scrub", startScrubHandler)

	log.Printf("serving on %s", *ListenAddress)

	go scrubberForever()
	http.ListenAndServe(*ListenAddress, nil)
}
