package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log"
	"lukechampine.com/blake3"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"
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

var _placementConfigs []*PlacementConfig

func getPlacementConfigs() []*PlacementConfig {
	return _placementConfigs
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

type ObjMeta struct {
	Size     int64
	B3Sum256 string
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

	hash := [32]byte{}
	_, err = f.ReadAt(hash[:], 0)
	if err != nil {
		panic(err)
	}

	buf, err := json.Marshal(ObjMeta{
		B3Sum256: hex.EncodeToString(hash[:]),
		Size:     stat.Size(),
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

	placementCfg := getPlacementConfigs()[0]
	locs, err := placementCfg.Crush(k)
	if err != nil {
		panic(err)
	}
	primaryLoc := locs[0]
	isPrimary := primaryLoc.Equals(ThisLocation)
	isReplication := q.Get("type") == "replicate"
	alreadyHashed := isReplication

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

	if alreadyHashed {
		_, err := io.ReadFull(dataFile, hash[:])
		if err != nil {
			panic(err)
		}
		_, err = tmpF.Write(hash[:])
		if err != nil {
			panic(err)
		}
	} else {
		// Reserve space for the future hash.
		_, err := tmpF.Write(hash[:])
		if err != nil {
			panic(err)
		}
	}

	hasher := blake3.New(32, nil)

	_, err = io.Copy(io.MultiWriter(tmpF, hasher), dataFile)
	if err != nil {
		panic(err)
	}

	if alreadyHashed {
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
		existingHash := [32]byte{}
		_, err := existingF.ReadAt(existingHash[:], 0)
		if err != nil {
			panic(err)
		}
		if hash != existingHash {
			w.WriteHeader(400)
			w.Write([]byte("conflicting put"))
			return
		}
	}

	err = tmpF.Sync()
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
				_, ok, err := checkObj(server, k)
				if err != nil {
					panic(err)
				}
				if ok {
					/* TODO check stat up to date. */
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

func scrub(fullScrub bool) {
	log.Printf("scrub started")
	defer log.Printf("scrub finished")

	placementCfg := getPlacementConfigs()[0]

	filepath.WalkDir(*DataDir, func(objPath string, e fs.DirEntry, err error) error {
		if e.IsDir() {
			return nil
		}
		k := filepath.Base(objPath)

		if strings.HasSuffix(k, ".tmp") {
			// TODO cleanup old tmp files.
			return nil
		}

		locs, err := placementCfg.Crush(k)
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

		if fullScrub {
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
				_, ok, err := checkObj(server, k)
				if err != nil {
					log.Printf("scrubber stat failed: %s", err)
					continue
				}
				if !ok /* XXX | out of date */ {
					log.Printf("restoring %q to %s", k, server)
					err := replicateObj(server, k, objF)
					if err != nil {
						log.Printf("scrubber replication failed: %s", err)
						continue
					}
				}
			}
		} else {
			primaryServer := primaryLoc[len(primaryLoc)-1][1]
			_, ok, err := checkObj(primaryServer, k)
			if err != nil {
				log.Printf("scrubber was unable to verify primary placement of %q: %s", k, err)
				return nil
			}
			if !ok {
				log.Printf("restoring %q to primary server %s", k, primaryServer)
				err := replicateObj(primaryServer, k, objF)
				if err == nil {
					ok = true
				}
				if err != nil {
					log.Printf("scrubber replication failed: %s", err)
				}
			}
			if ok {
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
		}

		return nil
	})
}

var scrubStartChan chan struct{} = make(chan struct{}, 1)

func scrubberForever() {
	for {
		scrub(true)
		select {
		case <-time.After(30 * time.Second): // XXX config/better/interval.
		case <-scrubStartChan:
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

	_placementConfigs = []*PlacementConfig{&PlacementConfig{
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
	},
	}

	http.HandleFunc("/get", getHandler)
	http.HandleFunc("/put", putHandler)
	http.HandleFunc("/check", checkHandler)
	http.HandleFunc("/refresh", refreshHandler)
	http.HandleFunc("/start_scrub", startScrubHandler)

	log.Printf("serving on %s", *ListenAddress)

	go scrubberForever()
	http.ListenAndServe(*ListenAddress, nil)
}
