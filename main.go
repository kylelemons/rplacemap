package main

import (
	"flag"
	"fmt"
	"net"
	"net/http"
	_ "net/http/pprof"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"github.com/emersion/go-appdir"
	"github.com/golang/glog"

	"github.com/kylelemons/rplacemap/dataset"
	"github.com/kylelemons/rplacemap/static"
	"github.com/kylelemons/rplacemap/tiles"
	"github.com/kylelemons/rplacemap/timelapse"
)

var (
	download = flag.Bool("download", false, "Force re-download of r/place map data")
	addr     = flag.String("http", "localhost:0", "HTTP serve address")

	dev = flag.Bool("dev", false, "Don't use builtin assets")
)

var (
	cacheDir = appdir.New("rplacemap").UserCache()
)

var (
	// Full 2017 dataset, CSV (~1GiB)
	placeData2017 = &url.URL{
		Scheme: "https",
		Host:   "storage.googleapis.com",
		Path:   "/justin_bassett/place_tiles",
	}
)

func main() {
	flag.Set("logtostderr", "true")
	flag.Set("v", "2")
	flag.Parse()

	records := make(chan []dataset.RawRecord, 1)
	go func() {
		loadRecords()
		//records <- loadRecords()
	}()

	serve(records)
}

func loadRecords() *dataset.Dataset {
	if err := os.MkdirAll(cacheDir, 0755); err != nil {
		glog.Fatalf("Failed to create cache directory: %s", err)
	}

	datasetFile := filepath.Join(cacheDir, "place_data_2017.gob.gz")
	var records *dataset.Dataset
	if _, err := os.Stat(datasetFile); os.IsNotExist(err) || *download {
		glog.Infof("No dataset found, downloading...")
		recs, err := dataset.Download2017(datasetFile, placeData2017)
		if err != nil {
			glog.Fatalf("Failed to download dataset: %s", err)
		}
		records = recs
	} else if err != nil {
		glog.Fatalf("Failed to check cache: %s", err)
	} else {
		glog.Infof("Loading cached dataset (--download to re-download)...")
		glog.Infof("  File: %s", datasetFile)
		recs, err := dataset.Load(datasetFile)
		if err != nil {
			glog.Fatalf("Failed to load dataset: %s", err)
		}
		records = recs
	}
	return records
}

func serve(records chan []dataset.RawRecord) {
	http.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		select {
		case recs := <-records:
			records <- recs
			fmt.Fprintf(w, "OK: %d records", len(records))
		case <-time.After(1 * time.Second):
			http.Error(w, "tiles not ready", http.StatusServiceUnavailable)
		}
	})

	http.HandleFunc("/tiles/", tiles.Handler(records))

	renderTimelapse := timelapse.Handler(records)
	http.HandleFunc("/render/timelapse.apng", renderTimelapse)
	http.HandleFunc("/render/timelapse.gif", renderTimelapse)

	http.Handle("/static/", static.Handler(*dev))
	http.Handle("/", http.RedirectHandler("/static/index.html", http.StatusTemporaryRedirect))

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		glog.Exitf("Failed to listen on %q: %s", *addr, err)
	}
	glog.Infof("Serving HTTP on http://%s", lis.Addr())

	glog.Exitf("HTTP Serve exited: %s", http.Serve(lis, nil))
}
