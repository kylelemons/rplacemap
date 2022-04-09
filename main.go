package main

import (
	"context"
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

	"github.com/kylelemons/rplacemap/v2/dataset"
	"github.com/kylelemons/rplacemap/v2/internal/gsync"
	"github.com/kylelemons/rplacemap/v2/static"
	"github.com/kylelemons/rplacemap/v2/tiles"
	"github.com/kylelemons/rplacemap/v2/timelapse"
)

var (
	download = flag.Bool("download", false, "Force re-download of r/place map data")
	addr     = flag.String("http", "localhost:0", "HTTP serve address")
	year     = flag.String("year", "2022", "Year to download / serve")

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

	glog.Infof("Welcome to the r/place %s map explorer!", *year)

	futureDataset := gsync.FutureOf[*dataset.Dataset]()
	go func() {
		if _, err := futureDataset.Provide(loadDataset()); err != nil {
			glog.Fatalf("Failed to initialize: %s", err)
		}
	}()

	serve(futureDataset)
}

func loadDataset() (*dataset.Dataset, error) {
	if err := os.MkdirAll(cacheDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create cache directory: %s", err)
	}

	file := fmt.Sprintf("place_data_%s%s", *year, dataset.FileSuffix)
	var source dataset.Source
	switch *year {
	case "2017":
		source = dataset.Dataset2017
	case "2022":
		source = dataset.Dataset2022
	default:
		return nil, fmt.Errorf("no known data source for --year=%s", *year)
	}

	datasetFile := filepath.Join(cacheDir, file)
	var loaded *dataset.Dataset
	if _, err := os.Stat(datasetFile); os.IsNotExist(err) || *download {
		glog.Infof("No dataset found, downloading...")
		ds, err := dataset.Download(context.TODO(), source)
		if err != nil {
			return nil, fmt.Errorf("failed to download dataset: %s", err)
		}
		go func() {
			if err := ds.SaveTo(datasetFile); err != nil {
				os.Remove(datasetFile) // best effort delete the corrupted file
				glog.Warningf("Failed to cache dataset to file: %s", err)
			}
		}()
		loaded = ds
	} else if err != nil {
		return nil, fmt.Errorf("failed to check cache: %s", err)
	} else {
		glog.Infof("Loading cached dataset (--download to re-download)...")
		glog.Infof("  File: %s", datasetFile)
		ds, err := dataset.Load(datasetFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load dataset: %s", err)
		}
		loaded = ds
	}
	return loaded, nil
}

func serve(futureDataset *gsync.Future[*dataset.Dataset]) {
	http.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 1*time.Second)
		defer cancel()

		if _, err := futureDataset.Wait(ctx); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}

		fmt.Fprintf(w, "OK")
	})

	http.HandleFunc("/tiles/", tiles.Handler(futureDataset))

	renderTimelapse := timelapse.Handler(futureDataset)
	http.HandleFunc("/render/timelapse.apng", renderTimelapse)
	http.HandleFunc("/render/timelapse.gif", renderTimelapse)

	http.Handle("/static/", static.Handler(*dev))
	http.Handle("/", http.RedirectHandler("/static/index.html", http.StatusTemporaryRedirect))

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		glog.Exitf("Failed to listen on %q: %s", *addr, err)
	}
	glog.Infof("Serving HTTP on http://%s", lis.Addr())
	glog.V(2).Infof(" - Debug: http://%s/debug/pprof", lis.Addr())

	glog.Exitf("HTTP Serve exited: %s", http.Serve(lis, nil))
}
