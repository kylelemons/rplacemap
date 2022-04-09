package dataset

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/gob"
	"fmt"
	"image/color"
	"math"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/golang/glog"
)

type RawRecord struct {
	Timestamp time.Time
	UserHash  string // pseudonymized user identifier
	X, Y      int
	Color     color.RGBA
}

const (
	FileSuffix = ".gob.gz"
)

func Download2017(outputFile string, datasetURL *url.URL) (*Dataset, error) {
	if !strings.HasSuffix(outputFile, FileSuffix) {
		return nil, fmt.Errorf("output file %q does not have required suffix %q", outputFile, FileSuffix)
	}

	f, err := os.Create(outputFile)
	if err != nil {
		return nil, fmt.Errorf("creating output file: %w", err) // contains filename
	}
	defer f.Close() // double close OK

	writeBuffer := bufio.NewWriterSize(f, 10*1024)
	compression, err := gzip.NewWriterLevel(writeBuffer, gzip.BestCompression)
	if err != nil {
		glog.Fatalf("NewWriterlevel: %s", err) // should never happen, means our level was wrong
	}
	enc := gob.NewEncoder(compression)

	ds, err := Download(context.TODO(), Dataset2017)
	if err != nil {
		return nil, fmt.Errorf("downloading: %s", err)
	}

	writeStart := time.Now()
	if err := enc.Encode(ds); err != nil {
		return nil, fmt.Errorf("writing dataset to %q: %w", outputFile, err)
	}
	compression.Comment = "r/place 2017 dataset"
	if err := compression.Close(); err != nil {
		return nil, fmt.Errorf("finalizing gzip data: %w", err)
	}
	if err := writeBuffer.Flush(); err != nil {
		return nil, fmt.Errorf("flushing buffer to file %q: %w", outputFile, err)
	}
	if err := f.Close(); err != nil {
		return nil, fmt.Errorf("closing output file: %w", err) // contains filename
	}
	glog.Infof("Wrote dataset to file in %s", time.Since(writeStart).Truncate(time.Millisecond))
	glog.Infof("  File: %s", outputFile)

	return ds, nil
}

const Version = "rplacemap-encoding-v2"

type Dataset struct {
	Version string // encoding version, should match Version

	// Global data
	Width, Height int           // Number of horizontal and vertical pixels
	Palette       color.Palette // Color indices

	// Encoding metadata
	Epoch       time.Time // Base time (t0)
	Start, End  time.Time // First and final pixel place timestamp
	ChunkStride int       // The number of chunks per row (to avoid repeated computation)
	UserIDs     []string  // User IDs by User Index

	// Chunked data for localized processing
	Chunks []Chunk // 256x256-pixel chunks
}

type partialDataset struct {
	*Dataset
	users  map[string]int
	colors map[color.RGBA]int
}

func (d *partialDataset) add(rec RawRecord) {
	if rec.Timestamp.After(d.End) {
		d.End = rec.Timestamp
	}
	if _, ok := d.colors[rec.Color]; !ok {
		d.colors[rec.Color] = len(d.colors)
	}
	if _, ok := d.users[rec.UserHash]; !ok {
		d.users[rec.UserHash] = len(d.users)
	}

	x, y := rec.X/256, rec.Y/256
	c := &d.Chunks[y*d.ChunkStride+x]

	ev := PixelEvent{
		DeltaMillis: int32(rec.Timestamp.Sub(d.Epoch).Milliseconds()),
		UserIndex:   int32(d.users[rec.UserHash]),
		ColorIndex:  uint8(d.colors[rec.Color]),
	}

	col, row := uint8(rec.X), uint8(rec.Y) // implicitly % 256
	c.Pixels[row][col] = append(c.Pixels[row][col], ev)
}

func (d *partialDataset) finalize() {
	start := time.Now()
	defer func() {
		glog.Infof("Dataset finalized in %s", time.Since(start).Truncate(time.Millisecond))
	}()

	// Sanity checks
	if got, max := len(d.colors), 256; got > max {
		glog.Fatalf("Color palette (%d) exceeds one byte (%d)", got, max)
	}

	// Prepare the lookup tables
	d.UserIDs = make([]string, len(d.users))
	for u, i := range d.users {
		d.UserIDs[i] = u
	}
	d.Palette = make(color.Palette, len(d.colors))
	for c, i := range d.colors {
		d.Palette[i] = c
	}

	// Stats
	var (
		totalEvents int
		first       int32 = math.MaxInt32
	)

	// Sort the events
	for _, chunk := range d.Chunks {
		for _, r := range chunk.Pixels {
			for _, ev := range r {
				sort.Slice(ev, func(i, j int) bool {
					if a, b := ev[i].DeltaMillis, ev[j].DeltaMillis; a != b {
						return a < b
					}
					return false
				})
				totalEvents += len(r)
				if ts := ev[0].DeltaMillis; ts < first {
					first = ts
				}
			}
		}
	}

	d.Start = d.Epoch.Add(time.Duration(first) * time.Millisecond)

	glog.Infof("Dataset statistics:")
	glog.Infof("  % 7d pixels placed", totalEvents)
	glog.Infof("  % 7d users recorded", len(d.UserIDs))
	glog.Infof("Event timestamps:")
	glog.Infof("  Epoch:       %s", d.Epoch)
	glog.Infof("  First Pixel: %s", d.Start)
	glog.Infof("  Final Pixel: %s", d.End)
}

type Chunk struct {
	Width, Height int // Width and Height of the lines (since the edge chunks won't be complete)

	Pixels [256][256][]PixelEvent // Ordered events grouped by pixel
}

type PixelEvent struct {
	DeltaMillis int32 // Delta between Epoch and this event
	UserIndex   int32 // Index into the user array
	ColorIndex  uint8 // Palette color index
}

func Load(filename string) (*Dataset, error) {
	if !strings.HasSuffix(filename, FileSuffix) {
		return nil, fmt.Errorf("input file %q does not have required suffix %q", filename, FileSuffix)
	}

	f, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("opening input file: %w", err) // contains filename
	}
	defer f.Close() // no data to flush

	readBuffer := bufio.NewReaderSize(f, 10*1024)
	compression, err := gzip.NewReader(readBuffer)
	if err != nil {
		return nil, fmt.Errorf("initializing decompression of %q: %w", filename, err)
	}
	defer compression.Close()
	dec := gob.NewDecoder(compression)

	start := time.Now()

	var ds Dataset
	if err := dec.Decode(&ds); err != nil {
		return nil, fmt.Errorf("decoding dataset from %q: %w (run with --download to redownload)", filename, err)
	}
	if got, want := ds.Version, Version; got != want {
		return nil, fmt.Errorf("version = %q, want %q (run with --download to redownload)", got, want)
	}

	var events int
	for _, c := range ds.Chunks {
		for _, row := range c.Pixels {
			for _, ev := range row {
				events += len(ev)
			}
		}
	}

	glog.Infof("Loaded %d events in %s", events, time.Since(start).Truncate(time.Millisecond))
	return &ds, nil
}

var progressBar = strings.Repeat("#", 50)

func init() {
	// Ensure RGBA can be used in color.Palette
	gob.Register(color.RGBA{})
}
