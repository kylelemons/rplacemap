package dataset

import (
	"bufio"
	"compress/gzip"
	"encoding/gob"
	"fmt"
	"image/color"
	"math"
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

const Version = "rplacemap-encoding-v2"

type Dataset struct {
	Version string // encoding version, should match Version

	// Global data
	Size    int           // Number of horizontal and vertical pixels
	Palette color.Palette // Color indices

	// Encoding metadata
	Epoch       time.Time // Base time (t0)
	Start, End  time.Time // First and final pixel place timestamp
	ChunkStride int       // The number of chunks per row (to avoid repeated computation)
	UserIDs     []string  // User IDs by User Index

	// Chunked data for localized processing
	Chunks []Chunk // 256x256-pixel chunks
}

func (d *Dataset) At(row, col int) []PixelEvent {
	y, cy, x, cx := row/256, row%256, col/256, col%256
	return d.Chunks[y*d.ChunkStride+x].Pixels[cy][cx]
}

func (d *Dataset) TimeAfter(deltaMills int32) time.Time {
	return d.Epoch.Add(time.Duration(deltaMills) * time.Millisecond)
}

func (d *Dataset) SaveTo(outputFile string) error {
	if !strings.HasSuffix(outputFile, FileSuffix) {
		return fmt.Errorf("output file %q does not have required suffix %q", outputFile, FileSuffix)
	}
	glog.Infof("Saving dataset...")

	start := time.Now()
	tempFile, err := d.writeTemp()
	if err != nil {
		return fmt.Errorf("saving to temp: %w", err)
	}
	defer os.Remove(tempFile) // make sure it's deleted if something goes wrong

	if err := os.Rename(tempFile, outputFile); err != nil {
		return fmt.Errorf("atomic file move: %w", err)
	}
	glog.Infof("Saved dataset to file in %s", time.Since(start).Truncate(time.Millisecond))
	glog.Infof("  File: %s", outputFile)
	return nil
}

func (d *Dataset) writeTemp() (string, error) {
	start := time.Now()

	f, err := os.CreateTemp("", "rplacemap-*"+FileSuffix)
	if err != nil {
		return "", fmt.Errorf("create temporary output file: %w", err)
	}
	defer f.Close()

	writeBuffer := bufio.NewWriterSize(f, 10*1024)

	compression, err := gzip.NewWriterLevel(writeBuffer, gzip.BestCompression)
	if err != nil {
		glog.Fatalf("NewWriterlevel: %s", err) // should never happen, means our level was wrong
	}
	defer compression.Close()

	enc := gob.NewEncoder(compression)

	if err := enc.Encode(d); err != nil {
		return "", fmt.Errorf("writing dataset to %q: %w", f.Name(), err)
	}
	compression.Comment = fmt.Sprintf("r/place %s dataset", d.Epoch.Year())
	if err := compression.Close(); err != nil {
		return "", fmt.Errorf("finalizing gzip data: %w", err)
	}
	if err := writeBuffer.Flush(); err != nil {
		return "", fmt.Errorf("flushing buffer to file %q: %w", f.Name(), err)
	}
	if err := f.Sync(); err != nil {
		return "", fmt.Errorf("syncing temp file: %w", err) // contains filename
	}
	if err := f.Close(); err != nil {
		return "", fmt.Errorf("closing temp file: %w", err) // contains filename
	}
	glog.V(2).Infof("Wrote dataset to temp file in %s", time.Since(start).Truncate(time.Millisecond))
	glog.V(2).Infof("  Temp: %s", f.Name())

	return f.Name(), nil
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
				if len(ev) == 0 {
					continue
				}

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

	logSummary(d.Dataset, totalEvents)
}

func logSummary(d *Dataset, totalEvents int) {
	glog.Infof("Event details:")
	glog.Infof("  Epoch:       %s", d.Epoch.Format(TimestampLayout))
	glog.Infof("  First Pixel: %s", d.Start.Format(TimestampLayout))
	glog.Infof("  Final Pixel: %s", d.End.Format(TimestampLayout))
	glog.Infof("Canvas information:")
	glog.Infof("  Canvas:  %d x %d pixels", d.Size, d.Size)
	glog.Infof("  Palette: %d colors", len(d.Palette))
	glog.Infof("  Chunks:  %d chunks (%d x %d)", len(d.Chunks), d.ChunkStride, d.ChunkStride)
	glog.Infof("Dataset statistics:")
	glog.Infof("  %d pixels placed", totalEvents)
	glog.Infof("  %d users recorded", len(d.UserIDs))
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
	defer func() {
		glog.Infof("Dataset loaded in %s", time.Since(start).Truncate(time.Millisecond))
	}()

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

	logSummary(&ds, events)
	return &ds, nil
}

func init() {
	// Ensure RGBA can be used in color.Palette
	gob.Register(color.RGBA{})
}
