package dataset

import (
	"bufio"
	"compress/gzip"
	"encoding/base64"
	"encoding/gob"
	"fmt"
	"image/color"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/golang/glog"
)

type RawRecord struct {
	UnixMillis int64
	UserHash   [16]byte // pseudonymized user identifier
	X, Y       int16    // coordinates, can represent +/-32k
	Color      uint8    // 16-color palette
}

const (
	FileSuffix     = ".gob.gz"
	RequiredHeader = "ts,user_hash,x_coordinate,y_coordinate,color"
)

func Download2017(outputFile string, datasetURL *url.URL) (*Dataset, error) {
	if !strings.HasSuffix(outputFile, FileSuffix) {
		return nil, fmt.Errorf("output file %q does not have required suffix %q", outputFile, FileSuffix)
	}

	// TODO: write to tempfile and then move?

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

	start := time.Now()
	resp, err := http.DefaultClient.Get(datasetURL.String())
	if err != nil {
		return nil, fmt.Errorf("starting download of %q: %w", datasetURL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %q returned %q", datasetURL, resp.Status)
	}
	if resp.ContentLength <= 0 {
		return nil, fmt.Errorf("GET %q returned unknown Content-Length", datasetURL)
	}
	glog.Infof("Starting download of %q", datasetURL)

	// Progress updates:
	//   Print a progress update periodically.
	//   We should be loading a static file, so content length should be provided.
	var processed, total int64 = 0, resp.ContentLength
	progress := time.NewTicker(3 * time.Second)
	defer progress.Stop()
	printProgress := func() {
		percent := processed * 100 / total
		glog.Infof("Progress: %3d%% [% -50s]", percent, progressBar[:percent/2])
	}

	readBuffer := bufio.NewReaderSize(resp.Body, 10*1024)
	lines := bufio.NewScanner(readBuffer)
	var lineno int
	var records []RawRecord
	for lines.Scan() {
		line := lines.Text()
		processed += int64(len(line)) + 1 // count the newline that isn't returned
		lineno++

		select {
		case <-progress.C:
			printProgress()
		default:
		}

		if lineno == 1 {
			if got, want := line, RequiredHeader; got != want {
				return nil, fmt.Errorf("header mismatch, dataset contains %q, expecting %q", got, want)
			}
			glog.V(3).Infof("Header: %q", line)
			continue
		}

		fields := strings.Split(line, ",")
		if got, want := len(fields), 5; got != want {
			return nil, fmt.Errorf("line %d: columns = %v, want %v: line %q", lineno, got, want, line)
		}
		var (
			tsStr       = fields[0]
			userHashStr = fields[1]
			xStr, yStr  = fields[2], fields[3]
			colorStr    = fields[4]
		)
		if len(xStr) == 0 || len(yStr) == 0 || len(colorStr) == 0 {
			continue
		}

		const TimestampLayout = "2006-01-02 15:04:05.999 MST"
		ts, err := time.Parse(TimestampLayout, tsStr)
		if err != nil {
			return nil, fmt.Errorf("line %d: timestamp %q invalid: %s", lineno, tsStr, err)
		}
		userHash, err := base64.StdEncoding.DecodeString(userHashStr)
		if err != nil {
			return nil, fmt.Errorf("line %d: user hash %q invalid: %s", lineno, userHashStr, err)
		}
		x, err := strconv.ParseInt(xStr, 10, 16)
		if err != nil {
			return nil, fmt.Errorf("line %d: x coordinate %q invalid: %s", lineno, xStr, err)
		}
		y, err := strconv.ParseInt(yStr, 10, 16)
		if err != nil {
			return nil, fmt.Errorf("line %d: y coordinate %q invalid: %s", lineno, yStr, err)
		}
		color, err := strconv.ParseUint(colorStr, 10, 8)
		if err != nil {
			return nil, fmt.Errorf("line %d: color %q invalid: %s", lineno, colorStr, err)
		}

		rec := RawRecord{
			UnixMillis: ts.UnixNano() / 1e6,
			UserHash:   *((*[16]byte)(userHash)),
			X:          int16(x),
			Y:          int16(y),
			Color:      uint8(color),
		}
		records = append(records, rec)
	}
	if err := lines.Err(); err != nil {
		return nil, fmt.Errorf("downloading %q: %w", datasetURL, err)
	}
	if processed != total {
		glog.Warningf("Processed %d/%d bytes; incomplete download?", processed, total)
	}
	printProgress() // everyone likes the 100% downloaded bit :)

	glog.Infof("Downloaded dataset (%d records, %.2fMiB, took %s)",
		len(records), float64(total)/(1<<20), time.Since(start).Truncate(time.Second))

	ds := NewDataset(records, Palette2017)

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

const Version = "rplacemap-encoding-v1"

type Dataset struct {
	Version string // encoding version, should match Version

	// Global data
	Width, Height int           // Number of horizontal and vertical pixels
	Palette       color.Palette // Color indices

	// Encoding metadata
	Epoch, End  time.Time  // Base time (t0) and final timestamp
	ChunkStride int        // The number of chunks per row (to avoid repeated computation)
	UserIDs     [][16]byte // User IDs by User Index

	// Chunked data for localized processing
	Chunks []Chunk // 256x256-pixel chunks
}

type Chunk struct {
	Width, Height int // Width and Height of the chunk (since the edge chunks won't be complete)

	Pixels [256][256][]PixelEvent // Ordered events grouped by pixel
}

type PixelEvent struct {
	DeltaMillis int32 // Delta between Epoch and this event
	UserIndex   int32 // Index into the user array
	ColorIndex  uint8 // Palette color index
}

func NewDataset(records []RawRecord, palette color.Palette) *Dataset {
	start := time.Now()
	defer func() {
		glog.Infof("Encoded dataset (%d records) in %s", len(records), time.Since(start).Truncate(time.Millisecond))
	}()

	sortByTime(records)

	epoch := records[0].UnixMillis
	end := records[len(records)-1].UnixMillis

	var (
		maxX, maxY = records[0].X, records[0].Y
		users      = make(map[[16]byte]int)
	)
	for _, r := range records {
		if r.X > maxX {
			maxX = r.X
		}
		if r.Y > maxY {
			maxY = r.Y
		}
		users[r.UserHash] = len(users)
	}

	// Collapse the user IDs
	userIDs := make([][16]byte, len(users)+1)
	for id, i := range users {
		userIDs[i] = id
	}

	// Create the chunk array
	chunkCols := int(maxX+255) / 256
	chunkRows := int(maxY+255) / 256
	chunks := make([]Chunk, chunkCols*chunkRows)
	for i := range chunks {
		c := &chunks[i]

		c.Width = 256
		c.Height = 256

		if i%chunkCols == chunkCols-1 {
			c.Width = int(maxX)%256 + 1
		}
		if i/chunkCols == chunkRows-1 {
			c.Height = int(maxY)%256 + 1
		}
	}

	// Store events in the chunks
	for _, r := range records {
		x, y := int(r.X/256), int(r.Y/256)
		c := &chunks[y*chunkCols+x]

		ev := PixelEvent{
			DeltaMillis: int32(r.UnixMillis - epoch),
			UserIndex:   int32(users[r.UserHash]),
			ColorIndex:  r.Color,
		}

		col, row := uint8(r.X), uint8(r.Y) // implicitly % 256
		c.Pixels[row][col] = append(c.Pixels[row][col], ev)
	}

	return &Dataset{
		Version:     Version,
		Width:       int(maxX + 1),
		Height:      int(maxY + 1),
		Palette:     palette,
		Epoch:       time.UnixMilli(epoch),
		End:         time.UnixMilli(end),
		ChunkStride: chunkCols,
		UserIDs:     userIDs,
		Chunks:      chunks,
	}
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

func sortByTime(records []RawRecord) {
	sort.Slice(records, func(i, j int) bool {
		return records[i].UnixMillis < records[j].UnixMillis
	})
}

var progressBar = strings.Repeat("#", 50)

var Palette2017 = color.Palette{
	0:  color.RGBA{R: 0xFF, G: 0xFF, B: 0xFF, A: 0xFF},
	1:  color.RGBA{R: 0xE4, G: 0xE4, B: 0xE4, A: 0xFF},
	2:  color.RGBA{R: 0x88, G: 0x88, B: 0x88, A: 0xFF},
	3:  color.RGBA{R: 0x22, G: 0x22, B: 0x22, A: 0xFF},
	4:  color.RGBA{R: 0xFF, G: 0xA7, B: 0xD1, A: 0xFF},
	5:  color.RGBA{R: 0xE5, G: 0x00, B: 0x00, A: 0xFF},
	6:  color.RGBA{R: 0xE5, G: 0x95, B: 0x00, A: 0xFF},
	7:  color.RGBA{R: 0xA0, G: 0x6A, B: 0x42, A: 0xFF},
	8:  color.RGBA{R: 0xE5, G: 0xD9, B: 0x00, A: 0xFF},
	9:  color.RGBA{R: 0x94, G: 0xE0, B: 0x44, A: 0xFF},
	10: color.RGBA{R: 0x02, G: 0xBE, B: 0x01, A: 0xFF},
	11: color.RGBA{R: 0x00, G: 0xE5, B: 0xF0, A: 0xFF},
	12: color.RGBA{R: 0x00, G: 0x83, B: 0xC7, A: 0xFF},
	13: color.RGBA{R: 0x00, G: 0x00, B: 0xEA, A: 0xFF},
	14: color.RGBA{R: 0xE0, G: 0x4A, B: 0xFF, A: 0xFF},
	15: color.RGBA{R: 0x82, G: 0x00, B: 0x80, A: 0xFF},
}

func init() {
	// Ensure RGBA can be used in color.Palette
	gob.Register(color.RGBA{})
}
