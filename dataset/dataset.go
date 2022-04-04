package dataset

import (
	"bufio"
	"compress/gzip"
	"encoding/base64"
	"encoding/gob"
	"errors"
	"fmt"
	"image/color"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/golang/glog"
)

type Record struct {
	UnixMillis int64
	UserHash   [16]byte // pseudonymized user identifier
	X, Y       int16    // coordinates, can represent +/-32k
	Color      uint8    // 16-color palette
}

const (
	FileSuffix     = ".gob.gz"
	RequiredHeader = "ts,user_hash,x_coordinate,y_coordinate,color"
)

func Download(outputFile string, datasetURL *url.URL) ([]Record, error) {
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
	var records []Record
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

		rec := Record{
			UnixMillis: ts.UnixNano() / 1e6,
			UserHash:   *((*[16]byte)(userHash)),
			X:          int16(x),
			Y:          int16(y),
			Color:      uint8(color),
		}
		if err := enc.Encode(rec); err != nil {
			return nil, fmt.Errorf("line %d: record %d: encoding record: %w", lineno, records, err)
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

	sortByTime(records)
	glog.Infof("Downloaded dataset (%.2fMiB, took %s)",
		float64(total)/(1<<20), time.Since(start).Truncate(time.Second))
	glog.Infof("  Wrote to: %s", outputFile)

	return records, nil
}

func Load(filename string) ([]Record, error) {
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
	var records []Record
	for {
		var rec Record
		if err := dec.Decode(&rec); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return nil, fmt.Errorf("decoding record %d: %w", len(records)+1, err)
		}
		records = append(records, rec)
	}

	sortByTime(records)
	glog.Infof("Decoded %d records in %s", len(records), time.Since(start).Truncate(time.Millisecond))
	return records, nil
}

func sortByTime(records []Record) {
	sort.Slice(records, func(i, j int) bool {
		return records[i].UnixMillis < records[j].UnixMillis
	})
}

var progressBar = strings.Repeat("#", 50)

var Palette = color.Palette{
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
