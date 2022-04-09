package dataset

import (
	"bufio"
	"compress/gzip"
	"context"
	"fmt"
	"image/color"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/golang/glog"

	"github.com/kylelemons/rplacemap/internal/progress"
)

const TimestampLayout = "2006-01-02 15:04:05.999 MST"

type Source struct {
	// Event information
	Year       int
	CanvasSize int // all canvasses so far have been square

	// Source information
	URLs []*url.URL // one or more sharded CSV files

	// Format information
	GZipped   bool                                   // if set, decompress before decoding as CSV
	Header    string                                 // header string to verify column order
	ParseLine func(line string) ([]RawRecord, error) // parse fields and disaggregate events
}

var (
	Dataset2017 = Source{
		Year:       2017,
		CanvasSize: 1001,
		URLs:       urls2017(),
		Header:     header2017,
		ParseLine:  parseLine2017,
	}
	Dataset2022 = Source{
		Year:       2022,
		CanvasSize: 2000,
		URLs:       urls2022(),
		Header:     header2022,
		GZipped:    true,
		ParseLine:  parseLine2022,
	}
)

type chunkSource struct {
	source int
	lines  []string
}

func Download(ctx context.Context, src Source) (*Dataset, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	start := time.Now()

	type errorSource struct {
		source int
		err    error
	}
	var (
		chunks      = make(chan chunkSource, 2*len(src.URLs))
		errors      = make(chan errorSource, len(src.URLs))
		done        = make(chan struct{})
		progressBar = new(progress.Bar)
	)

	var wg sync.WaitGroup
	for i := range src.URLs {
		i, u := i, src.URLs[i]

		wg.Add(1)
		go func() {
			defer wg.Done()
			errors <- errorSource{i, src.download(ctx, i, u, chunks, progressBar)}
		}()
	}
	go func() {
		wg.Wait()
		close(done)
	}()

	chunkSlice, chunkStride := src.makeChunks()
	out := Dataset{
		Version:     Version,
		Width:       src.CanvasSize,
		Height:      src.CanvasSize,
		Epoch:       time.Date(src.Year, 4, 1, 0, 0, 0, 0, time.UTC),
		ChunkStride: chunkStride,
		Chunks:      chunkSlice,
	}
	prep := partialDataset{
		Dataset: &out,
		users:   make(map[string]int),
		colors:  make(map[color.RGBA]int),
	}
	defer prep.finalize()

	printProgress := time.NewTicker(5 * time.Second)
	defer printProgress.Stop()

	var (
		processed         int
		sourceLineNumbers = make([]int, len(src.URLs))
	)
	for {
		select {
		case <-done:
			// Everybody loves the 100% bar :)
			glog.Infof("Progress: %s", progressBar)
			glog.Infof("Download complete after %s", time.Since(start).Truncate(time.Second))
			return &out, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-printProgress.C:
			glog.Infof("Progress: %s", progressBar)
		case es := <-errors:
			if err := es.err; err != nil {
				return nil, fmt.Errorf("download[%d]: %w", es.source, err)
			}
		case chunk := <-chunks:
			for _, line := range chunk.lines {
				records, err := src.ParseLine(line)
				if err != nil {
					return nil, fmt.Errorf("download[%d]: line %d (%q): %w",
						chunk.source, sourceLineNumbers[chunk.source], line, err)
				}
				processed++
				sourceLineNumbers[chunk.source]++
				for _, rec := range records {
					prep.add(rec)
				}
			}
		}
	}
}

func (s *Source) download(ctx context.Context, source int, u *url.URL, chunks chan chunkSource, bar *progress.Bar) error {
	start := time.Now()
	req := &http.Request{Method: http.MethodGet, URL: u}
	resp, err := http.DefaultClient.Do(req.WithContext(ctx))
	if err != nil {
		return fmt.Errorf("starting download of %q: %w", u, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("GET %q returned %q", u, resp.Status)
	}
	if resp.ContentLength <= 0 {
		return fmt.Errorf("GET %q returned unknown Content-Length", u)
	}
	glog.V(1).Infof("[%02d] Starting download of %q", source, u)

	// Spread out downloads and logs a tiny bit
	time.Sleep(time.Duration(source) * 50 * time.Millisecond)

	// Count the bytes read off the wire against the ContentLength
	reader := bar.Wrap(resp.Body, resp.ContentLength)

	// Buffer our reads for better performance
	reader = bufio.NewReaderSize(reader, 10*1024)

	// Decompress if requested
	if s.GZipped {
		zr, err := gzip.NewReader(reader)
		if err != nil {
			return fmt.Errorf("initializing decompression: %s", err)
		}
		defer zr.Close()
		reader = zr
	}

	// Scan for and discard newlines for easier processing
	lines := bufio.NewScanner(reader)

	var lineno int
	var pending []string
	for lines.Scan() {
		line := lines.Text()
		lineno++

		if lineno == 1 && line == s.Header {
			glog.V(3).Infof("[%02d] Header: %q", source, line)
			continue
		}

		pending = append(pending, line)

		if len(pending) > 1000 {
			select {
			case chunks <- chunkSource{source, pending}:
				pending = make([]string, 0, len(pending))
			default:
			}
		}
	}
	if len(pending) > 0 {
		chunks <- chunkSource{source, pending}
	}
	if err := lines.Err(); err != nil {
		return fmt.Errorf("downloading %q: %w", u, err)
	}

	glog.V(1).Infof("[%02d] Shard downloaded (%d records, %.2fMiB, took %s)", source,
		lineno, float64(resp.ContentLength)/(1<<20), time.Since(start).Truncate(time.Second))

	return nil
}

func (s *Source) makeChunks() (chunks []Chunk, stride int) {
	// Create the lines array
	stride = int(s.CanvasSize+255) / 256
	chunks = make([]Chunk, stride*stride)
	for i := range chunks {
		c := &chunks[i]

		c.Width = 256
		c.Height = 256

		if i%stride == stride-1 {
			c.Width = s.CanvasSize%256 + 1
		}
		if i/stride == stride-1 {
			c.Height = s.CanvasSize%256 + 1
		}
	}
	return chunks, stride
}
