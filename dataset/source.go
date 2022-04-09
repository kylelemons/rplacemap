package dataset

import (
	"bufio"
	"context"
	"fmt"
	"image/color"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/golang/glog"
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

	type errorSource struct {
		source int
		err    error
	}
	var (
		chunks = make(chan chunkSource, 2*len(src.URLs))
		errors = make(chan errorSource, len(src.URLs))
		done   = make(chan struct{})
	)

	var wg sync.WaitGroup
	for i := range src.URLs {
		i, u := i, src.URLs[i]

		wg.Add(1)
		go func() {
			defer wg.Done()
			errors <- errorSource{i, src.download(ctx, i, u, chunks)}
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

	var (
		processed         int
		sourceLineNumbers = make([]int, len(src.URLs))
	)
	for {
		select {
		case <-done:
			return &out, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		case es := <-errors:
			if err := es.err; err != nil {
				return nil, fmt.Errorf("download[%d]: %w", es.source, err)
			}
		case chunk := <-chunks:
			glog.V(1).Infof("[%02d] Processing %d-line chunk", chunk.source, len(chunk.lines))
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

func (s *Source) download(ctx context.Context, source int, u *url.URL, chunks chan chunkSource) error {
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
	glog.Infof("[%02d] Starting download of %q", source, u)

	// Spread out downloads and logs a tiny bit
	time.Sleep(time.Duration(source) * 50 * time.Millisecond)

	// Progress updates:
	//   Print a progress update periodically.
	//   We should be loading a static file, so content length should be provided.
	var processed, total int64 = 0, resp.ContentLength
	progress := time.NewTicker(5 * time.Second)
	defer progress.Stop()
	printProgress := func() {
		percent := processed * 100 / total
		glog.Infof("[%02d] Progress: %3d%% [% -50s]", source, percent, progressBar[:percent/2])
	}

	readBuffer := bufio.NewReaderSize(resp.Body, 10*1024)
	lines := bufio.NewScanner(readBuffer)

	var lineno int
	var pending []string
	for lines.Scan() {
		line := lines.Text()
		processed += int64(len(line)) + 1 // count the newline that isn't returned
		lineno++

		select {
		case <-progress.C:
			printProgress()
		default:
		}

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
	printProgress() // everyone likes the 100% downloaded bit :)
	if processed != total {
		glog.Warningf("[%02d] Processed %d/%d bytes; incomplete download?", source, processed, total)
	}

	glog.Infof("[%02d] Downloaded complete (%d records, %.2fMiB, took %s)", source,
		lineno, float64(total)/(1<<20), time.Since(start).Truncate(time.Second))

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
