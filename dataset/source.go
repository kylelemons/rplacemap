package dataset

import (
	"bufio"
	"context"
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/golang/glog"
)

const TimestampLayout = "2006-01-02 15:04:05.999 MST"

type Source struct {
	// Source information
	URLs []*url.URL // one or more sharded CSV files

	// Format information
	GZipped   bool                                   // if set, decompress before decoding as CSV
	Header    string                                 // header string to verify column order
	ParseLine func(line string) ([]RawRecord, error) // parse fields and disaggregate events

	// Event information
	CanvasSize int // all canvasses so far have been square
}

var (
	Dataset2017 = Source{
		URLs:       urls2017(),
		Header:     header2017,
		ParseLine:  parseLine2017,
		CanvasSize: 1001,
	}
	Dataset2022 = Source{
		URLs:       urls2022(),
		Header:     header2022,
		GZipped:    true,
		ParseLine:  parseLine2022,
		CanvasSize: 2000,
	}
)

type chunkSource struct {
	source int
	lines  []string
}

func (s *Source) Download(ctx context.Context) (*Dataset, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	type errorSource struct {
		source int
		err    error
	}
	var (
		chunks = make(chan chunkSource, 2*len(s.URLs))
		errors = make(chan errorSource, len(s.URLs))
		done   = make(chan struct{})
	)

	var wg sync.WaitGroup
	for i := range s.URLs {
		i, u := i, s.URLs[i]

		wg.Add(1)
		go func() {
			defer wg.Done()
			errors <- errorSource{i, s.download(ctx, i, u, chunks)}
		}()
	}
	go func() {
		wg.Wait()
		close(done)
	}()

	var (
		out               Dataset
		processed         int
		sourceLineNumbers = make([]int, len(s.URLs))
	)
	for {
		select {
		case <-done:
			return out.prepare()
		case <-ctx.Done():
			return nil, ctx.Err()
		case es := <-errors:
			if err := es.err; err != nil {
				return nil, fmt.Errorf("download[%d]: %w", es.source, err)
			}
		case chunk := <-chunks:
			glog.V(1).Infof("[%02d] Processing %d-line chunk", chunk.source, len(chunk.lines))
			for _, line := range chunk.lines {
				records, err := s.ParseLine(line)
				if err != nil {
					return nil, fmt.Errorf("download[%d]: line %d (%q): %w",
						chunk.source, sourceLineNumbers[chunk.source], line, err)
				}
				processed++
				sourceLineNumbers[chunk.source]++
				for _, rec := range records {
					out.add(rec)
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

		if lineno == 1 {
			if got, want := line, s.Header; got != want {
				return fmt.Errorf("header mismatch, dataset contains %q, expecting %q", got, want)
			}
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
