package timelapse

import (
	"bytes"
	"fmt"
	"image"
	"image/gif"
	"net/http"
	"strings"
	"time"

	"github.com/golang/glog"
	"github.com/kettek/apng"

	"github.com/kylelemons/rplacemap/v2/dataset"
	"github.com/kylelemons/rplacemap/v2/internal/gsync"
	"github.com/kylelemons/rplacemap/v2/internal/progress"
)

type timelineData struct {
	ds     *dataset.Dataset
	frames []*image.Paletted
}

func Handler(futureDataset *gsync.Future[*dataset.Dataset]) http.HandlerFunc {
	futureFrames := gsync.After(futureDataset, func(ds *dataset.Dataset) (*timelineData, error) {
		return &timelineData{
			ds:     ds,
			frames: renderFrames(ds, 10*time.Minute),
		}, nil
	})
	futureAPNG := gsync.After(futureFrames, func(data *timelineData) (*bytes.Buffer, error) {
		buf := new(bytes.Buffer)
		return buf, writeAPNG(buf, data.ds, data.frames)
	})
	futureGIF := gsync.After(futureFrames, func(data *timelineData) (*bytes.Buffer, error) {
		buf := new(bytes.Buffer)
		return buf, writeGIF(buf, data.ds, data.frames)
	})

	return func(w http.ResponseWriter, r *http.Request) {
		var (
			future *gsync.Future[*bytes.Buffer]
			ctype  string
		)
		switch {
		case strings.HasSuffix(r.URL.Path, ".apng"):
			ctype, future = "image/apng", futureAPNG
		case strings.HasSuffix(r.URL.Path, ".gif"):
			ctype, future = "image/gif", futureGIF
		}
		buf, err := future.Wait(r.Context())
		if err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		writeBuffer(w, ctype, buf)
	}
}

func writeBuffer(w http.ResponseWriter, ctype string, buf *bytes.Buffer) {
	start := time.Now()

	w.Header().Set("Content-Type", ctype)
	w.Header().Set("Content-Length", fmt.Sprint(buf.Len()))

	w.Write(buf.Bytes())
	glog.Infof("Wrote %.2fMiB %q image in %s",
		float64(buf.Len())/(1<<20), ctype, time.Since(start).Truncate(time.Millisecond))
}

func renderFrames(ds *dataset.Dataset, frameAggregation time.Duration) (frames []*image.Paletted) {
	start := time.Now()
	defer func() {
		glog.Infof("Timelapse complete: rendered %d frames in %s",
			len(frames), time.Since(start).Truncate(time.Millisecond))
	}()

	pixels := make([]uint8, ds.Size*ds.Size)
	pending := make([][][]dataset.PixelEvent, ds.Size)
	var dbg int
	for r := range pending {
		pending[r] = make([][]dataset.PixelEvent, ds.Size)
		for c := range pending[r] {
			pending[r][c] = ds.At(r, c)
			dbg += len(pending[r][c])
		}
	}
	glog.Infof("DEBUG: %d queues", dbg)

	dbg = 0
	for _, c := range ds.Chunks {
		for _, row := range c.Pixels {
			for _, ev := range row {
				dbg += len(ev)
			}
		}
	}
	glog.Infof("DEBUG: %d queues without At", dbg)

	bar := progress.NewBar(progress.Counter)
	bar.AddTotal(int64(ds.End.Sub(ds.Epoch) / frameAggregation))
	printBar := time.NewTicker(60 * time.Second)
	defer printBar.Stop()

	for threshold := ds.Epoch; threshold.Before(ds.End); threshold = threshold.Add(frameAggregation) {
		endDeltaMillis := threshold.Sub(ds.Epoch).Milliseconds()

		select {
		case <-printBar.C:
			glog.V(1).Infof("Timelapse: %s", bar)
		default:
		}

		w := ds.Size
		for r := range pending {
			for c := range pending[r] {
				for ev := pending[r][c]; len(ev) > 0; {
					current := ev[0]
					if int64(current.DeltaMillis) >= endDeltaMillis {
						break
					} else {
						ev = ev[1:]
					}
					pixels[r*w+c] = current.ColorIndex
				}
			}
		}

		// Create the frame
		frames = append(frames, &image.Paletted{
			Pix:     pixels,
			Stride:  ds.Size,
			Rect:    image.Rect(0, 0, ds.Size, ds.Size),
			Palette: ds.Palette,
		})

		// Clone for the next frame
		pixels = append([]uint8(nil), pixels...)
		bar.AddProgress(1)
	}
	glog.V(1).Infof("Timelapse: %s", bar)

	// Freeze at the end for a little.
	const TrailerFrames = 100
	last := frames[len(frames)-1]
	for i := 0; i < TrailerFrames; i++ {
		frames = append(frames, last)
	}
	return frames
}

func writeAPNG(buf *bytes.Buffer, ds *dataset.Dataset, frames []*image.Paletted) error {
	apngFrames := make([]apng.Frame, len(frames))
	for i := range apngFrames {
		apngFrames[i] = apng.Frame{
			Image:            frames[i],
			DelayNumerator:   1,
			DelayDenominator: 30,
		}
	}

	img := apng.APNG{
		Frames:    apngFrames,
		LoopCount: 0,
	}

	start := time.Now()
	if err := apng.Encode(buf, img); err != nil {
		return fmt.Errorf("encoding APNG: %s", err)
	}
	glog.Infof("Rendered %d APNG frames (%.2fMiB) in %s",
		len(frames), float64(buf.Len())/(1<<20), time.Since(start).Truncate(time.Millisecond))
	return nil
}

func writeGIF(buf *bytes.Buffer, ds *dataset.Dataset, frames []*image.Paletted) error {
	delays := make([]int, len(frames))
	for i := range delays {
		delays[i] = 3
	}

	img := &gif.GIF{
		Image: frames,
		Delay: delays,
		Config: image.Config{
			Width:      ds.Size,
			Height:     ds.Size,
			ColorModel: ds.Palette,
		},
	}

	start := time.Now()
	if err := gif.EncodeAll(buf, img); err != nil {
		return fmt.Errorf("encoding GIF: %s", err)
	}
	glog.Infof("Rendered %d GIF frames (%.2fMiB) in %s",
		len(frames), float64(buf.Len())/(1<<20), time.Since(start).Truncate(time.Millisecond))
	return nil
}
