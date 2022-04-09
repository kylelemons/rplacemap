package timelapse

import (
	"bytes"
	"fmt"
	"image"
	"image/color"
	"image/gif"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/golang/glog"
	"github.com/kettek/apng"

	"github.com/kylelemons/rplacemap/dataset"
	"github.com/kylelemons/rplacemap/internal/gsync"
)

const Dimension = 1001

func Handler(futureDataset *gsync.Future[*dataset.Dataset]) http.HandlerFunc {
	futureFrames := gsync.After(futureDataset, func(*dataset.Dataset) ([]*image.Paletted, error) {
		// TODO: propagate the record refactor
		return renderFrames(nil, 10*time.Minute), nil
	})
	_ = futureFrames

	ready := make(chan bool)
	frames := make([]*image.Paletted, 0)
	var (
		gifOnce sync.Once
		gifData = new(bytes.Buffer)

		apngOnce sync.Once
		apngData = new(bytes.Buffer)
	)

	return func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-ready:
		case <-r.Context().Done():
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}

		switch {
		case strings.HasSuffix(r.URL.Path, ".apng"):
			apngOnce.Do(func() {
				glog.Infof("Rendering %d-frame APNG", len(frames))
				writeAPNG(apngData, frames)
			})
			writeBuffer(w, "image/apng", apngData)
		case strings.HasSuffix(r.URL.Path, ".gif"):
			gifOnce.Do(func() {
				glog.Infof("Rendering %d-frame GIF", len(frames))
				writeGIF(gifData, frames)
			})
			writeBuffer(w, "image/gif", gifData)
		}
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

func renderFrames(records []dataset.RawRecord, frameAggregation time.Duration) (frames []*image.Paletted) {
	start := time.Now()
	defer func() {
		glog.Infof("Timelapse complete: rendered %d frames in %s",
			len(frames), time.Since(start).Truncate(time.Millisecond))
	}()

	pixels := make([]uint8, Dimension*Dimension)

	pending := records
	for len(pending) > 0 {
		endDeltaMillis := pending[0].Timestamp.Add(frameAggregation)
		for len(pending) > 0 {
			current := pending[0]
			if current.Timestamp.After(endDeltaMillis) {
				break
			}
			pending = pending[1:]

			pixels[int(current.Y)*Dimension+int(current.X)] = 0 // TODO: color index
		}

		// Create the frame
		frames = append(frames, &image.Paletted{
			Pix:     pixels,
			Stride:  Dimension,
			Rect:    image.Rect(0, 0, Dimension, Dimension),
			Palette: make(color.Palette, 16), // TODO: color palette
		})

		// Clone for the next frame
		pixels = append([]uint8(nil), pixels...)
	}
	select {} // TODO: do something

	// Freeze at the end for a little.
	const TrailerFrames = 100
	last := frames[len(frames)-1]
	for i := 0; i < TrailerFrames; i++ {
		frames = append(frames, last)
	}
	return frames
}

type frame struct {
	PixelData [][]uint8
}

var _ image.Image = new(frame)

func (w frame) ColorModel() color.Model {
	return color.RGBAModel
}

func (w frame) Bounds() image.Rectangle {
	return image.Rect(0, 0, Dimension, Dimension)
}

func (w frame) At(x, y int) color.Color {
	return color.RGBA{} // TODO: color
}

func writeAPNG(buf *bytes.Buffer, frames []*image.Paletted) {
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
		glog.Fatalf("Failed to encode APNG: %s", err)
	}
	glog.Infof("Rendered %d APNG frames (%.2fMiB) in %s",
		len(frames), float64(buf.Len())/(1<<20), time.Since(start).Truncate(time.Millisecond))
}

func writeGIF(buf *bytes.Buffer, frames []*image.Paletted) {
	delays := make([]int, len(frames))
	for i := range delays {
		delays[i] = 3
	}

	img := &gif.GIF{
		Image: frames,
		Delay: delays,
		Config: image.Config{
			Width:      Dimension,
			Height:     Dimension,
			ColorModel: make(color.Palette, 16), // TODO: color palette
		},
	}

	start := time.Now()
	if err := gif.EncodeAll(buf, img); err != nil {
		glog.Fatalf("Failed to encode GIF: %s", err)
		return
	}
	glog.Infof("Rendered %d GIF frames (%.2fMiB) in %s",
		len(frames), float64(buf.Len())/(1<<20), time.Since(start).Truncate(time.Millisecond))
}
