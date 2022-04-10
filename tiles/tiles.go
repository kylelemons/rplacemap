package tiles

import (
	"bytes"
	"fmt"
	"image"
	"image/color"
	"image/png"
	"net/http"
	"regexp"

	"github.com/golang/glog"

	"github.com/kylelemons/rplacemap/v2/dataset"
	"github.com/kylelemons/rplacemap/v2/internal/gsync"
)

type window struct {
	Size                  int
	CanvasSize            int
	PixelData             [][]uint8
	Palette               color.Palette
	TileX, TileY          int
	TileWidth, TileHeight int
	PixelScale            int
	GlobalScale           int
}

func (w window) ColorModel() color.Model {
	return color.RGBAModel
}

func (w window) Bounds() image.Rectangle {
	x0 := w.TileX * w.TileWidth
	y0 := w.TileY * w.TileHeight
	x1 := x0 + w.TileWidth
	y1 := y0 + w.TileHeight
	return image.Rect(x0, y0, x1, y1)
}

var transparentWhite = color.RGBA{
	R: 255,
	G: 255,
	B: 255,
	A: 0,
}

func (w window) At(x, y int) color.Color {
	pX := x * w.GlobalScale / w.PixelScale
	pY := y * w.GlobalScale / w.PixelScale

	if pY < 0 || pX < 0 || pY >= w.CanvasSize || pX >= w.CanvasSize {
		return transparentWhite
	}

	// idx := w.PixelData[pY%w.Size][pX%w.Size]
	idx := w.PixelData[pY][pX]
	return w.Palette[idx]
}

var _ image.Image = new(window)

var tilePath = regexp.MustCompile(`^/tiles/(\d+)_(\d+)_z(\d+)_(\d+)x(\d+).png$`)

type tileData struct {
	pixels      [][]uint8
	palette     color.Palette
	canvasSize  int
	globalScale int
}

func Handler(futureDataset *gsync.Future[*dataset.Dataset]) http.HandlerFunc {
	futurePixels := gsync.After(futureDataset, func(ds *dataset.Dataset) (d tileData, err error) {
		var lastNonwhitePixel int32
		for _, chunk := range ds.Chunks {
			for _, row := range chunk.Pixels {
				for _, events := range row {
					for _, ev := range events {
						if ev.ColorIndex > 2 { // transp, black, white
							if ev.DeltaMillis > lastNonwhitePixel {
								lastNonwhitePixel = ev.DeltaMillis
							}
						}
					}
				}
			}
		}

		// Make an image that is a perfect multiple of 256, since that's what is expected by Leaflet
		size := ds.ChunkStride * 256
		pixels := make([][]uint8, size)
		for r := range pixels {
			pixels[r] = make([]uint8, size)
			for c := range pixels[r] {
				ev := ds.At(r, c)
				if len(ev) == 0 {
					continue
				}
				for i := len(ev) - 1; i >= 0; i-- {
					if ev[i].DeltaMillis > lastNonwhitePixel {
						// Ignore pixel set events after "the whitening"
						continue
					}
					pixels[r][c] = ev[i].ColorIndex
					break
				}
			}
		}
		d.pixels = pixels
		d.palette = ds.Palette
		d.canvasSize = ds.Size
		d.globalScale = ds.ChunkStride // works because chunks are also 256x256

		glog.Infof("Tile data ready (%dx%d)", size, size)
		return d, nil
	})
	return func(rw http.ResponseWriter, r *http.Request) {
		m := tilePath.FindStringSubmatch(r.URL.Path)
		if m == nil {
			http.Error(rw, "not found", http.StatusNotFound)
			return
		}
		glog.V(2).Infof("Serving %q", r.URL.Path)

		data, err := futurePixels.Wait(r.Context())
		if err != nil {
			http.Error(rw, "not ready: "+err.Error(), http.StatusServiceUnavailable)
			return
		}

		var x, y, z, w, h int
		for _, parse := range []struct {
			ptr *int
			str string
		}{
			{&x, m[1]},
			{&y, m[2]},
			{&z, m[3]},
			{&w, m[4]},
			{&h, m[5]},
		} {
			if _, err := fmt.Sscan(parse.str, parse.ptr); err != nil {
				http.Error(rw, err.Error(), http.StatusBadRequest)
				return
			}
		}

		win := &window{
			Size:        len(data.pixels),
			CanvasSize:  data.canvasSize,
			PixelData:   data.pixels,
			Palette:     data.palette,
			TileX:       x,
			TileY:       y,
			TileWidth:   w,
			TileHeight:  h,
			PixelScale:  1 << z,
			GlobalScale: data.globalScale,
		}
		writePNG(rw, win)
	}
}

func writePNG(w http.ResponseWriter, img *window) {
	buf := new(bytes.Buffer)
	if err := png.Encode(buf, img); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "image/png")
	w.Header().Set("Content-Length", fmt.Sprint(buf.Len()))
	buf.WriteTo(w)
}
