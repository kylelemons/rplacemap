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

	"github.com/kylelemons/rplacemap/dataset"
)

const CanvasSize = 1024

type tileData struct {
	ready  chan struct{}
	pixels [][]uint8
}

func (d *tileData) init(records []dataset.Record) {
	defer close(d.ready)

	pixels := make([][]uint8, CanvasSize)
	for r := range pixels {
		pixels[r] = make([]uint8, CanvasSize)
	}

	for _, rec := range records {
		pixels[int(rec.Y)][int(rec.X)] = rec.Color
	}

	d.pixels = pixels

	glog.Infof("Tile data ready")
}

type window struct {
	PixelData             [][]uint8
	TileX, TileY          int
	TileWidth, TileHeight int
	PixelScale            int
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

func clamp(v, max int) int {
	if v > max {
		return max
	}
	return v
}

const GlobalScale = 4

func (w window) At(x, y int) color.Color {
	pX := x * GlobalScale / w.PixelScale
	pY := y * GlobalScale / w.PixelScale

	idx := w.PixelData[pY%CanvasSize][pX%CanvasSize]
	return dataset.Palette[idx]
}

var _ image.Image = new(window)

var tilePath = regexp.MustCompile(`^/tiles/(\d+)_(\d+)_z(\d+)_(\d+)x(\d+).png$`)

func (d *tileData) Handle(rw http.ResponseWriter, r *http.Request) {
	select {
	case <-d.ready:
	case <-r.Context().Done():
		http.Error(rw, "not ready", http.StatusServiceUnavailable)
		return
	}

	m := tilePath.FindStringSubmatch(r.URL.Path)
	if m == nil {
		http.Error(rw, "not found", http.StatusNotFound)
		return
	}
	glog.V(1).Infof("Serving %q", r.URL.Path)

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
		PixelData:  d.pixels,
		TileX:      x,
		TileY:      y,
		TileWidth:  w,
		TileHeight: h,
		PixelScale: 1 << z,
	}
	writePNG(rw, win)
}

func Handler(records chan []dataset.Record) http.HandlerFunc {
	data := &tileData{
		ready: make(chan struct{}),
	}
	go func() {
		recs := <-records
		data.init(recs)
		records <- recs
	}()
	return data.Handle
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
