package dataset

import (
	"encoding/hex"
	"fmt"
	"image/color"
	"net/url"
	"strconv"
	"strings"
	"time"
)

func urls2022() []*url.URL {
	urls := make([]*url.URL, 79)
	for i := range urls {
		urls[i] = &url.URL{
			Scheme: "https",
			Host:   "placedata.reddit.com",
			Path:   fmt.Sprintf("/data/canvas-history/2022_place_canvas_history-%012d.csv.gzip", i),
		}
	}
	return urls
}

func parseLine2022(line string) ([]RawRecord, error) {
	// Example:
	//   2022-04-04 00:55:57.168 UTC,tPcrtm7OtEmSThdRSWmB7jmTF9lUVZ1pltNv1oKqPY9bom/EGIO3/b5kjRenbD3vMF48psnR9MnhIrTT1bpC9A==,#6A5CFF,"1908,1854"
	fields := strings.Split(line, ",")
	if got, short, long := len(fields), 5, 7; got != short && got != long {
		return nil, fmt.Errorf("columns = %v, want %v or %v: line %q", got, short, long, line)
	}
	var (
		tsStr      = fields[0]
		userHash   = fields[1]
		colorStr   = fields[2]
		xStr, yStr = fields[3], fields[4]
	)
	xStr = strings.Trim(xStr, `"`)
	yStr = strings.Trim(yStr, `"`)
	if len(xStr) == 0 || len(yStr) == 0 || len(colorStr) == 0 {
		return nil, nil
	}

	ts, err := time.Parse(TimestampLayout, tsStr)
	if err != nil {
		return nil, fmt.Errorf("timestamp %q invalid: %s", tsStr, err)
	}
	x, err := strconv.Atoi(xStr)
	if err != nil {
		return nil, fmt.Errorf("x coordinate %q invalid: %s", xStr, err)
	}
	y, err := strconv.Atoi(yStr)
	if err != nil {
		return nil, fmt.Errorf("y coordinate %q invalid: %s", yStr, err)
	}
	col, err := parseColor(colorStr)
	if err != nil {
		return nil, fmt.Errorf("color %q invalid: %s", colorStr, err)
	}
	if len(fields) == 7 {
		return adminRect(fields, x, y, ts, userHash, col)
	}

	return []RawRecord{{
		Timestamp: ts,
		UserHash:  userHash,
		X:         x,
		Y:         y,
		Color:     col,
	}}, nil
}

func adminRect(fields []string, x0, y0 int, ts time.Time, userHash string, col color.RGBA) ([]RawRecord, error) {
	var (
		xStr, yStr = fields[5], fields[6]
	)
	xStr = strings.Trim(xStr, `"`)
	yStr = strings.Trim(yStr, `"`)

	x1, err := strconv.Atoi(xStr)
	if err != nil {
		return nil, fmt.Errorf("x2 coordinate %q invalid: %s", xStr, err)
	}
	y1, err := strconv.Atoi(yStr)
	if err != nil {
		return nil, fmt.Errorf("y2 coordinate %q invalid: %s", yStr, err)
	}

	// Normalize rect so we can iterate
	if x0 > x1 {
		x0, x1 = x1, x0
	}
	if y0 > y1 {
		y0, y1 = y1, y0
	}

	records := make([]RawRecord, 0, (x1-x0)*(y1-y0))
	for x := x0; x <= x1; x++ {
		for y := y0; y <= y1; y++ {
			records = append(records, RawRecord{
				Timestamp: ts,
				UserHash:  userHash,
				X:         x,
				Y:         y,
				Color:     col,
			})
		}
	}
	return records, nil
}

const header2022 = "timestamp,user_id,pixel_color,coordinate"

func parseColor(s string) (color.RGBA, error) {
	if got, want := len(s), 7; got != want {
		return color.RGBA{}, fmt.Errorf("length = %d, want %d (format #rrggbb)", got, want)
	}
	if got, want := rune(s[0]), '#'; got != want {
		return color.RGBA{}, fmt.Errorf("color[0] = %q, want %q", got, want)
	}
	numeric, err := hex.DecodeString(s[1:])
	if err != nil {
		return color.RGBA{}, fmt.Errorf("invalid hex: %s", err)
	}
	return color.RGBA{
		R: numeric[0],
		G: numeric[1],
		B: numeric[2],
		A: 255,
	}, nil
}
