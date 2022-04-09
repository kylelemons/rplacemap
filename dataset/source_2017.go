package dataset

import (
	"fmt"
	"image/color"
	"net/url"
	"strconv"
	"strings"
	"time"
)

func urls2017() []*url.URL {
	return []*url.URL{{
		Scheme: "https",
		Host:   "storage.googleapis.com",
		Path:   "/justin_bassett/place_tiles",
	}}
}

func parseLine2017(line string) ([]RawRecord, error) {
	fields := strings.Split(line, ",")
	if got, want := len(fields), 5; got != want {
		return nil, fmt.Errorf("columns = %v, want %v: line %q", got, want, line)
	}
	var (
		tsStr      = fields[0]
		userHash   = fields[1]
		xStr, yStr = fields[2], fields[3]
		colorStr   = fields[4]
	)
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
	colorIndex, err := strconv.ParseUint(colorStr, 10, 8)
	if err != nil {
		return nil, fmt.Errorf("color %q invalid: %s", colorStr, err)
	}

	return []RawRecord{{
		Timestamp: ts,
		UserHash:  userHash,
		X:         x,
		Y:         y,
		Color:     palette2017[colorIndex],
	}}, nil
}

const header2017 = "ts,user_hash,x_coordinate,y_coordinate,color"

var palette2017 = []color.RGBA{
	0:  {R: 0xFF, G: 0xFF, B: 0xFF, A: 0xFF},
	1:  {R: 0xE4, G: 0xE4, B: 0xE4, A: 0xFF},
	2:  {R: 0x88, G: 0x88, B: 0x88, A: 0xFF},
	3:  {R: 0x22, G: 0x22, B: 0x22, A: 0xFF},
	4:  {R: 0xFF, G: 0xA7, B: 0xD1, A: 0xFF},
	5:  {R: 0xE5, G: 0x00, B: 0x00, A: 0xFF},
	6:  {R: 0xE5, G: 0x95, B: 0x00, A: 0xFF},
	7:  {R: 0xA0, G: 0x6A, B: 0x42, A: 0xFF},
	8:  {R: 0xE5, G: 0xD9, B: 0x00, A: 0xFF},
	9:  {R: 0x94, G: 0xE0, B: 0x44, A: 0xFF},
	10: {R: 0x02, G: 0xBE, B: 0x01, A: 0xFF},
	11: {R: 0x00, G: 0xE5, B: 0xF0, A: 0xFF},
	12: {R: 0x00, G: 0x83, B: 0xC7, A: 0xFF},
	13: {R: 0x00, G: 0x00, B: 0xEA, A: 0xFF},
	14: {R: 0xE0, G: 0x4A, B: 0xFF, A: 0xFF},
	15: {R: 0x82, G: 0x00, B: 0x80, A: 0xFF},
}
