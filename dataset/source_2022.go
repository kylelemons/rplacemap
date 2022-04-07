package dataset

import (
	"fmt"
	"net/url"
)

func urls2022() []*url.URL {
	urls := make([]*url.URL, 78)
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
	return nil, fmt.Errorf("unimplemented")
}

const header2022 = ""
