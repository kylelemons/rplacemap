package details

import (
	"encoding/json"
	"fmt"
	"image/color"
	"net/http"

	"github.com/kylelemons/rplacemap/v2/dataset"
	"github.com/kylelemons/rplacemap/v2/internal/gsync"
)

func PixelEvents(futureDataset *gsync.Future[*dataset.Dataset]) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ds, err := futureDataset.Wait(r.Context())
		if err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}

		var x, y int
		fmt.Sscan(r.FormValue("x"), &x)
		fmt.Sscan(r.FormValue("y"), &y)
		if x < 0 || y < 0 || x >= ds.Size || y >= ds.Size {
			http.NotFound(w, r)
			return
		}
		type eventJSON struct {
			Timestamp string
			X, Y      int
			UserID    string
			Color     string
		}
		var events []eventJSON
		for _, ev := range ds.At(y, x) {
			c := ds.Palette[ev.ColorIndex].(color.RGBA)
			events = append(events, eventJSON{
				Timestamp: ds.TimeAfter(ev.DeltaMillis).Format(dataset.TimestampLayout),
				X:         x,
				Y:         y,
				UserID:    ds.UserIDs[ev.UserIndex],
				Color:     fmt.Sprintf("#%02X%02X%02X", c.R, c.G, c.B),
			})
		}
		writeJSON(w, events)
	}
}

func writeJSON(w http.ResponseWriter, obj interface{}) {
	w.Header().Set("Content-Type", "application/json;charset=UTF-8")
	json.NewEncoder(w).Encode(obj)
}
