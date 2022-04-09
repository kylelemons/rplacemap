package progress

import (
	"fmt"
	"io"
	"math"
	"strings"
	"sync"
)

type Bar struct {
	mu       sync.Mutex
	progress int64
	total    int64
}

func (b *Bar) AddProgress(amount int64) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.progress += int64(amount)
}

func (b *Bar) AddTotal(amount int64) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.total += int64(amount)
}

func (b *Bar) Progress() (progress, total int64) {
	b.mu.Lock()
	defer b.mu.Unlock()

	progress = b.progress
	total = b.total
	return
}

func (b *Bar) String() string {
	progress, total := b.Progress()
	if total == 0 {
		return "(unknown)"
	}

	percent := progress * 100 / total

	offset := 50 - (percent / 2)
	if offset < 0 {
		offset = 0
	}
	if offset > 50 {
		offset = 50
	}

	// This works well for non-size_t style progress:
	// width := int(math.Ceil(math.Log10(float64(total + 1))))
	// return fmt.Sprintf("%3d%% [%s] %*d/%d", percent, bar[offset:][:50], width, progress, total)

	div, unit := 1<<20, "MiB"
	if total > 1<<30 {
		div, unit = 1<<30, "GiB"
	}

	var (
		progressMiB = float64(progress) / float64(div)
		totalMiB    = float64(total) / float64(div)
	)

	width := int(math.Ceil(math.Log10(totalMiB))) + 3 // count ".00"
	return fmt.Sprintf("%3d%% [%s] %*.2f/%.2f %s",
		percent, bar[offset:][:50], width, progressMiB, totalMiB, unit)
}

func (b *Bar) Wrap(r io.Reader, size int64) io.Reader {
	b.AddTotal(size)
	return countingReader{b, r}
}

type countingReader struct {
	b *Bar
	r io.Reader
}

var _ io.Reader = countingReader{}

func (r countingReader) Read(b []byte) (n int, err error) {
	n, err = r.r.Read(b)
	r.b.AddProgress(int64(n))
	return
}

var bar = strings.Repeat("#", 50) + strings.Repeat(" ", 50)
