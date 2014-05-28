package sketch

import (
	"math"

	"github.com/dustin/go-probably"
)

type Hokusai struct {
	sk *probably.Sketch

	epoch0     int64
	endEpoch   int64
	windowSize int64
	timeUnits  int

	width int
	depth int

	// FIXME(dgryski): rename these to be the same as the paper?
	itemAggregate     []*probably.Sketch // A sketch
	timeAggregate     []*probably.Sketch // M sketch
	itemtimeAggregate []*probably.Sketch // B sketch
}

// The paper used 2**23 bins and 4 hash functions (section 5.1)
const DefaultWidth = 23
const DefaultDepth = 4

func newSketch(width, depth int) *probably.Sketch {
	return probably.NewSketch(1<<uint(width), depth)
}

func NewHokusai(epoch0 int64, windowSize int64, width, depth int) *Hokusai {
	return &Hokusai{
		width:      width,
		depth:      depth,
		epoch0:     epoch0,
		endEpoch:   epoch0 + windowSize,
		sk:         newSketch(width, depth),
		windowSize: windowSize,
	}
}

func (h *Hokusai) Add(epoch int64, s string, count uint32) {

	if epoch < h.endEpoch {
		// still in the current window
		h.sk.Add(s, count)
		return
	}

	h.timeUnits++
	h.endEpoch += h.windowSize

	// Algorithm 3 -- Item Aggregation
	ln := len(h.itemAggregate)
	l := ilog2(h.timeUnits - 1)
	for k := 1; k < l; k++ {
		// itemAggregation[t] is the data array for time t
		sk := h.itemAggregate[ln-1<<uint(k)]
		sk.Compress()
	}
	h.itemAggregate = append(h.itemAggregate, h.sk.Clone())

	// Algorithm 2 -- Time Aggregation
	l = 0
	for h.timeUnits%(1<<uint(l)) == 0 {
		l++
	}

	m := h.sk.Clone()
	for j := 0; j < l; j++ {
		t := m.Clone()
		if len(h.timeAggregate) <= j {
			h.timeAggregate = append(h.timeAggregate, newSketch(h.width, h.depth))
		}
		mj := h.timeAggregate[j]

		m.Merge(mj)
		h.timeAggregate[j] = t
	}

	// Algorithm 4 -- Item and Time Aggregation
	if h.timeUnits >= 2 {
		ssk := h.timeAggregate[0].Clone()
		for j := 0; j < l; j++ {
			ssk.Compress()
			t := ssk.Clone()

			if len(h.itemtimeAggregate) <= j {
				n := newSketch(h.width-j-1, h.depth)
				h.itemtimeAggregate = append(h.itemtimeAggregate, n)
			}
			bj := h.itemtimeAggregate[j]
			ssk.Merge(bj)
			h.itemtimeAggregate[j] = t
		}
	}

	// reset current sketch
	h.sk = newSketch(h.width, h.depth)
	h.sk.Increment(s)
}

func (h *Hokusai) Count(epoch int64, s string) uint32 {

	// t is our unit time index
	t := int((epoch - h.epoch0) / h.windowSize)

	// current window?
	if t == h.timeUnits {
		return h.sk.Count(s)
	}

	// Algorithm 5

	Avals := h.itemAggregate[t].Values(s)
	minA := Avals[0]
	for _, v := range Avals[1:] {
		if v < minA {
			minA = v
		}
	}

	if float64(minA) > (math.E*float64(t))/float64(int(1<<uint(h.width-t-1))) {
		// heavy hitter
		return minA
	}

	jstar := ilog2(h.timeUnits-t) - 1
	Mvals := h.timeAggregate[jstar].Values(s)
	Bvals := h.itemtimeAggregate[jstar].Values(s)

	var nxt uint32 = math.MaxUint32

	for i := 0; i < len(Avals); i++ {
		if Bvals[i] == 0 {
			nxt = 0
		} else if n := (Mvals[i] * Avals[i]) / Bvals[i]; n < nxt {
			nxt = n
		}
	}

	return nxt
}

func ilog2(v int) int {

	r := 0

	for ; v != 0; v >>= 1 {
		r++
	}

	return r
}
