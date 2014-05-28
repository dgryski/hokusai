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

	// FIXME(dgryski): rename these to be the same as the paper?
	itemAggregate     []*probably.Sketch // A sketch
	timeAggregate     []*probably.Sketch // M sketch
	itemtimeAggregate []*probably.Sketch // B sketch
}

const defaultSize = 18 // from the paper

func newSketch(size uint) *probably.Sketch {
	return probably.NewSketch(1<<size, 4)
}

func NewHokusai(epoch0 int64, windowSize int64) *Hokusai {
	return &Hokusai{
		epoch0:     epoch0,
		endEpoch:   epoch0 + windowSize,
		sk:         newSketch(defaultSize),
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
		l := h.itemAggregate[ln-1<<uint(k)]
		l.Compress()
	}
	h.itemAggregate = append(h.itemAggregate, h.sk.Clone())

	// Algorithm 2 -- Time Aggregation
	for h.timeUnits%(1<<uint(l)) == 0 {
		l++
	}

	m := h.sk.Clone()
	for j := 0; j < l; j++ {
		t := m.Clone()
		if len(h.timeAggregate) <= j {
			h.timeAggregate = append(h.timeAggregate, newSketch(defaultSize))
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
				n := newSketch(defaultSize - uint(j) - 1)
				h.itemtimeAggregate = append(h.itemtimeAggregate, n)
			}
			bj := h.itemtimeAggregate[j]
			ssk.Merge(bj)
			h.itemtimeAggregate[j] = t
		}
	}

	// reset current sketch
	h.sk = newSketch(defaultSize)
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

	if float64(minA) > (math.E*float64(t))/float64(int(1<<(defaultSize-uint(t)-1))) {
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
