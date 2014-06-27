package main

// FIXME(dgryski): this is unbounded at the moment -- limit it to 1<<intervals (active) elements

import (
	"sync"

	"github.com/dgryski/go-topk"
)

type topkstream struct {
	streams []*topk.Stream
	sync.Mutex
}

var TopKs topkstream

func (tk *topkstream) Insert(x string, count int) {
	tk.Lock()
	defer tk.Unlock()
	tk.streams[len(tk.streams)-1].Insert(x, count)
}

func (tk *topkstream) Keys(t int) []topk.Element {
	tk.Lock()
	defer tk.Unlock()
	return tk.streams[t].Keys()
}

func (tk *topkstream) Tick(size int) {
	tk.Lock()
	defer tk.Unlock()
	tk.streams = append(tk.streams, topk.New(size))
}

func (tk *topkstream) Len() int {
	tk.Lock()
	defer tk.Unlock()
	return len(tk.streams)
}
