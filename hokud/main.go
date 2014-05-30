package main

// TODO: topk
// TODO: batch API
// TODO: configure size of sketches (width/depth/window)
// TODO: expvar, net/http/pprof
// TODO: graphite?
// TODO: logging
// TODO: persist/reload hokusai data
// TODO: support multiple (dynamic?) named sketches: /create?name=stream1&...

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/dgryski/hokusai/sketch"
)

// FIXME: protect with a mutex
var Hoku *sketch.Hokusai

var Epoch0 int64

var WindowSize int64 = 60

func defaultInt(s string, defval int) (int, error) {
	if s == "" {
		return defval, nil
	}

	sint, err := strconv.Atoi(s)
	if err != nil {
		return 0, err
	}

	return sint, nil
}

func addHandler(w http.ResponseWriter, r *http.Request) {

	item := r.FormValue("item")

	count, err := defaultInt(r.FormValue("count"), 1)
	if err != nil {
		http.Error(w, "bad count", http.StatusBadRequest)
		return
	}

	targ, err := defaultInt(r.FormValue("t"), 0)
	if err != nil {
		http.Error(w, "bad epoch", http.StatusBadRequest)
		return
	}

	var epoch int64
	if targ == 0 {
		epoch = time.Now().UnixNano() / int64(time.Second)
	} else {
		epoch = int64(targ)
	}

	Hoku.Add(epoch, item, uint32(count))

	// TODO: more interesting response?
	fmt.Fprintln(w, "Ok")
}

type QueryResponse struct {
	Query  string
	Start  int
	Step   int64
	Counts []uint32
}

func queryHandler(w http.ResponseWriter, r *http.Request) {

	q := r.FormValue("q")

	start, err := defaultInt(r.FormValue("start"), 0)
	if start == 0 || int64(start) < Epoch0 || err != nil {
		http.Error(w, "bad start epoch", http.StatusBadRequest)
		return
	}

	stop, err := defaultInt(r.FormValue("stop"), 0)
	if stop == 0 || int64(stop) < Epoch0 || err != nil {
		http.Error(w, "bad stop epoch", http.StatusBadRequest)
		return
	}

	queryResponse := QueryResponse{
		Query: q,
		Start: start,
		Step:  WindowSize,
	}

	for t := start; t <= stop; t += int(WindowSize) {
		queryResponse.Counts = append(queryResponse.Counts, Hoku.Count(int64(t), q))
	}

	w.Header().Set("Content-Type", "application/json")
	jenc := json.NewEncoder(w)
	jenc.Encode(queryResponse)
}

func main() {

	epoch0 := flag.Int("epoch", 0, "start epoch (from file)")
	file := flag.String("f", "", "load data from file (instead of http)")
	port := flag.Int("p", 8080, "http port")
	width := flag.Int("w", 22, "default sketch width")
	depth := flag.Int("d", 5, "default sketch depth")
	win := flag.Int("win", 60, "default window size")
	intv := flag.Int("intv", 11, "intervals to keep")

	flag.Parse()

	WindowSize = int64(*win)

	if *file != "" {
		loadDataFrom(*file, int64(*epoch0), uint(*intv), *width, *depth)
	} else {

		now := time.Now().UnixNano() / int64(time.Second)
		Epoch0 = now - (now % int64(WindowSize))

		Hoku = sketch.NewHokusai(Epoch0, int64(WindowSize), uint(*intv), *width, *depth)
		go func() {
			for {
				time.Sleep(time.Second * time.Duration(WindowSize))
				t := time.Now().UnixNano() / int64(time.Second)
				Hoku.Add(t, "", 0)
			}
		}()
	}

	http.HandleFunc("/add", addHandler)
	http.HandleFunc("/query", queryHandler)

	log.Fatal(http.ListenAndServe(":"+strconv.Itoa(*port), nil))

}

func loadDataFrom(file string, epoch0 int64, intervals uint, width, depth int) {

	f, err := os.Open(file)
	if err != nil {
		log.Fatal(err)
	}

	scanner := bufio.NewScanner(f)

	Hoku = sketch.NewHokusai(epoch0, int64(WindowSize), intervals, width, depth)

	var maxEpoch int

	var lines int

	for scanner.Scan() {
		line := scanner.Text()
		lines++
		fields := strings.Split(line, "\t")

		t, err := strconv.Atoi(fields[0])
		if err != nil {
			log.Println("skipping ", fields[0])
			continue
		}

		if t > maxEpoch {
			maxEpoch = t
		}

		if lines%(1<<20) == 0 {
			log.Println("processed", lines)
		}

		var count uint32 = 1

		if len(fields) == 3 {
			cint, err := strconv.Atoi(fields[2])
			if err != nil {
				log.Println("failed to parse count: ", fields[2], ":", err)
				continue
			}
			count = uint32(cint)
		}

		Hoku.Add(int64(t), fields[1], count)
	}
}
