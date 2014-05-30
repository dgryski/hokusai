package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/dgryski/hokusai/sketch"
)

func main() {

	file := flag.String("f", "", "input file")
	queries := flag.String("q", "", "query file")
	epoch0 := flag.Int("epoch", 0, "epoch0")
	windowSize := flag.Int("win", 1, "window size")
	intervals := flag.Int("intv", 11, "intervals")
	flag.Parse()

	f, err := os.Open(*file)
	if err != nil {
		log.Fatal(err)
	}

	scanner := bufio.NewScanner(f)

	h := sketch.NewHokusai(int64(*epoch0), int64(*windowSize), uint(*intervals), 20, 5)

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

		h.Add(int64(t), fields[1], count)
	}

	qf, err := os.Open(*queries)
	if err != nil {
		log.Fatal(err)
	}

	scanner = bufio.NewScanner(qf)

	for scanner.Scan() {
		q := scanner.Text()

		for t := *epoch0; t <= maxEpoch; t += *windowSize {
			fmt.Println("#", t, q, h.Count(int64(t), q))
		}
	}
}
