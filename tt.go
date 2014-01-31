package main

import (
	"bufio"
	"flag"
	"fmt"
	"github.com/jasonmoo/bloom"
	"github.com/jasonmoo/bloom/scalable"
	"log"
	"os"
	"runtime"
	"time"
)

var (
	intersection = flag.Bool("i", false, "calculate the intersection")
	diff         = flag.Bool("d", false, "calculate the difference")
	union        = flag.Bool("u", false, "calculate the union")

	hint = flag.Uint("hint", 4096, "min number of tokens per file")

	// buffered io
	stdout = bufio.NewWriterSize(os.Stdout, 4096)

	// total tokens in output
	total uint64
)

func main() {

	start := time.Now()

	defer func() {
		stdout.Flush()
		fmt.Fprintln(os.Stderr, "** Token Report **")
		fmt.Fprintln(os.Stderr, "Tokens output: ", total)
		fmt.Fprintln(os.Stderr, "Total time: ", time.Since(start))
	}()

	runtime.GOMAXPROCS(runtime.NumCPU())

	flag.Parse()

	file_paths := flag.Args()

	if *union {

		filter := scalable.New(*hint)

		for _, file_path := range file_paths {
			file, err := os.Open(file_path)
			if err != nil {
				log.Fatal(err)
			}
			scanner := bufio.NewScanner(file)
			for scanner.Scan() {
				token := scanner.Bytes()
				if !filter.Check(token) {
					stdout.Write(token)
					stdout.WriteByte('\n')
					total++
					filter.Add(token)
				}
			}
			file.Close()
		}

		return
	}

	// optimize muthafuckas
	if len(file_paths) == 2 {

		filter := scalable.New(*hint)

		filea, err := os.Open(file_paths[0])
		if err != nil {
			log.Fatal(err)
		}
		defer filea.Close()

		scanner := bufio.NewScanner(filea)
		for scanner.Scan() {
			filter.Add(scanner.Bytes())
		}
		if err := scanner.Err(); err != nil {
			log.Fatal(err)
		}

		fileb, err := os.Open(file_paths[1])
		if err != nil {
			log.Fatal(err)
		}
		defer fileb.Close()

		scanner = bufio.NewScanner(fileb)

		switch {
		case *intersection:
			for scanner.Scan() {
				token := scanner.Bytes()
				if !filter.Check(token) {
					stdout.Write(token)
					stdout.WriteByte('\n')
					total++
					filter.Add(token)
				}
			}
			if err := scanner.Err(); err != nil {
				log.Fatal(err)
			}
			return
		case *diff:
			for scanner.Scan() {
				token := scanner.Bytes()
				if !filter.Check(token) {
					stdout.Write(token)
					stdout.WriteByte('\n')
					total++
					filter.Add(token)
				}
			}
			if err := scanner.Err(); err != nil {
				log.Fatal(err)
			}
			return
		}
	}

	// multi file handling below
	filters := make([]bloom.Bloom, len(file_paths))
	filter_chan := make(chan bloom.Bloom, len(file_paths))

	// may require throttling due to disk thrashing
	// initial scan to fill the bloom filters
	for _, file_path := range file_paths {
		go func(path string) {
			filter := scalable.New(*hint)
			file, err := os.Open(path)
			if err != nil {
				log.Fatal(err)
			}
			scanner := bufio.NewScanner(file)
			for scanner.Scan() {
				filter.Add(scanner.Bytes())
			}
			file.Close()
			filter_chan <- filter
		}(file_path)
	}

	// fill the filters
	for i := range filters {
		filters[i] = <-filter_chan
	}

	// do the work
	switch {
	case *intersection:
		for _, file_path := range file_paths {
			file, err := os.Open(file_path)
			if err != nil {
				log.Fatal(err)
			}
			scanner := bufio.NewScanner(file)

		NEXT_TOKEN:
			for scanner.Scan() {
				token := scanner.Bytes()
				for _, filter := range filters {
					if !filter.Check(token) {
						goto NEXT_TOKEN
					}
				}
				stdout.Write(token)
				stdout.WriteByte('\n')
				total++
			}
			file.Close()
		}
	case *diff:
		for _, file_path := range file_paths {
			file, err := os.Open(file_path)
			if err != nil {
				log.Fatal(err)
			}
			scanner := bufio.NewScanner(file)

			for scanner.Scan() {
				token := scanner.Bytes()
				for _, filter := range filters {
					if !filter.Check(token) {
						stdout.Write(token)
						stdout.WriteByte('\n')
						total++
					}
				}
			}
			file.Close()
		}
	default:
		fmt.Println("Usage: tt -[i,d,u] file1 file2[ file3..]")
		flag.PrintDefaults()
		os.Exit(1)
	}

}
