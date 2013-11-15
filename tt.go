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

	hint   = flag.Uint("hint", 4096, "min number of tokens per file")
	unique = flag.Bool("unique", false, "output the unique set of the values")

	// global unique filter
	ufilter = scalable.New(*hint)

	// buffered io
	stdout = bufio.NewWriterSize(os.Stdout, 4096)

	// total tokens in output
	total uint64
)

func main() {

	defer stdout.Flush()

	runtime.GOMAXPROCS(runtime.NumCPU())

	flag.Parse()

	file_paths, start := flag.Args(), time.Now()

	if *union {
		for _, file_path := range file_paths {
			file, err := os.Open(file_path)
			if err != nil {
				log.Fatal(err)
			}
			scanner := bufio.NewScanner(file)
			for scanner.Scan() {
				token := scanner.Bytes()
				if !ufilter.Check(token) {
					stdout.Write(token)
					stdout.WriteByte('\n')
					total++
					ufilter.Add(token)
				}
			}
			file.Close()
		}

		return
	}

	// optimize muthafuckas
	if *unique && len(file_paths) == 2 {

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
				if filter.Check(token) && !ufilter.Check(token) {
					stdout.Write(token)
					stdout.WriteByte('\n')
					total++
					ufilter.Add(token)
				}
			}
			if err := scanner.Err(); err != nil {
				log.Fatal(err)
			}
			return
		case *diff:
			for scanner.Scan() {
				token := scanner.Bytes()
				if !filter.Check(token) && !ufilter.Check(token) {
					stdout.Write(token)
					stdout.WriteByte('\n')
					total++
					ufilter.Add(token)
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
	for _, file_path := range file_paths {
		go func() {
			filter := scalable.New(*hint)
			file, err := os.Open(file_path)
			if err != nil {
				log.Fatal(err)
			}
			scanner := bufio.NewScanner(file)
			for scanner.Scan() {
				filter.Add(scanner.Bytes())
			}
			file.Close()
			filter_chan <- filter
		}()
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
				if *unique {
					if ufilter.Check(token) {
						continue
					}
					ufilter.Add(token)
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
						if *unique {
							if ufilter.Check(token) {
								continue
							}
							ufilter.Add(token)
						}
						stdout.Write(token)
						stdout.WriteByte('\n')
						total++
					}
				}
			}
			file.Close()
		}
	default:
		fmt.Println("Usage: tt -[i,d,u] [-unique] file1 file2[ file3..]")
		flag.PrintDefaults()
		os.Exit(1)
	}

	fmt.Fprintln(os.Stderr, "** Token Report **")
	fmt.Fprintln(os.Stderr, "Tokens output: ", total)
	fmt.Fprintln(os.Stderr, "Total time: ", time.Since(start))

}
