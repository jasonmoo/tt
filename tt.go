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
)

var (
	intersection = flag.Bool("i", false, "calculate the intersection")
	diff         = flag.Bool("d", false, "calculate the difference")
	union        = flag.Bool("u", false, "calculate the union")

	unique = flag.Bool("unique", false, "output the unique set of the values")

	// global unique filter
	ufilter = scalable.New(1 << 20)
)

func main() {

	runtime.GOMAXPROCS(runtime.NumCPU())

	flag.Parse()

	file_paths := flag.Args()

	if *union {
		// init to 1mm per file, will grow to whatever
		filter := scalable.New(uint(len(file_paths) << 20))

		for _, file_path := range file_paths {
			file, err := os.Open(file_path)
			if err != nil {
				log.Fatal(err)
			}
			scanner := bufio.NewScanner(file)
			for scanner.Scan() {
				token := scanner.Bytes()
				if !filter.Check(token) {
					os.Stdout.Write(token)
					os.Stdout.Write([]byte{'\n'})
				}
			}
			file.Close()
		}

		return
	}

	// optimize muthafuckas
	if len(file_paths) == 2 {

		filter := scalable.New(1 << 20)

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
				if token := scanner.Bytes(); filter.Check(token) {
					if *unique {
						if ufilter.Check(token) {
							continue
						}
						ufilter.Add(token)
					}
					os.Stdout.Write(token)
					os.Stdout.Write([]byte{'\n'})
				}
			}
			if err := scanner.Err(); err != nil {
				log.Fatal(err)
			}
		case *diff:
			for scanner.Scan() {
				if token := scanner.Bytes(); !filter.Check(token) {
					if *unique {
						if ufilter.Check(token) {
							continue
						}
						ufilter.Add(token)
					}
					os.Stdout.Write(token)
					os.Stdout.Write([]byte{'\n'})
				}
			}
			if err := scanner.Err(); err != nil {
				log.Fatal(err)
			}
		}
	}

	// multi file handling below
	filters, filter_chan := make([]bloom.Bloom, len(file_paths)), make(chan bloom.Bloom, len(file_paths))

	// may require throttling due to disk thrashing
	for _, file_path := range file_paths {
		go func() {
			filter := scalable.New(1 << 20)
			file, err := os.Open(file_path)
			if err != nil {
				log.Fatal(err)
			}
			defer file.Close()
			scanner := bufio.NewScanner(file)
			for scanner.Scan() {
				filter.Add(scanner.Bytes())
			}
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
				os.Stdout.Write(token)
				os.Stdout.Write([]byte{'\n'})
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
						os.Stdout.Write(token)
						os.Stdout.Write([]byte{'\n'})
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

}
