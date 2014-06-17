package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"log"
	"os"
	"regexp"
	"time"

	"github.com/reducedb/bloom"
	"github.com/reducedb/bloom/scalable"
)

type (
	Bloomer struct {
		bloom.Bloom
		filters []bloom.Bloom
	}
	Emitter struct {
		file    *os.File
		scanner *bufio.Scanner

		regex_match   *regexp.Regexp
		regex_capture *regexp.Regexp
	}
)

func NewEmitter(file_path, regex_match, regex_capture string) (*Emitter, error) {

	var (
		e   = new(Emitter)
		err error
	)

	e.file, err = os.Open(file_path)
	if err != nil {
		return nil, err
	}

	e.scanner = bufio.NewScanner(e.file)

	if regex_match != "" {
		e.regex_match = regexp.MustCompile(regex_match)
	}
	if regex_capture != "" {
		e.regex_capture = regexp.MustCompile(regex_capture)
	}

	return e, nil

}
func (e *Emitter) Scan() bool {
	return e.scanner.Scan()
}
func (e *Emitter) Bytes() []byte {
	data := e.scanner.Bytes()
	if e.regex_match != nil && !e.regex_match.Match(data) {
		return []byte{}
	}
	if e.regex_capture != nil {
		matches := e.regex_capture.FindSubmatch(data)
		return bytes.Join(matches, []byte{','})
	}
	return data
}
func (e *Emitter) Text() string {
	return string(e.Bytes())
}
func (e *Emitter) Close() error {
	return e.file.Close()
}

var (
	intersection = flag.Bool("i", false, "calculate the intersection")
	diff         = flag.Bool("d", false, "calculate the difference")
	union        = flag.Bool("u", false, "calculate the union")

	// activate lossy processing
	blooms = flag.Uint("blooms", 0, "number of bloom filters to use (lossy)")

	// options
	trim          = flag.Bool("trim", false, "trim each line")
	regex_match   = flag.String("regex", "", "only process matching lines")
	regex_capture = flag.String("regex_capture", "", "only process captured data")

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

	flag.Parse()

	if !*intersection && !*diff && !*union {
		fmt.Println("Usage: tt -[i,d,u] [-blooms N] file1 file2[ file3..]")
		flag.PrintDefaults()
		os.Exit(1)
	}

	file_paths := flag.Args()

	switch {

	// activate bloom filter solution
	case *blooms > 0:

		if *union {

			unique_set := NewScalableBloom(*blooms)

			for _, file_path := range file_paths {

				e, err := NewEmitter(file_path, *regex_match, *regex_capture)
				if err != nil {
					log.Fatal(err)
				}

				for e.Scan() {
					token := e.Bytes()
					if !unique_set.Check(token) {
						stdout.Write(token)
						stdout.WriteByte('\n')
						total++
						unique_set.Add(token)
					}
				}

				e.Close()

			}

			return
		}

		// multi file handling below
		sets := make([]bloom.Bloom, len(file_paths))

		// may require throttling due to disk thrashing
		// initial scan to fill the bloom filters
		for i, file_path := range file_paths {

			set := NewScalableBloom(*blooms)

			e, err := NewEmitter(file_path, *regex_match, *regex_capture)
			if err != nil {
				log.Fatal(err)
			}

			for e.Scan() {
				set.Add(e.Bytes())
			}

			e.Close()

			sets[i] = set

		}

		// do the work
		switch {

		// unique set of tokens that exist in all files
		case *intersection:

			echoed_set := NewScalableBloom(*blooms)

			for _, file_path := range file_paths {

				e, err := NewEmitter(file_path, *regex_match, *regex_capture)
				if err != nil {
					log.Fatal(err)
				}

			NEXT_TOKEN:
				for e.Scan() {

					token := e.Bytes()

					if echoed_set.Check(token) {
						goto NEXT_TOKEN
					}

					for _, set := range sets {
						if !set.Check(token) {
							goto NEXT_TOKEN
						}
					}

					stdout.Write(token)
					stdout.WriteByte('\n')
					total++
					echoed_set.Add(token)

				}

				e.Close()

			}

		// unique set of tokens not in the intersection
		case *diff:

			echoed_set := NewScalableBloom(*blooms)

			for _, file_path := range file_paths {

				e, err := NewEmitter(file_path, *regex_match, *regex_capture)
				if err != nil {
					log.Fatal(err)
				}

				for e.Scan() {

					token := e.Bytes()

					if echoed_set.Check(token) {
						continue
					}

					for _, set := range sets {
						if !set.Check(token) {
							stdout.Write(token)
							stdout.WriteByte('\n')
							total++
							echoed_set.Add(token)
						}
					}

				}

				e.Close()

			}
		}

	// defaults to map solution
	default:

		if *union {

			unique_set := make(map[string]bool)

			for _, file_path := range file_paths {

				e, err := NewEmitter(file_path, *regex_match, *regex_capture)
				if err != nil {
					log.Fatal(err)
				}

				for e.Scan() {
					token := e.Text()
					if _, exists := unique_set[token]; !exists {
						stdout.WriteString(token)
						stdout.WriteByte('\n')
						total++
						unique_set[token] = true
					}
				}

				e.Close()

			}

			return
		}

		// multi file handling below
		sets := make([]map[string]bool, len(file_paths))

		// may require throttling due to disk thrashing
		// initial scan to fill the bloom filters
		for i, file_path := range file_paths {

			set := make(map[string]bool)

			e, err := NewEmitter(file_path, *regex_match, *regex_capture)
			if err != nil {
				log.Fatal(err)
			}

			for e.Scan() {
				set[e.Text()] = true
			}

			e.Close()

			sets[i] = set

		}

		// do the work
		switch {

		// unique set of tokens that exist in all files
		case *intersection:

			echoed_set := make(map[string]bool)

			for _, file_path := range file_paths {

				e, err := NewEmitter(file_path, *regex_match, *regex_capture)
				if err != nil {
					log.Fatal(err)
				}

			NEXT_TOKEN2:
				for e.Scan() {

					token := e.Text()

					if _, echoed := echoed_set[token]; echoed {
						goto NEXT_TOKEN2
					}

					for _, set := range sets {
						if _, in_this_set := set[token]; !in_this_set {
							goto NEXT_TOKEN2
						}
					}

					stdout.WriteString(token)
					stdout.WriteByte('\n')
					total++
					echoed_set[token] = true

				}

				e.Close()

			}

		// unique set of tokens not in the intersection
		case *diff:

			echoed_set := make(map[string]bool)

			for _, file_path := range file_paths {

				e, err := NewEmitter(file_path, *regex_match, *regex_capture)
				if err != nil {
					log.Fatal(err)
				}

				for e.Scan() {

					token := e.Text()

					if _, echoed := echoed_set[token]; echoed {
						continue
					}

					for _, set := range sets {
						if _, in_this_set := set[token]; !in_this_set {
							stdout.WriteString(token)
							stdout.WriteByte('\n')
							total++
							echoed_set[token] = true
							break
						}
					}

				}

				e.Close()

			}
		}

	}

}

func NewScalableBloom(size uint) bloom.Bloom {

	filters := make([]bloom.Bloom, size)

	for i, _ := range filters {
		filter := scalable.New(4096)
		// filter.SetHasher(adler32.New())
		filter.Reset()
		filters[i] = filter
	}

	return &Bloomer{
		filters: filters,
	}

}

func (b *Bloomer) Add(token []byte) bloom.Bloom {

	if *blooms > 1 {
		token = append(make([]byte, len(token)), token...)
	}

	for _, filter := range b.filters {
		filter.Add(token)
		if *blooms > 1 {
			mash(token)
		}
	}

	return b
}

func (b *Bloomer) Check(token []byte) bool {
	if *blooms > 1 {
		token = append(make([]byte, len(token)), token...)
	}
	for _, filter := range b.filters {
		if !filter.Check(token) {
			return false
		}
		if *blooms > 1 {
			mash(token)
		}
	}
	return true
}

// modifies the underlying structure
func mash(token []byte) {
	for i, c := range token {
		token[i] ^= (20 * c)
	}
}
