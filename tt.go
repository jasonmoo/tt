package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"regexp"
	"time"

	"github.com/reducedb/bloom"
	"github.com/reducedb/bloom/scalable"
)

type (
	Emitter struct {
		file    *os.File
		scanner *bufio.Scanner

		regex_match   *regexp.Regexp
		regex_capture *regexp.Regexp

		current []byte
	}
)

var (
	intersection = flag.Bool("i", false, "calculate the intersection")
	diff         = flag.Bool("d", false, "calculate the difference")
	union        = flag.Bool("u", false, "calculate the union")

	// activate lossy processing
	estimated_lines = flag.Uint64("estimated_lines", 0, "use bloom filters above this number of lines to scan")
	bloom_threshold = flag.String("bloom_threshold", "1M", "use bloom filters above this number of lines to scan")

	// options
	trim          = flag.Bool("trim", false, "trim each line")
	regex_match   = flag.String("match", "", "only process matching lines")
	regex_capture = flag.String("capture", "", "only process captured data")

	buffer_size = flag.Int("buffer_size", 1<<20, "buffered io chunk size")

	// totals
	total_tokens_emitted  uint64
	total_bytes_processed uint64
	total_lines_scanned   uint64
)

func main() {

	start := time.Now()

	flag.Parse()

	// buffered io
	stdout := bufio.NewWriterSize(os.Stdout, *buffer_size)

	defer func() {
		stdout.Flush()
		fmt.Fprintln(os.Stderr, "** Token Report **")
		fmt.Fprintln(os.Stderr, "Lines scanned: ", total_lines_scanned)
		fmt.Fprintln(os.Stderr, "Tokens emitted: ", total_tokens_emitted)
		fmt.Fprintln(os.Stderr, "Time: ", time.Since(start))
	}()

	if !*intersection && !*diff && !*union {
		fmt.Println("Usage: tt -[i,d,u] [-match \"pattern\"] -[capture \"pattern\"] file1 file2[ file3..]")
		flag.PrintDefaults()
		os.Exit(1)
	}

	file_paths := flag.Args()

	fmt.Fprintln(os.Stderr, "tt starting up")

	// if no estimate supplied, count lines
	if *estimated_lines == 0 {

		for _, file_path := range file_paths {

			file, err := os.Open(file_path)
			if err != nil {
				log.Fatal(err)
			}

			var lines uint64

			buf := make([]byte, *buffer_size)

			for {
				n, err := file.Read(buf)
				if err != nil && err != io.EOF {
					log.Fatal(err)
				}
				if n == 0 {
					break
				}
				for _, c := range buf[:n] {
					if c == '\n' {
						lines++
					}
				}
			}

			*estimated_lines += lines

			file.Close()
		}

	}

	// count number of bytes to process
	for _, file_path := range file_paths {

		info, err := os.Stat(file_path)
		if err != nil {
			log.Fatal(err)
		}

		total_bytes_processed += uint64(info.Size())

	}

	fmt.Fprintln(os.Stderr, "Bytes to process: ", total_bytes_processed)
	fmt.Fprintln(os.Stderr, "Lines to process: ", *estimated_lines)

	os.Exit(0)

	// if *estimated_lines > *bloom_threshold {

	// 	// double the lines processed
	// 	bloom_size := *estimated_lines * 2

	// 	if *union {

	// 		unique_set := NewScalableBloom(*bloom_size)

	// 		for _, file_path := range file_paths {

	// 			e, err := NewEmitter(file_path, *regex_match, *regex_capture)
	// 			if err != nil {
	// 				log.Fatal(err)
	// 			}

	// 			for e.Scan() {
	// 				token := e.Bytes()
	// 				if !unique_set.Check(token) {
	// 					stdout.Write(token)
	// 					stdout.WriteByte('\n')
	// 					unique_set.Add(token)
	// 				}
	// 			}

	// 			e.Close()

	// 		}

	// 		return
	// 	}

	// 	// multi file handling below
	// 	sets := make([]bloom.Bloom, len(file_paths))

	// 	// may require throttling due to disk thrashing
	// 	// initial scan to fill the bloom filters
	// 	for i, file_path := range file_paths {

	// 		set := NewScalableBloom(*use_bloom)

	// 		e, err := NewEmitter(file_path, *regex_match, *regex_capture)
	// 		if err != nil {
	// 			log.Fatal(err)
	// 		}

	// 		for e.Scan() {
	// 			set.Add(e.Bytes())
	// 		}

	// 		e.Close()

	// 		sets[i] = set

	// 	}

	// 	// do the work
	// 	switch {

	// 	// unique set of tokens that exist in all files
	// 	case *intersection:

	// 		echoed_set := NewScalableBloom(*use_bloom)

	// 		for _, file_path := range file_paths {

	// 			e, err := NewEmitter(file_path, *regex_match, *regex_capture)
	// 			if err != nil {
	// 				log.Fatal(err)
	// 			}

	// 		NEXT_TOKEN:
	// 			for e.Scan() {

	// 				token := e.Bytes()

	// 				if echoed_set.Check(token) {
	// 					goto NEXT_TOKEN
	// 				}

	// 				for _, set := range sets {
	// 					if !set.Check(token) {
	// 						goto NEXT_TOKEN
	// 					}
	// 				}

	// 				stdout.Write(token)
	// 				stdout.WriteByte('\n')
	// 				echoed_set.Add(token)

	// 			}

	// 			e.Close()

	// 		}

	// 	// unique set of tokens not in the intersection
	// 	case *diff:

	// 		echoed_set := NewScalableBloom(*use_bloom)

	// 		for _, file_path := range file_paths {

	// 			e, err := NewEmitter(file_path, *regex_match, *regex_capture)
	// 			if err != nil {
	// 				log.Fatal(err)
	// 			}

	// 			for e.Scan() {

	// 				token := e.Bytes()

	// 				if echoed_set.Check(token) {
	// 					continue
	// 				}

	// 				for _, set := range sets {
	// 					if !set.Check(token) {
	// 						stdout.Write(token)
	// 						stdout.WriteByte('\n')
	// 						echoed_set.Add(token)
	// 					}
	// 				}

	// 			}

	// 			e.Close()

	// 		}
	// 	}

	// 	// defaults to map solution
	// } else {

	// 	if *union {

	// 		unique_set := make(map[string]bool)

	// 		for _, file_path := range file_paths {

	// 			e, err := NewEmitter(file_path, *regex_match, *regex_capture)
	// 			if err != nil {
	// 				log.Fatal(err)
	// 			}

	// 			for e.Scan() {
	// 				token := e.Text()
	// 				if _, exists := unique_set[token]; !exists {
	// 					stdout.WriteString(token)
	// 					stdout.WriteByte('\n')

	// 					unique_set[token] = true
	// 				}
	// 			}

	// 			e.Close()

	// 		}

	// 		return
	// 	}

	// 	// multi file handling below
	// 	sets := make([]map[string]bool, len(file_paths))

	// 	// may require throttling due to disk thrashing
	// 	// initial scan to fill the bloom filters
	// 	for i, file_path := range file_paths {

	// 		set := make(map[string]bool)

	// 		e, err := NewEmitter(file_path, *regex_match, *regex_capture)
	// 		if err != nil {
	// 			log.Fatal(err)
	// 		}

	// 		for e.Scan() {
	// 			set[e.Text()] = true
	// 		}

	// 		e.Close()

	// 		sets[i] = set

	// 	}

	// 	// do the work
	// 	switch {

	// 	// unique set of tokens that exist in all files
	// 	case *intersection:

	// 		echoed_set := make(map[string]bool)

	// 		for _, file_path := range file_paths {

	// 			e, err := NewEmitter(file_path, *regex_match, *regex_capture)
	// 			if err != nil {
	// 				log.Fatal(err)
	// 			}

	// 		NEXT_TOKEN2:
	// 			for e.Scan() {

	// 				token := e.Text()

	// 				if _, echoed := echoed_set[token]; echoed {
	// 					goto NEXT_TOKEN2
	// 				}

	// 				for _, set := range sets {
	// 					if _, in_this_set := set[token]; !in_this_set {
	// 						goto NEXT_TOKEN2
	// 					}
	// 				}

	// 				stdout.WriteString(token)
	// 				stdout.WriteByte('\n')

	// 				echoed_set[token] = true

	// 			}

	// 			e.Close()

	// 		}

	// 	// unique set of tokens not in the intersection
	// 	case *diff:

	// 		echoed_set := make(map[string]bool)

	// 		for _, file_path := range file_paths {

	// 			e, err := NewEmitter(file_path, *regex_match, *regex_capture)
	// 			if err != nil {
	// 				log.Fatal(err)
	// 			}

	// 			for e.Scan() {

	// 				token := e.Text()

	// 				if _, echoed := echoed_set[token]; echoed {
	// 					continue
	// 				}

	// 				for _, set := range sets {
	// 					if _, in_this_set := set[token]; !in_this_set {
	// 						stdout.WriteString(token)
	// 						stdout.WriteByte('\n')

	// 						echoed_set[token] = true
	// 						break
	// 					}
	// 				}

	// 			}

	// 			e.Close()

	// 		}
	// 	}

	// }

}

func NewScalableBloom(size uint) bloom.Bloom {
	// memory aligned double size in millions
	return scalable.New(size << 21)
}

func NewEmitter(file_path, regex_match, regex_capture string) (*Emitter, error) {

	var (
		e   = new(Emitter)
		err error
	)

	e.file, err = os.Open(file_path)
	if err != nil {
		return nil, err
	}

	e.scanner = bufio.NewScanner(bufio.NewReaderSize(e.file, *buffer_size))

	if regex_match != "" {
		e.regex_match = regexp.MustCompile(regex_match)
	}
	if regex_capture != "" {
		e.regex_capture = regexp.MustCompile(regex_capture)
	}

	return e, nil

}
func (e *Emitter) Scan() bool {
	for e.scanner.Scan() {
		total_lines_scanned++
		e.current = e.scanner.Bytes()
		if *trim {
			e.current = bytes.TrimSpace(e.current)
		}
		if e.regex_match != nil && !e.regex_match.Match(e.current) {
			continue
		}
		if e.regex_capture != nil {
			matches := e.regex_capture.FindSubmatch(e.current)
			if len(matches) == 2 {
				e.current = matches[1]
			} else {
				continue
			}
		}
		total_tokens_emitted++
		return true
	}
	return false
}
func (e *Emitter) Bytes() []byte {
	return e.current
}
func (e *Emitter) Text() string {
	return string(e.current)
}
func (e *Emitter) Close() error {
	return e.file.Close()
}
