package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"time"

	"github.com/jasonmoo/wc"
	"github.com/zhenjl/bloom"
	"github.com/zhenjl/bloom/scalable"
)

var (
	intersection = flag.Bool("i", false, "calculate the intersection")
	diff         = flag.Bool("d", false, "calculate the difference")
	union        = flag.Bool("u", false, "calculate the union")

	count = flag.Bool("c", false, "output counts of each token on non-large unions")

	// bloom processing
	large           = flag.Bool("large", false, "use bloom filters for large data size (may be lossy)")
	estimated_lines = flag.Uint64("estimated_lines", 0, "estimate used to size bloom filters (set this to avoid prescan)")

	// options
	trim          = flag.Bool("trim", false, "trim each line")
	match_regex   = flag.String("match", "", "only process matching lines")
	capture_regex = flag.String("capture", "", "only process captured data")

	// for fs opts
	devnull     = flag.Bool("devnull", false, "do not output tokens, just counts")
	buffer_size = flag.Int("buffer_size", 1<<20, "buffered io chunk size")

	// totals
	total_tokens_emitted  uint64
	total_bytes_processed uint64
	total_lines_scanned   uint64
	total_lines_matched   uint64
)

func init() {
	runtime.GOMAXPROCS(runtime.NumCPU())
}

func main() {

	flag.Parse()

	if !*intersection && !*diff && !*union {
		fmt.Println(`Usage: tt -[i,d,u] [-c] [-trim] [-match "regex"] [-capture "regex"] [-large [-estimated_lines N]] file1 file2[ file3..]`)
		flag.PrintDefaults()
		os.Exit(1)
	}

	start := time.Now()

	var stdout WriteFlusher

	if *devnull {
		stdout = new(DevNullWriter)
	} else {
		// buffered io
		stdout = bufio.NewWriterSize(os.Stdout, *buffer_size)
	}

	defer func() {
		stdout.Flush()
		fmt.Fprintln(os.Stderr, "** Token Report **")
		fmt.Fprintln(os.Stderr, "Lines scanned: ", total_lines_scanned)
		if *match_regex != "" {
			fmt.Fprintln(os.Stderr, "Lines matched: ", total_lines_matched)
		}
		fmt.Fprintln(os.Stderr, "Tokens emitted: ", total_tokens_emitted)
		fmt.Fprintln(os.Stderr, "Time: ", time.Since(start))
	}()

	file_paths := flag.Args()

	fmt.Fprintln(os.Stderr, "tt starting up")

	// if no estimate supplied, count lines
	if *large && *estimated_lines == 0 {

		var bytes_to_process uint64

		for _, file_path := range file_paths {

			file, err := os.Open(file_path)
			if err != nil {
				log.Fatal(err)
			}

			counter := wc.NewCounter(file)
			err = counter.Count(false, true, true, false)
			if err != nil {
				log.Fatal(err)
			}

			*estimated_lines += counter.Lines
			bytes_to_process += counter.Bytes

			file.Close()
		}

		fmt.Fprintln(os.Stderr, "Bytes to process: ", bytes_to_process)
		fmt.Fprintln(os.Stderr, "Lines to process: ", *estimated_lines)
	}

	if *large {

		if *union {

			unique_set := NewScalableBloom(*estimated_lines)

			for _, file_path := range file_paths {

				e, err := NewEmitter(file_path, *match_regex, *capture_regex, *buffer_size)
				if err != nil {
					log.Fatal(err)
				}

				for e.Scan() {
					token := e.Bytes()
					if !unique_set.Check(token) {
						total_tokens_emitted++
						stdout.Write(token)
						stdout.WriteByte('\n')
						unique_set.Add(token)
					}
				}

				e.Close()

				total_lines_scanned += e.LinesScanned

			}

			return
		}

		// multi file handling below
		sets := make([]bloom.Bloom, len(file_paths))

		// may require throttling due to disk thrashing
		// initial scan to fill the bloom filters
		for i, file_path := range file_paths {

			set := NewScalableBloom(*estimated_lines)

			e, err := NewEmitter(file_path, *match_regex, *capture_regex, *buffer_size)
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

			echoed_set := NewScalableBloom(*estimated_lines)

			for _, file_path := range file_paths {

				e, err := NewEmitter(file_path, *match_regex, *capture_regex, *buffer_size)
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

					total_tokens_emitted++
					stdout.Write(token)
					stdout.WriteByte('\n')
					echoed_set.Add(token)

				}

				total_lines_scanned += e.LinesScanned

				e.Close()

			}

		// unique set of tokens not in the intersection
		case *diff:

			echoed_set := NewScalableBloom(*estimated_lines)

			for _, file_path := range file_paths {

				e, err := NewEmitter(file_path, *match_regex, *capture_regex, *buffer_size)
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
							total_tokens_emitted++
							stdout.Write(token)
							stdout.WriteByte('\n')
							echoed_set.Add(token)
						}
					}

				}

				total_lines_scanned += e.LinesScanned

				e.Close()

			}
		}

		// defaults to map solution
	} else {

		if *union {

			unique_set := make(map[string]int)

			for _, file_path := range file_paths {

				e, err := NewEmitter(file_path, *match_regex, *capture_regex, *buffer_size)
				if err != nil {
					log.Fatal(err)
				}

				for e.Scan() {
					unique_set[e.Text()]++
				}

				total_lines_scanned += e.LinesScanned

				e.Close()

			}

			if *count {
				for token, ct := range unique_set {
					total_tokens_emitted++
					fmt.Fprintf(stdout, "%d: %s\n", ct, token)
				}
			} else {
				for token, _ := range unique_set {
					total_tokens_emitted++
					stdout.WriteString(token)
					stdout.WriteByte('\n')
				}
			}

			return
		}

		// multi file handling below
		sets := make([]map[string]bool, len(file_paths))

		// may require throttling due to disk thrashing
		// initial scan to fill the bloom filters
		for i, file_path := range file_paths {

			set := make(map[string]bool)

			e, err := NewEmitter(file_path, *match_regex, *capture_regex, *buffer_size)
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

				e, err := NewEmitter(file_path, *match_regex, *capture_regex, *buffer_size)
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

					total_tokens_emitted++
					stdout.WriteString(token)
					stdout.WriteByte('\n')

					echoed_set[token] = true

				}

				total_lines_scanned += e.LinesScanned

				e.Close()

			}

		// unique set of tokens not in the intersection
		case *diff:

			echoed_set := make(map[string]bool)

			for _, file_path := range file_paths {

				e, err := NewEmitter(file_path, *match_regex, *capture_regex, *buffer_size)
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
							total_tokens_emitted++
							stdout.WriteString(token)
							stdout.WriteByte('\n')

							echoed_set[token] = true
							break
						}
					}

				}

				total_lines_scanned += e.LinesScanned

				e.Close()

			}
		}

	}

}

func NewScalableBloom(size uint64) bloom.Bloom {
	return scalable.New(uint(size))
}
