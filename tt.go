package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"time"
)

var (
	intersection = flag.Bool("i", false, "calculate the intersection")
	diff         = flag.Bool("d", false, "calculate the difference")
	union        = flag.Bool("u", false, "calculate the union")

	// activate lossy processing
	blooms     = flag.Uint("blooms", 0, "number of bloom filters to use (lossy)")
	mash_bytes = flag.Bool("mash", false, "mash bytes for fewer collisions in bloom filters")

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
		fmt.Println("Usage: tt -[i,d,u] [-blooms N [-mash]] file1 file2[ file3..]")
		flag.PrintDefaults()
		os.Exit(1)
	}

	file_paths := flag.Args()

	switch {

	// activate bloom filter solution
	case *blooms > 0:

	// defaults to map solution
	default:

		if *union {

			unique_set := make(map[string]bool)

			for _, file_path := range file_paths {

				file, err := os.Open(file_path)
				if err != nil {
					log.Fatal(err)
				}

				scanner := bufio.NewScanner(file)

				for scanner.Scan() {
					token := scanner.Text()
					if _, exists := unique_set[token]; !exists {
						stdout.WriteString(token)
						stdout.WriteByte('\n')
						total++
						unique_set[token] = true
					}
				}

				file.Close()

			}

			return
		}

		// multi file handling below
		sets := make([]map[string]bool, len(file_paths))

		// may require throttling due to disk thrashing
		// initial scan to fill the bloom filters
		for i, file_path := range file_paths {

			set := make(map[string]bool)

			file, err := os.Open(file_path)
			if err != nil {
				log.Fatal(err)
			}

			scanner := bufio.NewScanner(file)

			for scanner.Scan() {
				set[scanner.Text()] = true
			}

			file.Close()

			sets[i] = set

		}

		// do the work
		switch {

		// unique set of tokens that exist in all files
		case *intersection:

			echoed_set := make(map[string]bool)

			for _, file_path := range file_paths {

				file, err := os.Open(file_path)
				if err != nil {
					log.Fatal(err)
				}

				scanner := bufio.NewScanner(file)

			NEXT_TOKEN:
				for scanner.Scan() {

					token := scanner.Text()

					if _, echoed := echoed_set[token]; echoed {
						goto NEXT_TOKEN
					}

					for _, set := range sets {
						if _, in_this_set := set[token]; !in_this_set {
							goto NEXT_TOKEN
						}
					}

					stdout.WriteString(token)
					stdout.WriteByte('\n')
					total++
					echoed_set[token] = true

				}

				file.Close()

			}

		// unique set of tokens not in the intersection
		case *diff:

			echoed_set := make(map[string]bool)

			for _, file_path := range file_paths {

				file, err := os.Open(file_path)
				if err != nil {
					log.Fatal(err)
				}

				scanner := bufio.NewScanner(file)

				for scanner.Scan() {

					token := scanner.Text()

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

				file.Close()

			}
		}

	}

}
