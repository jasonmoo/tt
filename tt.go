package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/reducedb/bloom"
	"github.com/reducedb/bloom/scalable"
)

type (
	Bloomer struct {
		bloom.Bloom
		filters []bloom.Bloom
	}
)

var (
	intersection = flag.Bool("i", false, "calculate the intersection")
	diff         = flag.Bool("d", false, "calculate the difference")
	union        = flag.Bool("u", false, "calculate the union")

	// activate lossy processing
	blooms = flag.Uint("blooms", 0, "number of bloom filters to use (lossy)")

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

				file, err := os.Open(file_path)
				if err != nil {
					log.Fatal(err)
				}

				scanner := bufio.NewScanner(file)

				for scanner.Scan() {
					token := scanner.Bytes()
					if !unique_set.Check(token) {
						stdout.Write(token)
						stdout.WriteByte('\n')
						total++
						unique_set.Add(token)
					}
				}

				file.Close()

			}

			return
		}

		// multi file handling below
		sets := make([]bloom.Bloom, len(file_paths))

		// may require throttling due to disk thrashing
		// initial scan to fill the bloom filters
		for i, file_path := range file_paths {

			set := NewScalableBloom(*blooms)

			file, err := os.Open(file_path)
			if err != nil {
				log.Fatal(err)
			}

			scanner := bufio.NewScanner(file)

			for scanner.Scan() {
				set.Add(scanner.Bytes())
			}

			file.Close()

			sets[i] = set

		}

		// do the work
		switch {

		// unique set of tokens that exist in all files
		case *intersection:

			echoed_set := NewScalableBloom(*blooms)

			for _, file_path := range file_paths {

				file, err := os.Open(file_path)
				if err != nil {
					log.Fatal(err)
				}

				scanner := bufio.NewScanner(file)

			NEXT_TOKEN:
				for scanner.Scan() {

					token := scanner.Bytes()

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

				file.Close()

			}

		// unique set of tokens not in the intersection
		case *diff:

			echoed_set := NewScalableBloom(*blooms)

			for _, file_path := range file_paths {

				file, err := os.Open(file_path)
				if err != nil {
					log.Fatal(err)
				}

				scanner := bufio.NewScanner(file)

				for scanner.Scan() {

					token := scanner.Bytes()

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

				file.Close()

			}
		}

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

			NEXT_TOKEN2:
				for scanner.Scan() {

					token := scanner.Text()

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
