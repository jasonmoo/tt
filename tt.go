package main

import (
	"bufio"
	"flag"
	"fmt"
	"github.com/reducedb/bloom"
	"github.com/reducedb/bloom/scalable"

	// "crypto/sha1"
	// "github.com/spaolacci/murmur3"
	// "github.com/zhenjl/cityhash"

	"log"
	"os"
	"time"
)

var (
	intersection = flag.Bool("i", false, "calculate the intersection")
	diff         = flag.Bool("d", false, "calculate the difference")
	union        = flag.Bool("u", false, "calculate the union")

	blooms = flag.Uint("blooms", 1, "number of bloom filters to use")

	// buffered io
	stdout = bufio.NewWriterSize(os.Stdout, 4096)

	// unique filter
	unique_set bloom.Bloom

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

	file_paths := flag.Args()

	unique_set = NewScalableBloom(*blooms)

	// may omit entries due to false positives
	// todo(jason): try crypto hash or use dual filters
	if *union {

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
		for _, file_path := range file_paths {

			file, err := os.Open(file_path)
			if err != nil {
				log.Fatal(err)
			}

			scanner := bufio.NewScanner(file)

		NEXT_TOKEN:
			for scanner.Scan() {
				token := scanner.Bytes()
				for _, set := range sets {
					if !set.Check(token) || unique_set.Check(token) {
						goto NEXT_TOKEN
					}
				}
				stdout.Write(token)
				stdout.WriteByte('\n')
				total++
				unique_set.Add(token)
			}

			file.Close()

		}

	// unique set of tokens not in the intersection
	case *diff:
		for _, file_path := range file_paths {

			file, err := os.Open(file_path)
			if err != nil {
				log.Fatal(err)
			}

			scanner := bufio.NewScanner(file)

			for scanner.Scan() {
				token := scanner.Bytes()
				for _, set := range sets {
					if !set.Check(token) && !unique_set.Check(token) {
						stdout.Write(token)
						stdout.WriteByte('\n')
						total++
						unique_set.Add(token)
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

type (
	Bloomer struct {
		bloom.Bloom
		filters []bloom.Bloom
	}
)

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
	token = append(make([]byte, len(token)), token...)
	for _, filter := range b.filters {
		filter.Add(token)
		mash(token)
	}
	return b
}

func (b *Bloomer) Check(token []byte) bool {
	token = append(make([]byte, len(token)), token...)
	for _, filter := range b.filters {
		if !filter.Check(token) {
			return false
		}
		mash(token)
	}
	return true
}

// modifies the underlying structure
func mash(token []byte) {
	for i, c := range token {
		c ^= (20 * c)
		token[i] = c
	}
}
