package main

import (
	"bufio"
	"flag"
	"fmt"
	"github.com/jasonmoo/bloom/scalable"
	"log"
	"os"
)

var (
	a   = flag.String("a", "", "input file a")
	b   = flag.String("b", "", "input file b")
	est = flag.Uint("est", 1<<20, "estimated size of set a")
)

func main() {

	flag.Parse()

	if *a == "" || *b == "" {
		fmt.Println("Usage: tt -a <filea.txt> -b <fileb.txt>")
		os.Exit(1)
	}

	filter := scalable.New(*est)

	filea, err := os.Open(*a)
	if err != nil {
		log.Fatal(err)
	}
	defer filea.Close()

	var ct_a, ct_b, ct_found uint64

	scanner := bufio.NewScanner(filea)
	for scanner.Scan() {
		ct_a++
		filter.Add(scanner.Bytes())
	}
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	fileb, err := os.Open(*b)
	if err != nil {
		log.Fatal(err)
	}
	defer fileb.Close()

	scanner = bufio.NewScanner(fileb)
	for scanner.Scan() {
		ct_b++
		if t := scanner.Bytes(); filter.Check(t) {
			ct_found++
			fmt.Printf("%s\n", t)
		}
	}
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	fmt.Fprintf(os.Stderr, "\nTotal A: %d\nTotal B: %d\nB found in A: %d\n", ct_a, ct_b, ct_found)

}
