package main

import (
	"bufio"
	"bytes"
	"io"
	"os"
	"runtime"
	"sync"
)

type (
	Sprayer struct {
		file *bufio.Reader

		chunkSize  int
		bufferSize int
	}
)

func NewSprayer(file *os.File, chunk, buffer int) *Sprayer {
	return &Sprayer{
		file:       bufio.NewReaderSize(file, buffer),
		chunkSize:  chunk,
		bufferSize: buffer,
	}
}

func (s *Sprayer) Run(f func(chunk []byte)) (err error) {

	chunkChan := make(chan []byte, 256)
	workers := runtime.NumCPU()

	wg := new(sync.WaitGroup)
	wg.Add(workers)

	for i := 0; i < workers; i++ {
		go func() {
			for chunk := range chunkChan {
				f(chunk)
			}
			wg.Done()
		}()
	}

	for {
		buf := make([]byte, s.bufferSize)

		var n int
		n, err = s.file.Read(buf)
		if err != nil && err != io.EOF {
			break
		}
		if n == 0 {
			break
		}
		buf = buf[:n]

		// read to a newline to prevent
		// spanning a word across two buffers
		if buf[len(buf)-1] != '\n' {
			var extra []byte
			extra, err = s.file.ReadBytes('\n')
			if err != nil && err != io.EOF {
				break
			}
			buf = append(buf, extra...)
		}

		for len(buf) > 0 {

			var chunk []byte

			if len(buf) < chunk {
				chunk = buf
				buf = buf[:0]
			} else {
				chunk = buf[:chunk]
				buf = buf[chunk:]
			}

			// read to a newline to prevent
			// spanning a word across two buffers
			if chunk[len(chunk)-1] != '\n' {
				i := bytes.IndexByte(buf, '\n')
				chunk = append(chunk, buf[:i]...)
				buf = buf[i:]
			}

			chunkChan <- chunk

		}

	}

	close(chunkChan)
	wg.Wait()

	return

}
