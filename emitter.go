package main

import (
	"bufio"
	"bytes"
	"os"
	"regexp"
)

func NewEmitter(file_path, match_regex, capture_regex string, buffer_size int) (*Emitter, error) {

	var (
		e   = new(Emitter)
		err error
	)

	e.file, err = os.Open(file_path)
	if err != nil {
		return nil, err
	}

	e.scanner = bufio.NewScanner(bufio.NewReaderSize(e.file, buffer_size))

	if match_regex != "" {
		e.match_regex = regexp.MustCompile(match_regex)
	}
	if capture_regex != "" {
		e.capture_regex = regexp.MustCompile(capture_regex)
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
		if e.match_regex != nil && !e.match_regex.Match(e.current) {
			continue
		}
		if e.capture_regex != nil {
			matches := e.capture_regex.FindSubmatch(e.current)
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
