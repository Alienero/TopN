package main

import (
	"bufio"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
)

type ReadeCloser interface {
	Read() (*Row, error)
	io.Closer
}

type RawDataSanner struct {
	file *os.File
	br   *bufio.Reader
	sync.Mutex
	isClosed bool
}

func NewRawDataSanner(path string) (*RawDataSanner, error) {
	// read raw data file.
	file, err := os.Open(path) // For read access.
	if err != nil {
		return nil, err
	}
	br := bufio.NewReader(file)

	return &RawDataSanner{
		file: file,
		br:   br,
	}, nil
}

func newRawDataSannerFromFile(file *os.File) (*RawDataSanner, error) {
	// reset
	file.Seek(0, 0)
	br := bufio.NewReader(file)

	return &RawDataSanner{
		file: file,
		br:   br,
	}, nil
}

func (r *RawDataSanner) Read() (*Row, error) {
	raw, _, err := r.br.ReadLine()
	if err != nil {
		return nil, err
	}

	return NewRow(string(raw), 1), nil
}

func (r *RawDataSanner) Close() error {
	r.Lock()
	defer r.Unlock()
	if r.isClosed {
		return nil
	}
	r.isClosed = true
	return r.file.Close()
}

type MergeDataScanner struct {
	*RawDataSanner
}

func NewMergeDataScannerFromPath(path string) (*MergeDataScanner, error) {
	s, err := NewRawDataSanner(path)
	if err != nil {
		return nil, err
	}
	return &MergeDataScanner{
		RawDataSanner: s,
	}, nil
}

func NewMergeDataScannerFromFile(file *os.File) (*MergeDataScanner, error) {
	s, err := newRawDataSannerFromFile(file)
	if err != nil {
		return nil, err
	}
	return &MergeDataScanner{
		RawDataSanner: s,
	}, nil
}

func (r *MergeDataScanner) Read() (*Row, error) {
	raw, _, err := r.br.ReadLine()
	if err != nil {
		return nil, err
	}

	ss := strings.Split(string(raw), "\t")
	row := ss[0]
	count, err := strconv.ParseInt(ss[1], 10, 64)
	if err != nil {
		return nil, err
	}

	return NewRow(row, count), nil
}

var _ ReadeCloser = new(MergeDataScanner)
var _ ReadeCloser = new(RawDataSanner)
