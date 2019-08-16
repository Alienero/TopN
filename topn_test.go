package main

import (
	"io"
	"log"
	"os"
	"path"
	"sort"
	"strconv"
	"sync"
	"testing"
)

var urls = []string{"aaa", "bbb", "ccc", "ddd", "eee"}

func TestBucketInMemoryTop3(t *testing.T) {
	result := []struct {
		k string
		v int64
	}{
		{"ccc", 26},
		{"bbb", 27},
		{"aaa", 27},
	}
	dir := path.Join(os.TempDir(), "spill")
	log.Println("spill dir:", dir)
	// only in mem, remove dir
	os.RemoveAll(dir)
	bucket := NewBucket(dir, 0, 1, true, 3, 4096*10*100)
	wg := &sync.WaitGroup{}
	wg.Add(1)
	ch := make(chan []*Row, 1)
	bucket.Run(wg, ch)
	for index := 0; index < 50; index++ {
		bucket.AppendRow(NewRow(urls[index%len(urls)], 1))
	}
	for index := 0; index < 50; index++ {
		bucket.AppendRow(NewRow(urls[index%(len(urls)-2)], 1))
	}
	bucket.NotifyScanFinished()
	rs := <-ch
	heap := &MinHeap{
		slice: rs,
		n:     3,
	}
	sort.Sort(heap)

	for n, r := range heap.slice {
		if r.K != result[n].k || r.V != result[n].v {
			t.Errorf("(%s %d) != (%s,%d)", r.K, r.V, result[n].k, result[n].v)
		}
	}
}

type Scanner struct {
	index int
	total int
	done  bool
}

func (r *Scanner) Read() (*Row, error) {
	if r.done {
		panic("EOF !!!")
	}
	r.index++
	if r.index > r.total {
		r.done = true
		return nil, io.EOF
	}
	return NewRow(strconv.Itoa(r.index), int64(r.index)), nil
}

func (r *Scanner) Close() error {
	return nil
}

func TestTopNTop10(t *testing.T) {
	dir := path.Join(os.TempDir(), "spill")
	// prepare
	os.RemoveAll(dir)
	log.Println("spill dir:", dir)
	group := NewGroupedTopN("", dir, true, 16, 10, 10*10*3)
	scanner := &Scanner{total: 1000}
	rs := group.calculateFromReader(scanner, true)

	heap := &MinHeap{
		slice: rs,
		n:     3,
	}
	sort.Sort(heap)

	for n, r := range heap.slice {
		if int64(n+991) != r.V {
			t.Error("result not match")
		}
	}
}
