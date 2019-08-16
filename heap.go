package main

import "container/heap"

type Row struct {
	K string
	V int64
}

func NewRow(row string, count int64) *Row {
	return &Row{
		K: row,
		V: count,
	}
}

// An MinHeap is a min-heap of HeapElements.
type MinHeap struct {
	slice []*Row
	n     int
}

func NewMinHeap(n int) *MinHeap {
	return &MinHeap{
		n:     n,
		slice: make([]*Row, 0, n+1),
	}
}

func (h MinHeap) Len() int           { return len(h.slice) }
func (h MinHeap) Less(i, j int) bool { return h.slice[i].V < h.slice[j].V }
func (h MinHeap) Swap(i, j int)      { h.slice[i], h.slice[j] = h.slice[j], h.slice[i] }

func (h *MinHeap) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	h.slice = append(h.slice, x.(*Row))
}

func (h *MinHeap) Pop() interface{} {
	old := h.slice
	n := len(old)
	x := old[n-1]
	h.slice = old[0 : n-1]
	return x
}

func (h *MinHeap) AppendRow(row *Row) {
	heap.Push(h, row)
	if h.Len() > h.n {
		heap.Pop(h)
	}
}

func (h *MinHeap) GetSlice() []*Row {
	return h.slice
}
