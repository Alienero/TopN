package main

import (
	"container/heap"
	"testing"
)

func TestMinHeapAppend(t *testing.T) {
	var top3HeapTest = map[string]int64{
		"a": 10,
		"b": 6,
		"c": 1,
		"d": 13,
		"e": 93,
	}

	var top3HeapTestResult = []struct {
		row   string
		count int64
	}{
		{"a", 10},
		{"d", 13},
		{"e", 93},
	}

	minHeap := NewMinHeap(3)

	for k, v := range top3HeapTest {
		minHeap.AppendRow(NewRow(k, v))
	}

	for i := 0; minHeap.Len() > 0; i++ {
		kv := heap.Pop(minHeap).(*Row)
		result := top3HeapTestResult[i]
		if kv.K != result.row || kv.V != result.count {
			t.Errorf("Result not macth: kv(%s,%d) != result(%s,%d)", kv.K, kv.V, result.row, result.count)
		}
	}
}
