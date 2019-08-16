package main

import (
	"bufio"
	"flag"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"os"
	"path"
	"sort"
	"strconv"
	"sync"
	"time"
)

var (
	dataPath  = flag.String("data", "/disk4/test/urls.txt", "url source data file")
	n         = flag.Int("n", 100, "TopN url")
	spillDir  = flag.String("spill-dir", "/disk4/test/sp", "The dir using for spill data files")
	memLimit  = flag.Int("mem", 768, "Memory limit for TopN calculate. Unit: MB")
	bucketNum = flag.Int("bucket", 4, "The number of buckets used by url")
)

func main() {
	flag.Parse()
	group := NewGroupedTopN(*dataPath, *spillDir, true, *bucketNum, *n, int64(*memLimit)*1024*1024)
	start := time.Now()
	rs := group.Calculate()
	end := time.Since(start)
	h := &MinHeap{
		n:     100,
		slice: rs,
	}
	sort.Sort(h)
	for _, r := range rs {
		fmt.Printf("%s  %d\n", r.K, r.V)
	}

	fmt.Printf("cost: %v", end)
}

// A GroupedTopN represents a TopN calculate process
type GroupedTopN struct {
	// source data file path
	dataPath string
	// the dir for bucket worker spill data
	spillDir string
	// Whether it is intermediate spill data
	isRawData bool
	// bucket works
	buckets []*Bucket
	// total bucket number
	bucketNum int
	// TopN
	n int // TopN
	// total memory limit for the GroupedTopN process
	memoryLimit int64
}

// NewGroupedTopN init a GroupedTopN process.
func NewGroupedTopN(dataPath string, spillDir string, isRawData bool,
	bucketNum int, n int, memoryLimit int64) *GroupedTopN {
	buckets := make([]*Bucket, 0, bucketNum)
	return &GroupedTopN{
		dataPath:    dataPath,
		spillDir:    spillDir,
		isRawData:   isRawData,
		buckets:     buckets,
		bucketNum:   bucketNum,
		n:           n,
		memoryLimit: memoryLimit,
	}
}

// ParllelCalculate represents start a mutil-thread calculate
func (g *GroupedTopN) ParllelCalculate() []*Row {
	return g.calculate(true)
}

// Calculate represents start a single hread calculate
func (g *GroupedTopN) Calculate() []*Row {
	return g.calculate(false)
}

func (g *GroupedTopN) calculate(parallel bool) []*Row {
	var (
		scanner ReadeCloser
		err     error
	)

	if g.isRawData {
		scanner, err = NewRawDataSanner(g.dataPath)
	} else {
		scanner, err = NewMergeDataScannerFromPath(g.dataPath)
	}
	if err != nil {
		log.Fatal(err)
	}
	defer scanner.Close()

	return g.calculateFromReader(scanner, parallel)
}

func (g *GroupedTopN) calculateFromReader(reader ReadeCloser, parallel bool) []*Row {
	bucketMemLimit := g.memoryLimit
	if parallel {
		bucketMemLimit = g.memoryLimit / int64(g.bucketNum)
	}

	for index := 0; index < g.bucketNum; index++ {
		g.buckets = append(g.buckets, NewBucket(g.spillDir, index,
			g.bucketNum, parallel, g.n, bucketMemLimit))
	}
	defer func() {
		for _, b := range g.buckets {
			b.Close()
		}
	}()

	if err := os.MkdirAll(g.spillDir, os.ModePerm); err != nil {
		log.Fatal(err)
	}

	var (
		resultCh chan []*Row
	)

	if parallel {
		wg := &sync.WaitGroup{}
		resultCh = make(chan []*Row, g.bucketNum)
		wg.Add(g.bucketNum)

		go func() {
			wg.Wait()
			close(resultCh)
		}()

		for _, b := range g.buckets {
			b.Run(wg, resultCh)
		}
	}

	if err := g.scanRawData(reader); err != nil {
		log.Fatal(err)
	}

	// merge data
	minHeap := NewMinHeap(g.n)

	if parallel {
		for _, b := range g.buckets {
			b.NotifyScanFinished()
		}
		// wait result
		for rows := range resultCh {
			for _, row := range rows {
				minHeap.AppendRow(row)
			}
		}
	} else {
		for _, b := range g.buckets {
			rows, err := b.ReduceDataWithTopN()
			if err != nil {
				log.Fatal(err)
			}
			for _, row := range rows {
				minHeap.AppendRow(row)
			}
		}
	}

	return minHeap.GetSlice()
}

func (g *GroupedTopN) scanRawData(r ReadeCloser) error {
	defer r.Close()
	for {
		row, err := r.Read()
		if err != nil {
			if err == io.EOF {
				break
			} else {
				return err
			}
		}

		// calculate hash code
		bucket := g.getBucket(row)
		// send line to bucket
		err = g.buckets[bucket].AppendRow(row)
		if err != nil {
			return err
		}
	}
	return nil
}

func (g *GroupedTopN) getBucket(row *Row) int {
	return int(crc32.ChecksumIEEE([]byte(row.K)) % uint32(g.bucketNum))
}

type Bucket struct {
	inParallel    bool
	ch            chan *Row
	memoryLimit   int64
	memoryInUse   int64
	inMemMap      map[string]int64
	spillDir      string
	id            int
	bucketNum     int
	spillFilePath string
	spillFile     *os.File
	n             int
	sync.Mutex
}

func NewBucket(spillDir string, id int, bucketNum int, inParallel bool, n int, memoryLimit int64) *Bucket {
	var ch chan *Row
	if inParallel {
		ch = make(chan *Row, 4)
	}
	return &Bucket{
		spillFilePath: fmt.Sprintf("%s/%d.dat", spillDir, id),
		spillDir:      spillDir,
		id:            id,
		bucketNum:     bucketNum,
		inParallel:    inParallel,
		n:             n,
		memoryLimit:   memoryLimit,
		ch:            ch,
		inMemMap:      make(map[string]int64),
	}
}

// Run represents start a backgroud goroutine for accept data
func (bucket *Bucket) Run(wg *sync.WaitGroup, resultCh chan []*Row) {
	go bucket.run(wg, resultCh)
}

func (bucket *Bucket) run(wg *sync.WaitGroup, resultCh chan []*Row) {
	defer func() {
		wg.Done()
	}()
	for raw := range bucket.ch {
		if err := bucket.processRow(raw, true); err != nil {
			log.Fatal(err)
		}
	}
	rs, err := bucket.ReduceDataWithTopN()
	if err != nil {
		log.Fatal(err)
	}
	resultCh <- rs
}

func (bucket *Bucket) processRow(row *Row, enableSpill bool) error {
	// the number of bytes in row.
	size := int64(len(row.K))

	if lastCount, ok := bucket.inMemMap[row.K]; ok {
		bucket.inMemMap[row.K] = lastCount + row.V
	} else {
		bucket.inMemMap[row.K] = row.V
	}
	bucket.memoryInUse += size
	if !bucket.hasMemory() && enableSpill {
		// spill to disk
		err := bucket.spillToDisk()
		if err != nil {
			return err
		}
	}
	return nil
}

func (bucket *Bucket) hasMemory() bool {
	return bucket.memoryInUse < bucket.memoryLimit
}

func (bucket *Bucket) spillToDisk() (err error) {
	if bucket.spillFile == nil {
		// create spill file
		bucket.spillFile, err = os.OpenFile(bucket.spillFilePath,
			os.O_APPEND|os.O_CREATE|os.O_RDWR, 0755)
		if err != nil {
			return
		}
	}

	writer := bufio.NewWriter(bucket.spillFile)
	for row, count := range bucket.inMemMap {
		writer.WriteString(row)
		writer.WriteByte('\t')
		writer.WriteString(strconv.FormatInt(count, 10))
		writer.WriteByte('\n')
	}

	// reset memory usage
	bucket.releaseResource(true)

	return writer.Flush()
}

func (bucket *Bucket) ReduceDataWithTopN() ([]*Row, error) {
	if bucket.spillFile == nil {
		log.Println("direct merge in memory")
		return bucket.reduceMemory(), nil
	}
	log.Println("read spill merge")
	// spill
	if err := bucket.spillToDisk(); err != nil {
		return nil, err
	}

	scanner, _ := NewMergeDataScannerFromFile(bucket.spillFile)
	for {
		row, err := scanner.Read()
		if err != nil {
			if err == io.EOF {
				break
			} else {
				return nil, err
			}
		}

		err = bucket.processRow(row, false)
		if err != nil {
			return nil, err
		}
		if !bucket.hasMemory() {
			// release memory
			bucket.releaseResource(false)
			// new topN
			topN := NewGroupedTopN(
				bucket.spillFilePath,
				path.Join(bucket.spillDir, strconv.Itoa(bucket.id)),
				false,
				bucket.bucketNum*16,
				bucket.n,
				bucket.memoryLimit,
			)
			return topN.Calculate(), nil
		}
	}

	log.Println("reduce in memory")
	return bucket.reduceMemory(), nil
}

func (bucket *Bucket) reduceMemory() []*Row {
	heap := NewMinHeap(bucket.n)
	for k, v := range bucket.inMemMap {
		heap.AppendRow(NewRow(k, v))
	}
	bucket.releaseResource(false)
	return heap.GetSlice()
}

// AppendRow represents add a row into this bucket
func (bucket *Bucket) AppendRow(row *Row) (err error) {
	if bucket.inParallel {
		bucket.ch <- row
	} else {
		err = bucket.processRow(row, true)
	}
	return
}

func (bucket *Bucket) Close() error {
	return bucket.releaseResource(false)
}

func (bucket *Bucket) NotifyScanFinished() {
	close(bucket.ch)
}

func (bucket *Bucket) releaseResource(renew bool) (err error) {
	bucket.Lock()
	defer bucket.Unlock()

	bucket.memoryInUse = 0
	if renew {
		bucket.inMemMap = make(map[string]int64)
	} else {
		bucket.inMemMap = nil
		if bucket.spillFile != nil {
			err = bucket.spillFile.Close()
			bucket.spillFile = nil
		}
	}
	return
}
