package main

import (
	"bufio"
	"flag"
	"math/rand"
	"os"
)

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randSeq(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

var (
	urls  = make([]string, 0, 900)
	out   = flag.String("out", "/tmp/url.txt", "")
	count = flag.Int("count", 2000*1000*100, "")
)

func main() {
	flag.Parse()
	for index := 0; index < 1000; index++ {
		urls = append(urls, randSeq(512))
	}
	file, err := os.Create(*out)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	w := bufio.NewWriter(file)
	defer w.Flush()

	for index := 0; index < *count; index++ {
		n := index % 1000
		w.WriteString(urls[n])
		w.WriteByte('\n')
		if n <= 100 {
			for index := 0; index < n; index++ {
				w.WriteString(urls[n])
				w.WriteByte('\n')
			}
		}
		println(index)
	}
}
