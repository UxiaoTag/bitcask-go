package benchmark

import (
	bitcask "bitcask-go"
	"bitcask-go/utils"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var db *bitcask.DB

func init() {
	opts := bitcask.DefaultDBOptions
	dir, _ := os.MkdirTemp("", "bitcask-go")
	opts.DirPath = dir
	var err error
	db, err = bitcask.Open(opts)
	if err != nil {
		panic(err)
	}
}

func Benchmark_Put(b *testing.B) {
	b.ResetTimer()
	//打印基准测试内存分配情况
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := db.Put(utils.GetTestKey(i), utils.GetTestKey(1024))
		assert.Nil(b, err)
	}
}

func Benchmark_Get(b *testing.B) {
	for i := 0; i < 10000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.GetTestKey(1024))
		assert.Nil(b, err)
	}

	src := rand.NewSource(time.Now().UnixNano())
	r := rand.New(src)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := db.Get(utils.GetTestKey(r.Int()))
		if err != nil && err != bitcask.ErrKeyNotFound {
			b.Fatal(err)
		}
	}
}

func Benchmark_Delete(b *testing.B) {
	for i := 0; i < 10000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.GetTestKey(1024))
		assert.Nil(b, err)
	}

	src := rand.NewSource(time.Now().UnixNano())
	r := rand.New(src)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		err := db.Delete(utils.GetTestKey(r.Int()))
		assert.Nil(b, err)
	}
}
