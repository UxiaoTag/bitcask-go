package example

import (
	bitcask "bitcask-go"
	"fmt"
	"os"
	"path"
)

func main() {
	options := bitcask.DefaultDBOptions
	filepath, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	path := path.Join(filepath, "tmp")
	options.DirPath = path
	db, err := bitcask.Open(options)
	if err != nil {
		panic(err)
	}

	err = db.Put([]byte("name"), []byte("bitcask"))
	if err != nil {
		panic(err)
	}
	val, err := db.Get([]byte("name"))
	if err != nil {
		panic(err)
	}
	fmt.Printf("Key:name vlaue:%s", string(val))

	err = db.Delete([]byte("name"))
	if err != nil {
		panic(err)
	}
}
