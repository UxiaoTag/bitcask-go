package fio

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMMapRead(t *testing.T) {

	defer clearTestTmp()
	//为空文件
	path, _ := os.Getwd()
	path = filepath.Join(path, "tmp", "mmap.data")

	mmap, err := NewMMapIOManager(path)
	assert.Nil(t, err)

	b1 := make([]byte, 10)
	n1, err := mmap.Read(b1, 0)
	t.Log(n1)
	t.Log(err)
	assert.Equal(t, 0, n1)
	assert.Equal(t, err, io.EOF)

	//存在一个文件
	path, _ = os.Getwd()
	path = filepath.Join(path, "tmp", "mmapa.data")

	file, err := NewFileIOManager(path)
	assert.Nil(t, err)

	data := []byte("test String")

	file.Write(data)

	file.Write([]byte("test2 String"))

	file.Close()

	mmapIO, err := NewMMapIOManager(path)
	t.Log(mmapIO)
	t.Log(err)

	readdata := make([]byte, len(data))

	size, err := mmapIO.Read(readdata, 0)
	t.Log(size)
	t.Log(err)
	t.Log(string(readdata))
	assert.Nil(t, err)
	assert.Equal(t, data, readdata)

	readdata2 := make([]byte, len(data)+1)

	size, err = mmapIO.Read(readdata2, int64(size))
	t.Log(size)
	t.Log(err)
	t.Log(string(readdata2))
	assert.Nil(t, err)
	assert.Equal(t, size, len(data)+1)
	// t.Fail()

	filesize, err := mmapIO.Size()
	t.Log(filesize)
	assert.Nil(t, err)
	assert.Equal(t, len(data)*2+1, int(filesize))

	mmap.Close()
	mmapIO.Close()
}

func clearTestTmp() {
	path, _ := os.Getwd()
	path = filepath.Join(path, "tmp")
	os.RemoveAll(path)
}
