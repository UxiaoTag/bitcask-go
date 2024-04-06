package fio

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func clearfile(name string) {
	err := os.RemoveAll(name)
	if err != nil {
		panic(err)
	}
}

func TestNewFileIOManager(t *testing.T) {
	path, _ := os.Getwd()
	file, err := NewFileIOManager(filepath.Join(path, "tmp", "a.data"))
	assert.Nil(t, err)
	assert.NotNil(t, file)

	err = file.Close()
	assert.Nil(t, err)
	clearfile(filepath.Join(path, "tmp", "a.data"))
}

func TestWriteFileIO(t *testing.T) {
	path, _ := os.Getwd()
	file, err := NewFileIOManager(filepath.Join(path, "tmp", "a.data"))
	assert.Nil(t, err)
	assert.NotNil(t, file)

	n, err := file.Write([]byte(""))
	assert.Equal(t, 0, n)
	assert.Nil(t, err)

	n, err = file.Write([]byte("447"))
	t.Log(n, err)
	assert.Equal(t, 3, n)
	assert.Nil(t, err)

	n, err = file.Write([]byte("KV"))
	t.Log(n, err)
	assert.Equal(t, 2, n)
	assert.Nil(t, err)

	err = file.Close()
	assert.Nil(t, err)
	clearfile(filepath.Join(path, "tmp", "a.data"))
}

func TestReadFileIO(t *testing.T) {

	path, _ := os.Getwd()
	file, err := NewFileIOManager(filepath.Join(path, "tmp", "a.data"))
	assert.Nil(t, err)
	assert.NotNil(t, file)

	n, err := file.Write([]byte(""))
	assert.Equal(t, 0, n)
	assert.Nil(t, err)

	n, err = file.Write([]byte("447"))
	t.Log(n, err)
	assert.Equal(t, 3, n)
	assert.Nil(t, err)

	n, err = file.Write([]byte("KV"))
	t.Log(n, err)
	assert.Equal(t, 2, n)
	assert.Nil(t, err)

	data := make([]byte, 3)
	n, err = file.Read(data, 0)
	assert.Equal(t, 3, n)
	assert.Nil(t, err)
	assert.Equal(t, []byte("447"), data)

	data = make([]byte, 3)
	n, err = file.Read(data, 2)
	assert.Equal(t, 3, n)
	assert.Nil(t, err)
	assert.Equal(t, []byte("7KV"), data)

	err = file.Close()
	assert.Nil(t, err)
	clearfile(filepath.Join(path, "tmp", "a.data"))
}

func TestFileIO_SYNC(t *testing.T) {
	path, _ := os.Getwd()
	file, err := NewFileIOManager(filepath.Join(path, "tmp", "a.data"))
	assert.Nil(t, err)
	assert.NotNil(t, file)

	err = file.Sync()
	assert.Nil(t, err)

	err = file.Close()
	assert.Nil(t, err)
	defer clearfile(filepath.Join(path, "tmp", "a.data"))
}
