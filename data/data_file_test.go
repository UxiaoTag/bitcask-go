package data

import (
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOpenDataFile(t *testing.T) {
	mypath, _ := os.Getwd()
	path := path.Join(mypath, "tmp")
	DataFile, err := OpenDataFile(path, 0)
	assert.Nil(t, err)
	assert.NotNil(t, DataFile)

	DataFile2, err := OpenDataFile(path, 111)
	assert.Nil(t, err)
	assert.NotNil(t, DataFile2)

	DataFile3, err := OpenDataFile(path, 111)
	assert.Nil(t, err)
	assert.NotNil(t, DataFile3)

	t.Log(os.Getwd())

}

func TestDataFile_Write(t *testing.T) {
	mypath, _ := os.Getwd()
	path := path.Join(mypath, "tmp")
	DataFile, err := OpenDataFile(path, 0)
	assert.Nil(t, err)
	assert.NotNil(t, DataFile)

	err = DataFile.Write([]byte("aaa"))
	assert.Nil(t, err)

	err = DataFile.Write([]byte("c"))
	assert.Nil(t, err)

	err = DataFile.Write([]byte("ddaa"))
	assert.Nil(t, err)
}

func TestDataFile_Close(t *testing.T) {
	mypath, _ := os.Getwd()
	path := path.Join(mypath, "tmp")
	DataFile, err := OpenDataFile(path, 0)
	assert.Nil(t, err)
	assert.NotNil(t, DataFile)

	err = DataFile.Close()
	assert.Nil(t, err)

}

func TestDataFile_Sync(t *testing.T) {
	mypath, _ := os.Getwd()
	path := path.Join(mypath, "tmp")
	DataFile, err := OpenDataFile(path, 2444)
	assert.Nil(t, err)
	assert.NotNil(t, DataFile)

	err = DataFile.Write([]byte("aaa"))
	assert.Nil(t, err)

	err = DataFile.Write([]byte("c"))
	assert.Nil(t, err)

	err = DataFile.Write([]byte("ddaa"))
	assert.Nil(t, err)

	err = DataFile.Sync()
	assert.Nil(t, err)

}
