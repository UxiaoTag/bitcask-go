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
	DataFile, err := OpenDataFile(path, 744)
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

func TestReadLogRecordDataFile(t *testing.T) {
	mypath, _ := os.Getwd()
	path := path.Join(mypath, "tmp")
	DataFile, err := OpenDataFile(path, 31)
	assert.Nil(t, err)
	assert.NotNil(t, DataFile)

	//一条LogRecord
	res := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("bitcask-go"),
		Type:  LogRecordNormal,
	}
	buf, bufsize := EncodeLogRecord(res)

	err = DataFile.Write(buf)

	assert.Nil(t, err)

	readlog, logsize, err := DataFile.ReadLogRecord(0)
	assert.Nil(t, err)
	assert.Equal(t, bufsize, logsize)
	assert.Equal(t, readlog, res)
	t.Log(buf, bufsize)

	//多条LogRecord
	res = &LogRecord{
		Key:   []byte("name"),
		Value: []byte("a new value"),
		Type:  LogRecordNormal,
	}
	buf2, bufsize2 := EncodeLogRecord(res)
	err = DataFile.Write(buf2)
	assert.Nil(t, err)
	t.Log(buf2, bufsize2)
	readlog2, logsize2, err := DataFile.ReadLogRecord(bufsize)
	assert.Nil(t, err)
	assert.Equal(t, bufsize2, logsize2)
	assert.Equal(t, readlog2, res)

	//删一个LogRecord
	res = &LogRecord{
		Key:   []byte("name"),
		Value: []byte(""),
		Type:  LogRecordDelete,
	}
	buf3, bufsize3 := EncodeLogRecord(res)
	err = DataFile.Write(buf3)
	assert.Nil(t, err)
	t.Log(buf3, bufsize3)
	readlog3, logsize3, err := DataFile.ReadLogRecord(bufsize + bufsize2)
	assert.Nil(t, err)
	assert.Equal(t, bufsize3, logsize3)
	assert.Equal(t, readlog3, res)
}
