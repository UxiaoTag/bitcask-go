package data

import (
	"hash/crc32"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEncodeLogRecord(t *testing.T) {
	log := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("bitcask-go"),
		Type:  LogRecordNormal,
	}
	res1, n := EncodeLogRecord(log)
	t.Log(string(res1))
	t.Log(n)
	assert.NotNil(t, res1)
	assert.Greater(t, n, int64(5))

	log = &LogRecord{
		Key:  []byte("name"),
		Type: LogRecordNormal,
	}
	res2, n2 := EncodeLogRecord(log)
	assert.NotNil(t, res2)
	assert.Greater(t, n2, int64(5))
	t.Log(res2)
	t.Log(n2)

	log = &LogRecord{
		Key:   []byte("name"),
		Value: []byte("bitcask-go"),
		Type:  LogRecordDelete,
	}
	res3, n3 := EncodeLogRecord(log)
	assert.NotNil(t, res3)
	assert.Greater(t, n3, int64(5))
	t.Log(res3)
	t.Log(n3)
	assert.NotNil(t, res3)
	assert.Greater(t, n3, int64(5))
}

func TestDecodeLogRecord(t *testing.T) {
	log := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("bitcask-go"),
		Type:  LogRecordNormal,
	}
	res1, _ := EncodeLogRecord(log)
	log1, n := decodeLogRecordHeader(res1[:maxLogRecordHeaderSize])
	assert.Equal(t, int64(7), n)
	assert.Equal(t, uint32(2532332136), log1.crc)
	assert.Equal(t, uint32(4), log1.keySize)
	assert.Equal(t, uint32(10), log1.valueSize)

	log = &LogRecord{
		Key:  []byte("name"),
		Type: LogRecordNormal,
	}
	res2, n2 := EncodeLogRecord(log)
	assert.NotNil(t, res2)
	assert.Greater(t, n2, int64(5))
	t.Log(res2)
	t.Log(n2)
	log2, n2 := decodeLogRecordHeader(res2[:11])
	t.Log(log2)
	assert.Equal(t, int64(7), n2)
	assert.Equal(t, uint32(240712713), log2.crc)
	assert.Equal(t, uint32(4), log2.keySize)
	assert.Equal(t, uint32(0), log2.valueSize)

	log = &LogRecord{
		Key:   []byte("name"),
		Value: []byte("bitcask-go"),
		Type:  LogRecordDelete,
	}
	res3, n3 := EncodeLogRecord(log)
	assert.NotNil(t, res3)
	assert.Greater(t, n3, int64(5))
	t.Log(res3)
	t.Log(n3)
	assert.NotNil(t, res3)
	assert.Greater(t, n3, int64(5))
	log3, n3 := decodeLogRecordHeader(res3[:maxLogRecordHeaderSize])
	t.Log(log3)
	assert.Equal(t, int64(7), n3)
	assert.Equal(t, uint32(290887979), log3.crc)
	assert.Equal(t, uint32(4), log3.keySize)
	assert.Equal(t, uint32(10), log3.valueSize)

}

func TestGetLogRecordCRC(t *testing.T) {
	log := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("bitcask-go"),
		Type:  LogRecordNormal,
	}
	res1, _ := EncodeLogRecord(log)
	log1, n := decodeLogRecordHeader(res1[:maxLogRecordHeaderSize])
	crc1 := getLogRecordCRC(log, res1[crc32.Size:n])
	assert.Equal(t, log1.crc, crc1)

	log = &LogRecord{
		Key:  []byte("name"),
		Type: LogRecordNormal,
	}
	res2, _ := EncodeLogRecord(log)
	log2, n2 := decodeLogRecordHeader(res2[:11])
	crc2 := getLogRecordCRC(log, res2[crc32.Size:n2])
	assert.Equal(t, log2.crc, crc2)

	log = &LogRecord{
		Key:   []byte("name"),
		Value: []byte("bitcask-go"),
		Type:  LogRecordDelete,
	}
	res3, n3 := EncodeLogRecord(log)
	assert.NotNil(t, res3)
	assert.Greater(t, n3, int64(5))
	t.Log(res3)
	t.Log(n3)
	assert.NotNil(t, res3)
	assert.Greater(t, n3, int64(5))
	log3, n3 := decodeLogRecordHeader(res3[:maxLogRecordHeaderSize])
	t.Log(log3)
	assert.Equal(t, int64(7), n3)
	assert.Equal(t, uint32(290887979), log3.crc)
	assert.Equal(t, uint32(4), log3.keySize)
	assert.Equal(t, uint32(10), log3.valueSize)
	crc3 := getLogRecordCRC(log, res3[crc32.Size:n3])
	assert.Equal(t, log3.crc, crc3)

}
