package data

import (
	"encoding/binary"
	"hash/crc32"
)

type LogRecordType byte

const (
	//正常操作
	LogRecordNormal LogRecordType = iota
	//删除操作
	LogRecordDelete
	//事务提交完成标识
	LogRecordTxnFinished
)

// crc type  keysize(变长) valueSize(变长)
// 4 + 1 + 5 + 5
const maxLogRecordHeaderSize = binary.MaxVarintLen32*2 + 5

// 写入到数据文件的日志记录
type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType
}

// 索引的数据结构，主要描述数据在磁盘的位置
type LogRecordPos struct {
	Fid    uint32 //文件id，表示数据存放到文件的哪个位置
	Offset int64  //表示偏移量
}

// 写入到数据文件的日志头部
type LogRecordHeader struct {
	crc        uint32        //头部校验
	recordType LogRecordType //操作类型
	keySize    uint32        //key长度
	valueSize  uint32        //value长度
}

// 暂存事务的日志结构
type TransactionReocrd struct {
	Record *LogRecord
	Pos    *LogRecordPos
}

// 对LogRecord进行编码，返回字节数组以及长度
// crc recordType keysize valuesize  key value
// 4         1     5         5
func EncodeLogRecord(log *LogRecord) ([]byte, int64) {
	//初始化header
	header := make([]byte, maxLogRecordHeaderSize)

	//recordType <= log.type
	header[4] = byte(log.Type)
	var index = 5
	//這裏存儲key，value的長度信息
	index += binary.PutVarint(header[index:], int64(len(log.Key)))
	index += binary.PutVarint(header[index:], int64(len(log.Value)))
	//此時size為實際log大小
	var size = index + len(log.Key) + len(log.Value)

	encBytes := make([]byte, size)

	//將header,key，value都放入該字節數組中
	copy(encBytes[:index], header[:index])
	copy(encBytes[index:], log.Key)
	copy(encBytes[index+len(log.Key):], log.Value)

	//除了前面四個字節，其餘用於crc
	crc := crc32.ChecksumIEEE(encBytes[4:])

	//這裏要百度一下
	binary.LittleEndian.PutUint32(encBytes[:4], crc)

	return encBytes, int64(size)
}

// 对LogRecordHeader进行解码，返回字节数组以及长度
func decodeLogRecordHeader(buf []byte) (*LogRecordHeader, int64) {
	//返回＜crc
	if len(buf) <= 4 {
		return nil, 0

	}
	header := &LogRecordHeader{
		crc:        binary.LittleEndian.Uint32(buf[:4]),
		recordType: LogRecordType(buf[4]),
	}

	var index = 5
	//取出keysize
	keySize, n := binary.Varint(buf[index:])
	header.keySize = uint32(keySize)
	index += n

	valueSize, n := binary.Varint(buf[index:])
	header.valueSize = uint32(valueSize)
	index += n

	return header, int64(index)
}

// 通过Header和LogRecord制作CRC
func getLogRecordCRC(lr *LogRecord, header []byte) uint32 {
	if lr == nil {
		return 0
	}

	crc := crc32.ChecksumIEEE(header[:])
	crc = crc32.Update(crc, crc32.IEEETable, lr.Key)
	crc = crc32.Update(crc, crc32.IEEETable, lr.Value)

	return crc
}
