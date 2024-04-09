package data

import "encoding/binary"

type LogRecordType byte

const (
	//正常操作
	LogRecordNormal LogRecordType = iota
	//删除操作
	LogRecordDelete
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

//写入到数据文件的日志头部
type LogRecordHeader struct {
	crc        uint32        //头部校验
	recordType LogRecordType //操作类型
	keySize    uint32        //key长度
	valueSize  uint32        //value长度
}

// 对LogRecord进行编码，返回字节数组以及长度TODO
func EncodeLogRecord(log *LogRecord) ([]byte, int64) {
	return nil, 0
}

// 对LogRecordHeader进行解码，返回字节数组以及长度TODO
func decodeLogRecordHeader(buf []byte) (*LogRecordHeader, int64) {
	return nil, 0
}

//通过Header和LogRecord制作CRC TODO
func getLogRecordCRC(lr *LogRecord, header []byte) uint32 {
	return 0
}
