package data

//写入到数据文件的日志记录
type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType
}

type LogRecordType uint16

const (
	//正常操作
	LogRecordNormal LogRecordType = iota
	//删除操作
	LogRecordDelete
)

//索引的数据结构，主要描述数据在磁盘的位置
type LogRecordPos struct {
	Fid    uint32 //文件id，表示数据存放到文件的哪个位置
	Offset int64  //表示偏移量
}

//对LogRecord进行编码，返回字节数组以及长度TODO
func EncodeLogRecord(log *LogRecord) ([]byte, int64) {
	return nil, 0
}
