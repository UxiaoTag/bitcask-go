package data

import "bitcask-go/fio"

const DataFileNameSuffix = ".data"

//数据文件
type DataFile struct {
	FileId   uint32        //文件id
	Offset   int64         //文件偏移
	IoManger fio.IOManager //io读写管理
}

//打开新的数据文件TODO
func OpenDataFile(dirPath string, fileId uint32) (*DataFile, error) {
	return nil, nil
}

//Sync持久化当前数据文件到磁盘TODO
func (df *DataFile) Sync() error {
	return nil
}

// 写入字节到文件中TODO
func (df *DataFile) Write(data []byte) error {
	return nil
}

//读取文件中的字节段TODO
func (df *DataFile) ReadLogRecord(Offset int64) (*LogRecord, int64, error) {
	return nil, 0, nil
}
