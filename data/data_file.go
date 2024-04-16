package data

import (
	"bitcask-go/fio"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"path/filepath"
)

var (
	ErrInvalidCRC = errors.New("invalid crc value,log record maybe corrupted")
)

const (
	DataFileNameSuffix = ".data"
	HintFileName       = "hint-index"
	MergeFinishedName  = "merge-finshed"
)

// 数据文件
type DataFile struct {
	FileId   uint32        //文件id
	Offset   int64         //文件偏移
	IoManger fio.IOManager //io读写管理
}

// 打开新的数据文件
func OpenDataFile(dirPath string, fileId uint32) (*DataFile, error) {
	filename := GetDataFileName(dirPath, fileId)
	return newDataFile(filename, fileId)
}

// 打开Hint索引文件
func OpenHintFile(dirPath string) (*DataFile, error) {
	filename := filepath.Join(dirPath, HintFileName)
	return newDataFile(filename, 0)
}

// 打开Merge完成索引文件
func OpenMergeFinishFile(dirPath string) (*DataFile, error) {
	filename := filepath.Join(dirPath, MergeFinishedName)
	return newDataFile(filename, 0)
}

// 获取数据文件名
func GetDataFileName(dirPath string, fileId uint32) string {
	return filepath.Join(dirPath, fmt.Sprintf("%09d", fileId)+DataFileNameSuffix)
}

// 打开新文件
func newDataFile(dirPath string, fileId uint32) (*DataFile, error) {
	//初始化IOManager管理器
	ioManager, err := fio.NewIOManager(dirPath)
	if err != nil {
		return nil, err
	}
	return &DataFile{
		FileId:   fileId,
		Offset:   0,
		IoManger: ioManager,
	}, nil
}

// Sync持久化当前数据文件到磁盘
func (df *DataFile) Sync() error {
	return df.IoManger.Sync()
}

// 写入字节到文件中
func (df *DataFile) Write(data []byte) error {
	byteSize, err := df.IoManger.Write(data)
	if err != nil {
		return err
	}
	df.Offset += int64(byteSize)
	return nil
}

// 写入索引信息进入Hint索引文件
func (df *DataFile) WriteHintRecord(key []byte, pos *LogRecordPos) error {
	record := &LogRecord{
		Key:   key,
		Value: EncodeLogRecordPos(pos),
	}
	encRecord, _ := EncodeLogRecord(record)
	return df.Write(encRecord)
}

// 读取文件中的LogRecord,返回LogRecord,recordSize,error
func (df *DataFile) ReadLogRecord(offset int64) (*LogRecord, int64, error) {
	filesize, err := df.IoManger.Size()
	if err != nil {
		return nil, 0, err
	}

	//这种情况判断最大长度header+offset超过文件长度，读到末尾即可
	var headerBytes int64 = maxLogRecordHeaderSize
	if offset+maxLogRecordHeaderSize > filesize {
		headerBytes = filesize - offset
	}

	//读取header信息
	headerBuf, err := df.readNBytes(headerBytes, offset)
	if err != nil {
		return nil, 0, err
	}

	header, headerSize := decodeLogRecordHeader(headerBuf)

	//下面俩个条件表示读取到了末尾，返回EOF
	if header == nil {
		return nil, 0, io.EOF
	}
	if header.crc == 0 && header.keySize == 0 && header.valueSize == 0 {
		return nil, 0, io.EOF
	}

	// 取出key value长度
	keySize, valueSize := int64(header.keySize), int64(header.valueSize)
	var recordSize = headerSize + keySize + valueSize

	log := &LogRecord{}

	//取出type
	log.Type = header.recordType

	//开始读取用户实际存储的key/value
	if keySize > 0 || valueSize > 0 {
		kvBuf, err := df.readNBytes(keySize+valueSize, offset+headerSize)
		if err != nil {
			return nil, 0, err
		}
		//将key，value提取
		log.Key = kvBuf[:keySize]
		log.Value = kvBuf[keySize:]
	}

	//校验crc
	//这里只截取从crc之后到header总长度之前的数据
	crc := getLogRecordCRC(log, headerBuf[crc32.Size:headerSize])
	if crc != header.crc {
		return nil, 0, ErrInvalidCRC
	}

	return log, recordSize, nil

}

// 关闭数据文件
func (df *DataFile) Close() error {
	return df.IoManger.Close()
}

// 读取文件中部分字节，该方法内部使用
func (df *DataFile) readNBytes(n, offset int64) (b []byte, err error) {
	b = make([]byte, n)
	_, err = df.IoManger.Read(b, offset)
	return
}
