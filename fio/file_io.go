package fio

import "os"

//标准系统文件IO
type FileIO struct {
	fd *os.File //系统文件描述符
}

//初始化文件IO
func NewFileIOManager(path string) (*FileIO, error) {
	fd, err := os.OpenFile(
		path,
		os.O_CREATE|os.O_RDWR|os.O_APPEND,
		DatafilePerm,
	)
	if err != nil {
		return nil, err
	}
	return &FileIO{fd: fd}, nil
}

// 从文件的给定位置读取对应数据
func (fio *FileIO) Read(data []byte, off int64) (int, error) {
	return fio.fd.ReadAt(data, off)
}

// 写入字节到文件中
func (fio *FileIO) Write(data []byte) (int, error) {
	return fio.fd.Write(data)
}

// Sync持久化数据
func (fio *FileIO) Sync() error {
	return fio.fd.Sync()
}

// Close关闭IO
func (fio *FileIO) Close() error {
	return fio.fd.Close()
}
