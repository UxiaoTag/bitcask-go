package fio

import (
	"os"

	"golang.org/x/exp/mmap"
)

// MMap io 内存文件映射,这里仅用于加载数据库，所以只用了只读
type MMap struct {
	readerAt *mmap.ReaderAt
}

// 初始化MMap
func NewMMapIOManager(filename string) (*MMap, error) {
	_, err := os.OpenFile(filename, os.O_CREATE, DatafilePerm)
	if err != nil {
		return nil, err
	}
	readerAt, err := mmap.Open(filename)
	if err != nil {
		return nil, err
	}
	return &MMap{readerAt: readerAt}, nil
}

// 从文件的给定位置读取对应数据
func (mmap *MMap) Read(buf []byte, offset int64) (int, error) {
	return mmap.readerAt.ReadAt(buf, offset)
}

// 写入字节到文件中
func (mmap *MMap) Write([]byte) (int, error) {
	panic("this mmap can't be write")
}

// Sync持久化数据
func (mmap *MMap) Sync() error {
	panic("this mmap can't be write")
}

// Close关闭IO
func (mmap *MMap) Close() error {
	return mmap.readerAt.Close()
}

// Size获取文件大小
func (mmap *MMap) Size() (int64, error) {
	return int64(mmap.readerAt.Len()), nil
}
