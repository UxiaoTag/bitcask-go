//go:build !windows
// +build !windows

package bitcask_go

import (
	"io/fs"
	"os"
	"path/filepath"
	"syscall"
)

type Stat struct {
	KeyNum          uint  //key的数量
	DataFileNum     uint  //数据文件的数量
	ReclaimableSize int64 //磁盘可回收字节空间，单位为字节
	DiskSize        int64 //所占磁盘空间
}

// 获取一个目录的占用大小
func DirSize(dirPath string) (int64, error) {
	var size int64
	err := filepath.Walk(dirPath, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	return size, err
}

// 获取磁盘可用的空间
func AvailableDiskSize() (uint64, error) {
	wd, err := os.Getwd()
	if err != nil {
		return 0, err
	}
	// 此代码无法在windows上编译
	var stat syscall.Statfs_t
	if err = syscall.Statfs(wd, &stat); err != nil {
		return 0, err
	}
	return stat.Bavail * uint64(stat.Bsize), nil

}
