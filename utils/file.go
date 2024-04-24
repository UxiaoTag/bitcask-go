package utils

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// 复制路径方法，输入源路径，copy路径，排除文件
func CopyDir(src, dest string, exclude []string) error {

	if _, err := os.Stat(dest); os.IsNotExist(err) {
		if err := os.MkdirAll(dest, os.ModePerm); err != nil {
			return err
		}
	}

	//遍历目录
	return filepath.Walk(src, func(path string, info fs.FileInfo, err error) error {
		filename := strings.Replace(path, src, "", 1)
		if filename == "" {
			return nil
		}
		//过滤屏蔽，若是屏蔽内的直接过滤
		for _, e := range exclude {
			matched, err := filepath.Match(e, info.Name())
			if err != nil {
				return err
			}
			if matched {
				return nil
			}
		}
		//是目录就创建目录
		if info.IsDir() {
			return os.MkdirAll(filepath.Join(dest, filename), info.Mode())
		}

		//是文件就读取然后写到另一个文件上
		data, err := os.ReadFile(filepath.Join(src, filename))
		if err != nil {
			return err
		}
		return os.WriteFile(filepath.Join(dest, filename), data, info.Mode())
	})
}
