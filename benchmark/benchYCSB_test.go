package benchmark

import (
	bitcask "bitcask-go"
	"context"
	"fmt"
	"log"
	"os"

	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

type BitcaskDB struct {
	db *bitcask.DB
}
type BitcaskCreator struct {
}

func (b BitcaskCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	opts := bitcask.DefaultDBOptions
	dir, _ := os.MkdirTemp("", "bitcask-go")
	opts.DirPath = dir
	var err error
	db, err := bitcask.Open(opts)
	return &BitcaskDB{db: db}, err

}

func (db *BitcaskDB) Close() error {
	return db.db.Close()
}

func (db *BitcaskDB) InitThread(ctx context.Context, threadID int, threadCount int) context.Context {
	log.Printf("Thread %d initialization complete.", threadID)
	// 直接返回传入的上下文，无需修改
	return ctx
}
func (db *BitcaskDB) CleanupThread(ctx context.Context) {
	log.Println("Cleaning up thread resources.")
	// 如果有线程特定的资源或连接需要关闭，可以在这里执行
}

func (db *BitcaskDB) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	value, err := db.db.Get([]byte(key))
	if err != nil {
		return nil, err
	}

	result := map[string][]byte{"value": value}
	return result, nil
}
func (db *BitcaskDB) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	opts := bitcask.DefaultIterOptions

	it := db.db.NewIterator(opts)
	it.Seek([]byte(startKey))

	var results []map[string][]byte
	for it.Valid() && len(results) < count {
		// 获取当前迭代器的键和值
		// key := string(it.Key())
		value, err := it.Value() // 获取当前键对应的值
		if err != nil {
			// 如果存在错误，关闭迭代器并返回错误
			it.Close()
			return nil, err
		}

		record := map[string][]byte{"value": value}

		results = append(results, record)
		// 移动迭代器到下一个记录
		it.Next()
	}
	// 关闭迭代器
	it.Close()

	return results, nil
}

func (db *BitcaskDB) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	// 检查 values 是否包含 "value" 字段的更新
	if value, ok := values["value"]; ok {
		// 使用 Bitcask 的 Put 方法来更新键对应的 "value" 字段
		// 假设 db.db 是 *bitcask.DB 类型
		return db.db.Put([]byte(key), value)
	} else {
		// 如果 values 不包含 "value" 字段，返回错误或忽略更新
		return fmt.Errorf("no value field provided for update")
	}
}

func (db *BitcaskDB) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	// 与 Update 方法类似，我们假设 Insert 方法也只处理 "value" 字段的插入。
	// 如果 values 包含多个字段，我们将只考虑 "value" 字段。

	// 检查 values 是否包含 "value" 字段
	if value, ok := values["value"]; ok {
		// 使用 Bitcask 的 Put 方法来插入新的键值对
		// 假设 db.db 是 *bitcask.DB 类型
		return db.db.Put([]byte(key), value)
	} else {
		// 如果 values 不包含 "value" 字段，返回错误，因为没有可插入的值
		return fmt.Errorf("no 'value' field provided for insert")
	}
}
func (db *BitcaskDB) Delete(ctx context.Context, table string, key string) error {
	return db.db.Delete([]byte(key))
}

func init() {
	ycsb.RegisterDBCreator("BitcaskCreator", BitcaskCreator{})
}

//TODO实现完接口还要实现测试方式
