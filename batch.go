package bitcask_go

import (
	"bitcask-go/data"
	"encoding/binary"
	"sync"
	"sync/atomic"
)

// 无事务SeqNo
const nonTransactionSeqNo uint64 = 0

// 事务完成专用key
var txnFinKey = []byte("txn-fin")

// 原子批量写数据，保证原子性
type WriteBatch struct {
	mu           *sync.Mutex
	db           *DB
	pendingWrite map[string]*data.LogRecord //暂存的数据
	options      WriteBatchOptions          //配置项
}

func (db *DB) NewWriteBatch(options WriteBatchOptions) *WriteBatch {
	//只有b+tree，不是第一次加载，没有seqNo的文件
	if db.options.IndexType == BPTree && !db.seqNoFileExists && !db.isInitial {
		panic("cannot use write batch,seq no file not exists")
	}
	return &WriteBatch{
		mu:           new(sync.Mutex),
		db:           db,
		pendingWrite: make(map[string]*data.LogRecord),
		options:      options,
	}
}

// 放置数据
func (wb *WriteBatch) Put(key, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	wb.mu.Lock()
	defer wb.mu.Unlock()

	//暂存logRecord
	logRecord := &data.LogRecord{Key: key, Value: value}
	wb.pendingWrite[string(key)] = logRecord
	return nil
}

// 删除数据
func (wb *WriteBatch) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	wb.mu.Lock()
	defer wb.mu.Unlock()

	//如果数据已经删除了或者不存在没必要
	index := wb.db.index.Get(key)
	if index == nil {
		if wb.pendingWrite[string(key)] != nil {
			delete(wb.pendingWrite, string(key))
		}
		return nil
	}

	//暂存logRecord
	logRecord := &data.LogRecord{Key: key, Type: data.LogRecordDelete}
	wb.pendingWrite[string(key)] = logRecord

	return nil
}

// 提交事务
func (wb *WriteBatch) Commit() error {
	wb.mu.Lock()
	defer wb.mu.Unlock()

	if len(wb.pendingWrite) == 0 {
		return nil
	}
	//超过了配置的最大提交数据量
	if uint(len(wb.pendingWrite)) > wb.options.MaxBatchNum {
		return ErrExceedMaxBatchNum
	}

	wb.db.mu.Lock()
	defer wb.db.mu.Unlock()

	//获取当前的事务序列号
	//atomic.AddUint64 函数用于执行原子操作+1 eg:wb.db.seqNo=1 atomic.AddUint64(&wb.db.seqNo, 1)-> seqNO=2 wb.db.seqNo=2
	seqNO := atomic.AddUint64(&wb.db.seqNo, 1)

	//暂存内存索引
	pos := make(map[string]*data.LogRecordPos)

	//取得事务队列号开始取数据
	for _, record := range wb.pendingWrite {
		logRecordPos, err := wb.db.appendLogRecord(&data.LogRecord{
			Key:   logRecordKeyAddSeq(record.Key, seqNO),
			Value: record.Value,
			Type:  record.Type,
		})
		if err != nil {
			return err
		}
		pos[string(record.Key)] = logRecordPos
	}

	//add 一条标识事务完成的消息
	finishedRecord := &data.LogRecord{
		Key:  logRecordKeyAddSeq(txnFinKey, seqNO),
		Type: data.LogRecordTxnFinished,
	}
	//当这条数据插入，才能代表事务完成
	if _, err := wb.db.appendLogRecord(finishedRecord); err != nil {
		return err
	}

	//根据配置持久化
	if wb.options.SyncWrites {
		//wb.db.Sync()不用这个是因为这个也带锁，会死锁，所以直接用wb.db.activeFile.Sync()
		if err := wb.db.activeFile.Sync(); err != nil {
			return err
		}
	}

	//更新内存索引
	for _, record := range wb.pendingWrite {
		reocrdPos := pos[string(record.Key)]
		var oldPos *data.LogRecordPos
		if record.Type == data.LogRecordDelete {
			oldPos, _ = wb.db.index.Delete(record.Key)

		}
		if record.Type == data.LogRecordNormal {
			oldPos = wb.db.index.Put(record.Key, reocrdPos)
		}

		if oldPos != nil {
			wb.db.reclaimSize += int64(oldPos.Size)
		}
	}
	wb.pendingWrite = make(map[string]*data.LogRecord)
	return nil
}

// key+Seq 编码 最后变成SeqKey
func logRecordKeyAddSeq(key []byte, seqNo uint64) []byte {
	seq := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(seq[:], seqNo)

	encKey := make([]byte, n+len(key))
	copy(encKey[:n], seq[:n])
	copy(encKey[n:], key)

	return encKey
}

// key+Seq 解码 最后返回Seq Key
func parseLogRecordKey(key []byte) ([]byte, uint64) {
	seq, n := binary.Uvarint(key)

	// decKey := make([]byte, len(key)-n)
	// copy(key[n:], decKey)
	decKey := key[n:]
	return decKey, seq
}
