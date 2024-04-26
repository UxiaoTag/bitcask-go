package main

import (
	bitcask "bitcask-go"
	bitcask_redis "bitcask-go/redis"
	"log"
	"sync"

	"github.com/tidwall/redcon"
)

const addr = "127.0.0.1:6969"

type BitcaskServer struct {
	dbs    map[int]*bitcask_redis.RedisDataStructure
	server *redcon.Server
	mu     sync.RWMutex
}

func main() {
	//打开redis数据结构服务
	redisDataStructure, err := bitcask_redis.NewRedisDataStructure(bitcask.DefaultDBOptions)
	if err != nil {
		panic(err)
	}

	//初始化BitcaskServer
	bitcaskServer := &BitcaskServer{
		dbs: make(map[int]*bitcask_redis.RedisDataStructure),
	}

	bitcaskServer.dbs[0] = redisDataStructure

	//初始化一个redis服务器
	bitcaskServer.server = redcon.NewServer(addr, execClientCommand, bitcaskServer.accept, bitcaskServer.close)
	bitcaskServer.listen()
}

func (svr *BitcaskServer) listen() {
	log.Println("bitcask Server running,ready to accept connections.")
	_ = svr.server.ListenAndServe()
}

func (svr *BitcaskServer) accept(conn redcon.Conn) bool {
	client := new(BitcaskClient)
	svr.mu.Lock()
	defer svr.mu.Unlock()

	client.server = svr
	db := svr.dbs[0]
	//TODO 给数据库加个Dead参数，判断数据库
	// var err error
	// if db == nil||db IsDead{
	// 	db, err = bitcask_redis.NewRedisDataStructure(bitcask.DefaultDBOptions)
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// }
	client.db = db
	conn.SetContext(client)
	return true
}

func (svr *BitcaskServer) close(conn redcon.Conn, err error) {
	//这里客户端退出不应该关闭数据库，或关闭数据库需要在另一边打开
	// for _, db := range svr.dbs {
	// 	_ = db.Close()
	// }
}
