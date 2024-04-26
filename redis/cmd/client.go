package main

import (
	bitcask "bitcask-go"
	bitcask_redis "bitcask-go/redis"
	"bitcask-go/utils"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/tidwall/redcon"
)

func newWrongNumberOfArgsError(cmd string) error {
	return fmt.Errorf("ERR wrong number of arguments for '%s' command", cmd)
}

type cmdHandler func(cli *BitcaskClient, args [][]byte) (interface{}, error)

var supportedCommands = map[string]cmdHandler{
	"set":   set,
	"get":   get,
	"hset":  hset,
	"sadd":  sadd,
	"lpush": lpush,
	"zadd":  zadd,
}

type BitcaskClient struct {
	server *BitcaskServer
	db     *bitcask_redis.RedisDataStructure
}

func execClientCommand(conn redcon.Conn, cmd redcon.Command) {
	command := strings.ToLower(string(cmd.Args[0]))

	client, _ := conn.Context().(*BitcaskClient)
	switch command {
	//上面负责常规命令
	case "quit":
		_ = conn.Close()
	case "ping":
		conn.WriteString("PONG")
	//TODO补全其他命令
	default:
		cmdFunc, ok := supportedCommands[command]
		if !ok {
			conn.WriteError("Err unsupported command:'" + command + "'")
			return
		}
		res, err := cmdFunc(client, cmd.Args[1:])
		if err != nil {
			if err == bitcask.ErrKeyNotFound {
				conn.WriteNull()
			} else {
				conn.WriteError(err.Error())
			}
			return
		}
		conn.WriteAny(res)
	}
}

func set(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) < 2 {
		return nil, newWrongNumberOfArgsError("set")
	}

	//set a 100 10
	//a 100 10
	key, value := args[0], args[1]

	var ttlDuration time.Duration = 0
	if len(args) == 3 && args[2] != nil {
		ttlStr := args[2]
		ttlSeconds, err := strconv.ParseInt(string(ttlStr), 10, 64)

		if err != nil || ttlSeconds <= 0 {
			// 如果转换失败或不是正数，则默认没有过期时间
		} else {
			//不然就正常转换
			ttlDuration = time.Duration(ttlSeconds) * time.Second
		}
	}

	if err := cli.db.Set(key, ttlDuration, value); err != nil {
		return nil, err
	}

	return redcon.SimpleString("OK"), nil
}

func get(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, newWrongNumberOfArgsError("get")
	}

	value, err := cli.db.Get(args[0])

	if err != nil {
		return nil, err
	}

	//个人洁癖，因为实现之后会出现get timeout没报错从而返回空字符串，
	//eg:put a 10 20 -> 20s后 get a 返回"" 你去get b返回时(nil)很难受。
	if value == nil {
		return nil, nil
	}
	return value, nil
}

func hset(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 3 {
		return nil, newWrongNumberOfArgsError("hset")
	}

	var ok = 0
	key, field, value := args[0], args[1], args[2]
	res, err := cli.db.HSet(key, field, value)
	if err != nil {
		return nil, err
	}
	if res {
		ok = 1
	}
	return redcon.SimpleInt(ok), nil
}

func sadd(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumberOfArgsError("sadd")
	}

	var ok = 0
	key, member := args[0], args[1]
	res, err := cli.db.SAdd(key, member)
	if err != nil {
		return nil, err
	}
	if res {
		ok = 1
	}
	return redcon.SimpleInt(ok), nil
}

func lpush(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumberOfArgsError("lpush")
	}

	key, value := args[0], args[1]
	res, err := cli.db.LPush(key, value)
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(res), nil
}

func zadd(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 3 {
		return nil, newWrongNumberOfArgsError("zadd")
	}

	var ok = 0
	key, score, member := args[0], args[1], args[2]
	res, err := cli.db.ZAdd(key, utils.FloatFromBytes(score), member)
	if err != nil {
		return nil, err
	}
	if res {
		ok = 1
	}
	return redcon.SimpleInt(ok), nil
}
