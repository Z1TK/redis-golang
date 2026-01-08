package main

import (
	"fmt"
	"strconv"
	"sync"
	"time"
)

type DataType struct {
	Strings map[string]string
	Lists map[string][]string
	Hashes map[string]map[string]string
	ExpireTime map[string]time.Time
	Mu sync.RWMutex
}

func createDT() *DataType {
	return &DataType{
		Strings: make(map[string]string),
		Lists: make(map[string][]string),
		Hashes: make(map[string]map[string]string),
		ExpireTime: make(map[string]time.Time),
	}
}

var Handlers = map[string]func(*DataType, []Value) Value {
	"PING": ping, // connection commands //
	"SET": set, // string commands //
	"GET": get,
	"SETNX": setnx,
	"SETEX": setex,
	"GETEX": getex,
	"STRLEN": strlen,
	"GETRANGE": getrange,
	"MSET": mset,
	"MGET": mget,
	"INCR": incr,
	"DECR": decr,
	"HSET": hset, // hash commands //
	"HGET": hget,
	"HDEL": hdel,
	"HEXISTS": hexists,
	"HMGET": hmget,
	"HGETALL": hgetall,
	"HLEN":  hlen,
	"HKEYS": hkeys,
	"HVALS": hvals,
	"RPUSH": rpush, // list commands //
	"LPUSH": lpush,
	"RPOP": rpop,
	"LPOP": lpop,
	"LRANGE": lrange,
	"LPUSHX": lpushx,
	"RPUSHX": rpushx,
	"LLEN": llen,
	"DEL": del, // generic commands //
	"EXPIRE": expire,
	"TTL": ttl,
}

// helpers //
func checkExpireTime(dt *DataType, key string) bool {
	if _, exist := dt.ExpireTime[key]; !exist {
		return false
	}

	if time.Now().After(dt.ExpireTime[key]) {
		delete(dt.Strings, key)
		delete(dt.Lists, key)
		delete(dt.Hashes, key)
		delete(dt.ExpireTime, key)

		return true
	}

	return false
}

// CONECTION COMMANDS //
func ping(_ *DataType, args []Value) Value {
	if len(args) == 0 {
		return Value{typ: "string", str: "PONG"}
	}

	return Value{typ: "string", str: args[0].bulk}
}

// STRING COMMANDS //
func set(dt *DataType, args []Value) Value {
	if len(args) != 2 {
		return Value{typ: "error", str: "wrong number of arguments for 'set' command"}
	}

	key := args[0].bulk
	val := args[1].bulk

	dt.Mu.Lock()
	defer dt.Mu.Unlock()

	dt.Strings[key] = val

	return Value{typ: "string", str: "OK"}
}

func get(dt *DataType, args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "error", str: "wrong number of arguments for 'get' command"}
	}

	key := args[0].bulk

	dt.Mu.RLock()
	defer dt.Mu.RUnlock()

	val, ok := dt.Strings[key]
	if !ok {
		return Value{typ: "null"}
	}

	if checkExpireTime(dt, key) {
		return Value{typ: "null"}
	}

	return Value{typ: "bulk", bulk: val}
}

func setnx(dt *DataType, args []Value) Value {
	if len(args) != 2 {
		return Value{typ: "error", str: "wrong number of arguments for 'set' command"}
	}

	key := args[0].bulk
	val := args[1].bulk

	dt.Mu.Lock()
	defer dt.Mu.Unlock()

	if _, exist := dt.Strings[key]; !exist {
		dt.Strings[key] = val
		return Value{typ: "integer", num: 1}
	}

	return Value{typ: "integer", num: 0}
}

func setex(dt *DataType, args []Value) Value {
	if len(args) != 3 {
		return Value{typ: "error", str: "wrong number of arguments for 'setex' command"}
	}

	key := args[0].bulk
	t, err := strconv.Atoi(args[1].bulk)	
	if err != nil {
		return Value{typ: "error", str: "value is not an integer or out of range"}
	}
	val := args[2].bulk

	dt.Mu.Lock()
	defer dt.Mu.Unlock()

	dt.Strings[key] = val
	dt.ExpireTime[key] = time.Now().Add(time.Duration(t) * time.Second)

	return Value{typ: "string", str: "OK"}
}

func getex(dt *DataType, args []Value) Value {
	if len(args) < 1 || len(args) > 3 {
		return Value{typ: "error", str: "wrong number of arguments for 'get' command"}
	}

	key := args[0].bulk

	dt.Mu.RLock()
	defer dt.Mu.RUnlock()

	val, ok := dt.Strings[key]
	if !ok {
		return Value{typ: "null"}
	}

	if checkExpireTime(dt, key) {
		return Value{typ: "null"}
	}

	if len(args) == 3 {
		t, err := strconv.Atoi(args[2].bulk)
		if err != nil {
			return Value{typ: "error", str: "value is not an integer or out of range"}
		}

		dt.ExpireTime[key] = time.Now().Add(time.Duration(t) * time.Second)
	}

	return Value{typ: "bulk", bulk: val}
}

func strlen(dt *DataType, args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "error", str: "wrong number of arguments for 'strlen' command"}
	}

	key := args[0].bulk

	dt.Mu.Lock()
	defer dt.Mu.Unlock()

	val, ok := dt.Strings[key]
	if !ok {
		return Value{typ: "integer", num: 0}
	}

	if checkExpireTime(dt, key) {
		return Value{typ: "integer", num: 0}
	}

	return Value{typ: "integer", num: len(val)}
}

func getrange(dt *DataType, args []Value) Value {
	if len(args) != 3 {
		return Value{typ: "error", str: "wrong number of arguments for 'getrange' command"}
	}

	key := args[0].bulk
	startInt, err := strconv.Atoi(args[1].bulk)
	if err != nil {
		return Value{typ: "error", str: "value is not an integer or out of range"}
	}
	endInt, err := strconv.Atoi(args[2].bulk)
	if err != nil {
		return Value{typ: "error", str: "value is not an integer or out of range"}
	}

	dt.Mu.RLock()
	defer dt.Mu.RUnlock()

	data, exist := dt.Strings[key]
	if len(data) == 0 || !exist || checkExpireTime(dt, key){
		return Value{typ: "array", array: []Value{}}
	}
	length := len(data) 

	if startInt < 0 {
		startInt = length + startInt
	}

	if startInt < 0 {
		startInt = 0
	}

	if endInt < 0 {
		endInt = length + endInt
	}

	if startInt >= endInt {
		return Value{typ: "array", array: []Value{}}
	}

	if endInt >= length {
		endInt = length - 1
	}

	val := dt.Strings[key][startInt:endInt + 1]

	return Value{typ: "bulk", bulk: val}
}

func mset(dt *DataType, args []Value) Value {
	if len(args) % 2 != 0 {
		return Value{typ: "error", str: "wrong number of arguments for 'mset' command"}
	}

	dt.Mu.Lock()
	defer dt.Mu.Unlock()

	for i := 0; i < len(args); i += 2 {
		key := args[i].bulk
		val := args[i+1].bulk
		dt.Strings[key] = val
	}

	return Value{typ: "string", str: "OK"}
}

func mget(dt *DataType, args []Value) Value {
	if len(args) < 1 {
		return Value{typ: "error", str: "wrong number of arguments for 'mget' command"}
	}

	var res []Value
	dt.Mu.RLock()
	defer dt.Mu.RUnlock()

	for i := 0; i < len(args); i += 1 {
		key := args[i].bulk
		
		if checkExpireTime(dt, key) {
			return Value{typ: "null"}
		}
		
		if val, exist := dt.Strings[key]; exist {
			res = append(res, Value{typ: "bulk", bulk: val})
		} else {
			res = append(res, Value{typ: "null"})
		}
	}

	return Value{typ: "array", array: res}
}

func incr(dt *DataType, args []Value) Value {
	if len(args) != 1  {
		return Value{typ: "error", str: "wrong number of arguments for 'incr' command"}
	}

	key := args[0].bulk

	dt.Mu.Lock()
	defer dt.Mu.Unlock()

	if _, exist := dt.Strings[key]; !exist || checkExpireTime(dt, key) {
		dt.Strings[key] = "1"
		return Value{typ: "integer", num: 1}
	}

	n, err := strconv.Atoi(dt.Strings[key])
	if err != nil {
		return Value{typ: "error", str: "value is not an integer or out of range"}
	}
	n++

	dt.Strings[key] = strconv.Itoa(n)

	return Value{typ: "integer", num: n}
}

func decr(dt *DataType, args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "error", str: "wrong number of arguments for 'decr' command"}
	}

	key := args[0].bulk

	dt.Mu.Lock()
	defer dt.Mu.Unlock()

	if _, exist := dt.Strings[key]; !exist || checkExpireTime(dt, key) {
		dt.Strings[key] = "-1"
		return Value{typ: "integer", num: -1}
	}

	n, err := strconv.Atoi(dt.Strings[key])
	if err != nil {
		return Value{typ: "error", str: "value is not an integer or out of range"}
	}
	n--

	dt.Strings[key] = strconv.Itoa(n)

	return Value{typ: "integer", num: n}
}

// HASH COMMAND //
func hset(dt *DataType, args []Value) Value {
	if len(args) < 3 {
		return Value{typ: "error", str: "wrong number of arguments for 'hset' command"}
	}

	var n int
	hash := args[0].bulk

	dt.Mu.Lock()
	defer dt.Mu.Unlock()

	if dt.Hashes[hash] == nil {
		dt.Hashes[hash] = make(map[string]string)
	}

	if len(args) % 2 != 0 {
		for i := 1; i < len(args); i += 2 {
			key := args[i].bulk
			val := args[i + 1].bulk
			dt.Hashes[hash][key] = val
			n++
		}
	} else {
		return Value{typ: "error", str: "wrong number of arguments for 'hset' command"}
	}

	return Value{typ: "integer", num: n}
}

func hget(dt *DataType, args []Value) Value {
	if len(args) != 2 {
		return Value{typ: "error", str: "wrong number of arguments for 'hget' command"}
	}

	hash := args[0].bulk
	key := args[1].bulk

	dt.Mu.RLock()
	defer dt.Mu.RUnlock()

	if checkExpireTime(dt, hash) {
		return Value{typ: "null"}
	}

	val, exist := dt.Hashes[hash][key]; 
	if !exist {
		return Value{typ: "null"}
	}

	return Value{typ: "bulk", bulk: val}
}

func hdel(dt *DataType, args []Value) Value {
	if len(args) < 2 {
		return Value{typ: "error", str: "wrong number of arguments for 'hget' command"}
	}

	var n int
	hash := args[0].bulk
	key := args[1].bulk

	if _, exist := dt.Hashes[hash][key]; !exist {
		return Value{typ: "integer", num: 0}
	}

	dt.Mu.Lock()
	defer dt.Mu.Unlock()

	if dt.Hashes[hash] == nil {
		return Value{typ: "integer", num: 0}
	}

	for i := 1; i < len(args); i++ {
		key := args[i].bulk
		if _, exist := dt.Hashes[hash][key]; exist {
			delete(dt.Hashes[hash], key)
			n++
		}
	}

	return Value{typ: "integer", num: n}
}

func hexists(dt *DataType, args []Value) Value {
	if len(args) < 2 {
		return Value{typ: "error", str: "wrong number of arguments for 'hget' command"}
	}

	hash := args[0].bulk
	key := args[1].bulk

	dt.Mu.RLock()
	defer dt.Mu.RUnlock()

	if checkExpireTime(dt, hash) {
		return Value{typ: "integer", num: 0}
	}

	if _, exist := dt.Hashes[hash][key]; !exist {
		return Value{typ: "integer", num: 0}
	}

	return Value{typ: "integer", num: 1}
}

func hmget(dt *DataType, args []Value) Value {
	if len(args) < 1 {
		return Value{typ: "error", str: "wrong number of arguments for 'hmget' command"}
	}

	var res []Value
	hash := args[0].bulk

	dt.Mu.RLock()
	defer dt.Mu.RUnlock()

	if checkExpireTime(dt, hash) {
			return Value{typ: "null"}
		}

	for i := 1; i < len(args); i++ {
		key := args[i].bulk
		
		if val, exist := dt.Hashes[hash][key]; exist {
			res = append(res, Value{typ: "bulk", bulk: val})
		} else {
			res = append(res, Value{typ: "null"})
		}
	}

	return Value{typ: "array", array: res}
}

func hgetall(dt *DataType, args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "error", str: "wrong number of arguments for 'hgetall' command"}
	}

	var res []Value
	hash := args[0].bulk

	dt.Mu.RLock()
	defer dt.Mu.RUnlock()

	if _, exist := dt.Hashes[hash]; !exist {
		return Value{typ: "array", array: []Value{}}
	}

	if checkExpireTime(dt, hash) {
			return Value{typ: "array", array: []Value{}}
		}

	for key, val := range dt.Hashes[hash] {
		res = append(res, Value{typ: "bulk", bulk: key}, Value{typ: "bulk", bulk: val})
	}

	return Value{typ: "array", array: res}
}

func hlen(dt *DataType, args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "error", str: "wrong number of arguments for 'hlen' command"}
	}

	hash := args[0].bulk

	dt.Mu.Lock()
	defer dt.Mu.Unlock()

	val, ok := dt.Hashes[hash]
	if !ok {
		return Value{typ: "integer", num: 0}
	}

	if checkExpireTime(dt, hash) {
		return Value{typ: "integer", num: 0}
	}

	return Value{typ: "integer", num: len(val)}
}

func hkeys(dt *DataType, args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "error", str: "wrong number of arguments for 'hkeys' command"}
	}

	var res []Value
	hash := args[0].bulk

	dt.Mu.RLock()
	defer dt.Mu.RUnlock()

	if _, exist := dt.Hashes[hash]; !exist {
		return Value{typ: "array", array: []Value{}}
	}

	if checkExpireTime(dt, hash) {
			return Value{typ: "array", array: []Value{}}
		}

	for key := range dt.Hashes[hash] {
		res = append(res, Value{typ: "bulk", bulk: key})
	}

	return Value{typ: "array", array: res}
}

func hvals(dt *DataType, args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "error", str: "wrong number of arguments for 'hvals' command"}
	}

	var res []Value
	hash := args[0].bulk

	dt.Mu.RLock()
	defer dt.Mu.RUnlock()

	if _, exist := dt.Hashes[hash]; !exist {
		return Value{typ: "array", array: []Value{}}
	}

	if checkExpireTime(dt, hash) {
			return Value{typ: "array", array: []Value{}}
		}

	for _, val := range dt.Hashes[hash] {
		res = append(res, Value{typ: "bulk", bulk: val})
	}

	return Value{typ: "array", array: res}
}

// LIST COMMAND //
func rpush(dt *DataType, args []Value) Value {
	if len(args) < 2 {
		return Value{typ: "error", str: "wrong number of arguments for 'rpush' command"}
	}

	key := args[0].bulk
	length := len(args)

	dt.Mu.Lock()
	defer dt.Mu.Unlock()

	if _, exist := dt.Lists[key]; !exist || checkExpireTime(dt, key) {
		dt.Lists[key] = make([]string, 0)
	}

	for i := 1; i < length; i++ {
		dt.Lists[key] = append(dt.Lists[key], args[i].bulk)
	}

	n := len(dt.Lists[key])

	return Value{typ: "integer", num: n}
}

func lpush(dt *DataType, args []Value) Value {
	if len(args) < 2 {
		return Value{typ: "error", str: "wrong number of arguments for 'lpush' command"}
	}

	key := args[0].bulk
	length := len(args)

	dt.Mu.Lock()
	defer dt.Mu.Unlock()

	if _, exist := dt.Lists[key]; !exist || checkExpireTime(dt, key) {
		dt.Lists[key] = make([]string, 0)
	}

	for i := 1; i < length; i++ {
		dt.Lists[key] = append([]string{args[i].bulk}, dt.Lists[key]...)
	}

	n := len(dt.Lists[key])

	return Value{typ: "integer", num: n}
}

func rpop(dt *DataType, args []Value) Value {
	if len(args) < 1 {
		return Value{typ: "error", str: "wrong number of arguments for 'rpop' command"}
	}

	var res []Value
	key := args[0].bulk

	dt.Mu.Lock()
	defer dt.Mu.Unlock()

	data, exist := dt.Lists[key]
	if len(data) == 0 || !exist || checkExpireTime(dt, key) {
		return Value{typ: "null"}
	}
	length := len(data) 

	if len(args) > 1 {
		val, err := strconv.Atoi(args[1].bulk)
		if err != nil || val < 0 {
			return Value{typ: "error", str: "value is out of range, must be positive"}
		}

		if val > length {
			val = length
		}

		for i := 0; i < val; i++ {
			res = append(res, Value{typ: "bulk", bulk: dt.Lists[key][length - 1 - i]})
		}

		dt.Lists[key] = dt.Lists[key][:length - val]	
		return Value{typ: "array", array: res}
	} 

	val := dt.Lists[key][length - 1]
	dt.Lists[key] = dt.Lists[key][:length - 1]
	return Value{typ: "bulk", bulk: val}
}

func lpop(dt *DataType, args []Value) Value {
	if len(args) < 1 {
		return Value{typ: "error", str: "wrong number of arguments for 'lpop' command"}
	}

	var res []Value
	key := args[0].bulk

	dt.Mu.Lock()
	defer dt.Mu.Unlock()



	data, exist := dt.Lists[key]
	if len(data) == 0 || !exist || checkExpireTime(dt, key){
		return Value{typ: "array", array: []Value{}}
	}
	length := len(data) 

	if len(args) > 1 {
		val, err := strconv.Atoi(args[1].bulk)
		if err != nil || val < 0 {
			return Value{typ: "error", str: "value is out of range, must be positive"}
		}

		if val > length {
			val = length
		}

		for i := 0; i < val; i++ {
			res = append(res, Value{typ: "bulk", bulk: dt.Lists[key][i]})
		}

		dt.Lists[key] = dt.Lists[key][val:]
		return Value{typ: "array", array: res}
	}

	val := dt.Lists[key][0]
	dt.Lists[key] = dt.Lists[key][1:]
	return Value{typ: "bulk", bulk: val}
}

func lrange(dt *DataType, args []Value) Value {
	if len(args) != 3 {
		return Value{typ: "error", str: "wrong number of arguments for 'lrange' command"}
	}

	var res []Value
	key := args[0].bulk
	startInt, err := strconv.Atoi(args[1].bulk)
	if err != nil {
		return Value{typ: "error", str: "value is not an integer or out of range"}
	}
	endInt, err := strconv.Atoi(args[2].bulk)
	if err != nil {
		return Value{typ: "error", str: "value is not an integer or out of range"}
	}

	dt.Mu.RLock()
	defer dt.Mu.RUnlock()

	data, exist := dt.Lists[key]
	if len(data) == 0 || !exist || checkExpireTime(dt, key){
		return Value{typ: "array", array: []Value{}}
	}
	length := len(data) 

	if startInt < 0 {
		startInt = length + startInt
	}

	if startInt < 0 {
		startInt = 0
	}

	if endInt < 0 {
		endInt = length + endInt
	}

	if startInt > endInt {
		return Value{typ: "array", array: []Value{}}
	}

	if endInt >= length {
		endInt = length - 1
	}

	val := dt.Lists[key][startInt:endInt + 1]
	for i := 0; i < len(val); i++ {
		res = append(res, Value{typ: "bulk", bulk: val[i]})
	}

	return Value{typ: "array", array: res}
}

func lpushx(dt *DataType, args []Value) Value {
	if len(args) < 2 {
		return Value{typ: "error", str: "wrong number of arguments for 'lpushx' command"}
	}

	key := args[0].bulk
	length := len(args)

	dt.Mu.Lock()
	defer dt.Mu.Unlock()

	if _, exist := dt.Lists[key]; !exist || checkExpireTime(dt, key) {
		return Value{typ: "integer", num: 0}
	}

	for i := 1; i < length; i++ {
		dt.Lists[key] = append([]string{args[i].bulk}, dt.Lists[key]...)
	}

	n := len(dt.Lists[key])

	fmt.Println(dt.Lists[key][0])

	return Value{typ: "integer", num: n}
}

func rpushx(dt *DataType, args []Value) Value {
	if len(args) < 2 {
		return Value{typ: "error", str: "wrong number of arguments for 'rpushx' command"}
	}

	key := args[0].bulk
	length := len(args)

	dt.Mu.Lock()
	defer dt.Mu.Unlock()

	if _, exist := dt.Lists[key]; !exist || checkExpireTime(dt, key) {
		return Value{typ: "integer", num: 0}
	}

	for i := 1; i < length; i++ {
		dt.Lists[key] = append(dt.Lists[key], args[i].bulk)
	}

	n := len(dt.Lists[key])

	return Value{typ: "integer", num: n}
}

func llen(dt *DataType, args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "error", str: "wrong number of arguments for 'llen' command"}
	}

	key := args[0].bulk

	dt.Mu.Lock()
	defer dt.Mu.Unlock()

	val, ok := dt.Lists[key]
	if !ok {
		return Value{typ: "integer", num: 0}
	}

	if checkExpireTime(dt, key) {
		return Value{typ: "integer", num: 0}
	}

	return Value{typ: "integer", num: len(val)}
}

// GENERIC COMMANDS //
func del(dt *DataType, args []Value) Value {
	if len(args) < 1 {
		return Value{typ: "error", str: "wrong number of arguments for 'DEL' command"}
	}

	length := len(args)
	n := 0

	dt.Mu.Lock()
	defer dt.Mu.Unlock()

	for i := 0; i < length; i++ {
		key := args[i].bulk

		if _, exist := dt.Strings[key]; exist {
			delete(dt.Strings, key)
			n++
		}

		if _, exist := dt.Lists[key]; exist {
			delete(dt.Lists, key)
			n++
		}

		if _, exist := dt.Hashes[key]; exist {
			delete(dt.Hashes, key)
			n++
		}

		delete(dt.ExpireTime, key)
	}

	return Value{typ: "integer", num: n}
}

func expire(dt *DataType, args []Value) Value {
	if len(args) < 2 {
		return Value{typ: "error", str: "wrong number of arguments for 'expire' command"}
	}

	key := args[0].bulk
	n, err := strconv.Atoi(args[1].bulk)
	if err != nil {
		return Value{typ: "error", str: "value is not an integer or out of range"}
	}

	dt.Mu.Lock()
	defer dt.Mu.Unlock()

	var key_exist bool

	if _, exist := dt.Strings[key]; exist {
		key_exist = true
	}

	if _, exist := dt.Lists[key]; exist {
		key_exist = true
	}

	if _, exist := dt.Hashes[key]; exist {
		key_exist = true
	}

	if key_exist {
		dt.ExpireTime[key] = time.Now().Add(time.Duration(n) * time.Second)
		return Value{typ: "integer", num: 1}
	}

	return Value{typ: "integer", num: 0}
}

func ttl(dt *DataType, args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "error", str: "wrong number of arguments for 'ttl' command"}
	}

	key := args[0].bulk

	var key_exist bool

	if _, exist := dt.Strings[key]; exist {
		key_exist = true
	}

	if _, exist := dt.Lists[key]; exist {
		key_exist = true
	}

	if _, exist := dt.Hashes[key]; exist {
		key_exist = true
	}

	if key_exist {
		if expire_time, exist := dt.ExpireTime[key]; exist {
			if time.Now().Before(expire_time) {
				ttl := time.Until(expire_time).Seconds()
				return Value{typ: "integer", num: int(ttl)}
			}
		}

		return Value{typ: "integer", num: -1}
	}

	return Value{typ: "integer", num: -2}
}