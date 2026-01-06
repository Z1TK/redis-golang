package main

import (
	"sync"
	"strconv"
)

type KeyValuePair struct {
	Strings map[string]string
	Mu sync.RWMutex
}

func createKVP() *KeyValuePair {
	return &KeyValuePair{
		Strings: make(map[string]string),
	}
}

var Handlers = map[string]func(*KeyValuePair, []Value) Value {
	"PING": ping,
	"SET": set,
	"GET": get,
	"SETNX": setnx,
	"MSET": mset,
	"MGET": mget,
	"INCR": incr,
	"DECR": decr,
	// "RPUSH": rpush,
	// "LPUSH": lpush,
	// "RPOP": rpop,
	// "LPOP": lpop,
	// "LRANGE": lrange,
	// "DEL": del,
	// "EXPIRE": expire,
	// "TTL": ttl,
}

func ping(_ *KeyValuePair, args []Value) Value {
	if len(args) == 0 {
		return Value{typ: "string", str: "PONG"}
	}

	return Value{typ: "string", str: args[0].bulk}
}

func set(kv *KeyValuePair, args []Value) Value {
	if len(args) != 2 {
		return Value{typ: "error", str: "wrong number of arguments for 'set' command"}
	}

	key := args[0].bulk
	val := args[1].bulk

	kv.Mu.Lock()
	defer kv.Mu.Unlock()

	kv.Strings[key] = val

	return Value{typ: "string", str: "OK"}
}

func get(kv *KeyValuePair, args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "error", str: "wrong number of arguments for 'get' command"}
	}

	key := args[0].bulk

	kv.Mu.Lock()
	defer kv.Mu.Unlock()

	val, ok := kv.Strings[key]
	if !ok {
		return Value{typ: "null"}
	}

	return Value{typ: "bulk", bulk: val}
}

func setnx(kv *KeyValuePair, args []Value) Value {
	if len(args) != 2 {
		return Value{typ: "error", str: "wrong number of arguments for 'set' command"}
	}

	key := args[0].bulk
	val := args[1].bulk

	kv.Mu.Lock()
	defer kv.Mu.Unlock()

	if _, exist := kv.Strings[key]; !exist {
		kv.Strings[key] = val
		return Value{typ: "integer", num: 1}
	}

	return Value{typ: "integer", num: 0}
}

func mset(kv *KeyValuePair, args []Value) Value {
	if len(args) % 2 != 0 {
		return Value{typ: "error", str: "wrong number of arguments for 'mset' command"}
	}

	kv.Mu.Lock()
	defer kv.Mu.Unlock()

	for i := 0; i < len(args); i += 2 {
		key := args[i].bulk
		val := args[i+1].bulk
		kv.Strings[key] = val
	}

	return Value{typ: "string", str: "OK"}
}

func mget(kv *KeyValuePair, args []Value) Value {
	if len(args) < 1 {
		return Value{typ: "error", str: "wrong number of arguments for 'mget' command"}
	}

	var res []Value
	kv.Mu.Lock()
	defer kv.Mu.Unlock()

	for i := 0; i < len(args); i += 1 {
		key := args[i].bulk
		if val, exist := kv.Strings[key]; exist {
			res = append(res, Value{typ: "bulk", bulk: val})
		} else {
			res = append(res, Value{typ: "null"})
		}
	}

	return Value{typ: "array", array: res}
}

func incr(kv *KeyValuePair, args []Value) Value {
	if len(args) != 1  {
		return Value{typ: "error", str: "wrong number of arguments for 'incr' command"}
	}

	key := args[0].bulk

	kv.Mu.Lock()
	defer kv.Mu.Unlock()

	if _, exist := kv.Strings[key]; !exist {
		kv.Strings[key] = "1"
		return Value{typ: "integer", num: 1}
	}

	n, err := strconv.Atoi(kv.Strings[key])
	if err != nil {
		return Value{typ: "error", str: "value is not an integer or out of range"}
	}
	n++

	kv.Strings[key] = strconv.Itoa(n)

	return Value{typ: "integer", num: n}
}

func decr(kv *KeyValuePair, args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "error", str: "wrong number of arguments for 'decr' command"}
	}

	key := args[0].bulk

	kv.Mu.Lock()
	defer kv.Mu.Unlock()

	if _, exist := kv.Strings[key]; !exist {
		kv.Strings[key] = "-1"
		return Value{typ: "integer", num: -1}
	}

	n, err := strconv.Atoi(kv.Strings[key])
	if err != nil {
		return Value{typ: "error", str: "value is not an integer or out of range"}
	}
	n--

	kv.Strings[key] = strconv.Itoa(n)

	return Value{typ: "integer", num: n}
}