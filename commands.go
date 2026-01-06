package main

import (
	"sync"
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