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
}

func ping(_ *KeyValuePair, args []Value) Value {
	if len(args) == 0 {
		return Value{typ: "string", str: "PONG"}
	}

	return Value{typ: "string", str: args[0].bulk}
}