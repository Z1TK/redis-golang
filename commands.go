package main

import (
	"fmt"
	"strconv"
	"sync"
)

type DataType struct {
	Strings map[string]string
	List map[string][]string
	Mu sync.RWMutex
}

func createDT() *DataType {
	return &DataType{
		Strings: make(map[string]string),
		List: make(map[string][]string),
	}
}

var Handlers = map[string]func(*DataType, []Value) Value {
	"PING": ping,
	"SET": set,
	"GET": get,
	"SETNX": setnx,
	"MSET": mset,
	"MGET": mget,
	"INCR": incr,
	"DECR": decr,
	"RPUSH": rpush,
	"LPUSH": lpush,
	"RPOP": rpop,
	"LPOP": lpop,
	"LRANGE": lrange,
	// "DEL": del,
	// "EXPIRE": expire,
	// "TTL": ttl,
	// create fold for handlers, command_stuf, strings_command, list_command
}

func ping(_ *DataType, args []Value) Value {
	if len(args) == 0 {
		return Value{typ: "string", str: "PONG"}
	}

	return Value{typ: "string", str: args[0].bulk}
}

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

	dt.Mu.Lock()
	defer dt.Mu.Unlock()

	val, ok := dt.Strings[key]
	if !ok {
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
	dt.Mu.Lock()
	defer dt.Mu.Unlock()

	for i := 0; i < len(args); i += 1 {
		key := args[i].bulk
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

	if _, exist := dt.Strings[key]; !exist {
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

	if _, exist := dt.Strings[key]; !exist {
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

func rpush(dt *DataType, args []Value) Value {
	if len(args) < 2 {
		return Value{typ: "error", str: "wrong number of arguments for 'rpush' command"}
	}

	key := args[0].bulk
	length := len(args)

	dt.Mu.Lock()
	defer dt.Mu.Unlock()

	for i := 1; i < length; i++ {
		dt.List[key] = append(dt.List[key], args[i].bulk)
	}

	n := len(dt.List[key])

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

	for i := 1; i < length; i++ {
		dt.List[key] = append([]string{args[i].bulk}, dt.List[key]...)
	}

	n := len(dt.List[key])

	fmt.Println(dt.List[key][0])

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

	data, exist := dt.List[key]
	if len(data) == 0 || !exist {
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
			res = append(res, Value{typ: "bulk", bulk: dt.List[key][length - 1 - i]})
		}

		dt.List[key] = dt.List[key][:length - val]	
		return Value{typ: "array", array: res}
	} 

	val := dt.List[key][length - 1]
	dt.List[key] = dt.List[key][:length - 1]
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



	data, exist := dt.List[key]
	if len(data) == 0 || !exist {
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
			res = append(res, Value{typ: "bulk", bulk: dt.List[key][i]})
		}

		dt.List[key] = dt.List[key][val:]
		return Value{typ: "array", array: res}
	}

	val := dt.List[key][0]
	dt.List[key] = dt.List[key][1:]
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

	dt.Mu.Lock()
	defer dt.Mu.Unlock()

	data, exist := dt.List[key]
	if len(data) == 0 || !exist {
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

	val := dt.List[key][startInt:endInt + 1]
	for i := 0; i < len(val); i++ {
		res = append(res, Value{typ: "bulk", bulk: val[i]})
	}

	return Value{typ: "array", array: res}
}