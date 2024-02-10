package main

import "sync"

var Handlers = map[string]func([]Value) Value{
	"PING":    ping,
	"SET":     set,
	"GET":     get,
	"HSET":    hset,
	"HGET":    hget,
	"HGETALL": hgetall,
}

var sets = map[string]string{}
var setsMu = sync.RWMutex{}

var hsets = map[string]map[string]string{}
var hsetsMu = sync.RWMutex{}

func ping(args []Value) Value {
	if len(args) == 0 {
		return Value{typ: TYP_STRING, str: "PONG"}
	}

	return Value{typ: TYP_STRING, str: args[0].bulk}
}

func set(args []Value) Value {
	if len(args) != 2 {
		return Value{typ: TYP_ERROR, str: "ERR wrong number of arguments for 'set' command"}
	}

	key := args[0].bulk
	value := args[1].bulk

	setsMu.Lock()
	sets[key] = value
	setsMu.Unlock()

	return Value{typ: TYP_STRING, str: "OK"}
}

func get(args []Value) Value {
	if len(args) != 1 {
		return Value{typ: TYP_ERROR, str: "ERR wrong number of arguments for 'get' command"}
	}

	key := args[0].bulk

	setsMu.RLock()
	value, ok := sets[key]
	setsMu.RUnlock()

	if !ok {
		return Value{typ: TYP_NULL}
	}

	return Value{typ: TYP_BULK, bulk: value}
}

func hset(args []Value) Value {
	if len(args) != 3 {
		return Value{typ: TYP_ERROR, str: "ERR wrong number of arguments for 'hset' command"}
	}

	hash := args[0].bulk
	key := args[1].bulk
	value := args[2].bulk

	hsetsMu.Lock()
	if _, ok := hsets[hash]; !ok {
		hsets[hash] = map[string]string{}
	}
	hsets[hash][key] = value
	hsetsMu.Unlock()

	return Value{typ: TYP_STRING, str: "OK"}
}

func hget(args []Value) Value {
	if len(args) != 2 {
		return Value{typ: TYP_ERROR, str: "ERR wrong number of arguments for 'hget' command"}
	}

	hash := args[0].bulk
	key := args[1].bulk

	hsetsMu.RLock()
	value, ok := hsets[hash][key]
	hsetsMu.RUnlock()

	if !ok {
		return Value{typ: TYP_NULL}
	}

	return Value{typ: TYP_BULK, bulk: value}
}

func hgetall(args []Value) Value {
	if len(args) != 1 {
		return Value{typ: TYP_ERROR, str: "ERR wrong number of arguments for 'hgetall' command"}
	}

	hash := args[0].bulk

	hsetsMu.RLock()
	values, ok := hsets[hash]
	hsetsMu.RUnlock()

	if !ok {
		return Value{typ: TYP_NULL}
	}

	valueArray := []Value{}
	for key, value := range values {
		valueArray = append(valueArray, Value{typ: TYP_STRING, str: key})
		valueArray = append(valueArray, Value{typ: TYP_STRING, str: value})
	}

	return Value{typ: TYP_ARRAY, array: valueArray}
}
