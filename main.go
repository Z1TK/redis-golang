package main

import (
	"fmt"
	"net"
	"strings"
)

func main() {
	fmt.Println("Listening on port :6379")
	ser, err := net.Listen("tcp", ":6379")
	if err != nil {
		fmt.Println(err)
		return 
	}
	defer ser.Close()

	dt := createDT()
	
	aof, err := NewAof("database.aof")
	if err != nil {
		fmt.Println(err)
		return
	}

	aof.AofRead(func(value Value) {
		command := strings.ToUpper(value.array[0].bulk)
		args := value.array[1:]

		handler, ok := Handlers[command]
		if !ok {
			fmt.Println("Invalid command: ", command)
		}

		handler(dt, args)
	})

	conn, err := ser.Accept()
	if err != nil {
		fmt.Println(err)
		return
	}
	defer conn.Close()

	for {
		reader := NewRespReader(conn)
		value, err := reader.Read()
		if err != nil {
			fmt.Println(err)
			return
		}

		if value.typ != "array" {
			fmt.Println("Invalid request, expected array")
			continue
		}
		
		if len(value.array) == 0 {
			fmt.Println("Invalid request, expected array length > 0")
			continue
		}

		command := strings.ToUpper(value.array[0].bulk)
		args := value.array[1:]

		writer := NewRespWriter(conn)

		handler, ok := Handlers[command]
		if !ok {
			fmt.Println("Invalid command: ", command)
			writer.Write(Value{typ: "string", str: ""})
			continue
		}

		if command == "SET" || command == "HSET" {
			aof.AofWrite(value)
		}

		result := handler(dt, args)
		writer.Write(result)
	}
}