package main

import (
	"fmt"
	"net"
	"strings"
)

func connection(conn net.Conn, dt *DataType, aof *Aof, l *Log) {
	defer conn.Close()

	for {
		reader := NewRespReader(conn)
			value, err := reader.Read()
			if err != nil {
				l.Error(err)
				return
			}

			if value.typ != "array" {
				l.Info("Invalid request, expected array")
				continue
			}
			
			if len(value.array) == 0 {
				l.Info("Invalid request, expected array length > 0")
				continue
			}

			command := strings.ToUpper(value.array[0].bulk)
			args := value.array[1:]

			writer := NewRespWriter(conn)

			handler, ok := Handlers[command]
			if !ok {
				l.Info("Invalid command: " + command)
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

func main() {
	l, err := NewLogger("logs.log", "logger ")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer l.Close()

	l.Info("Listening on port :6379")
	server, err := net.Listen("tcp", ":6379")
	if err != nil {
		l.Error(err)
		return 
	}
	defer server.Close()

	dt := createDT()

	aof, err := NewAof("database.aof")
	if err != nil {
		l.Error(err)
		return
	}

	aof.AofRead(func(value Value) {
		command := strings.ToUpper(value.array[0].bulk)
		args := value.array[1:]

		handler, ok := Handlers[command]
		if !ok {
			l.Info("Invalid command: " + command)
		}

		handler(dt, args)
	})

	for {
		conn, err := server.Accept()
		if err != nil {
		l.Error(err)
		return
		}
		
		go connection(conn, dt, aof, l)
	}
}