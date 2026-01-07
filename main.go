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

	conn, err := ser.Accept()
	if err != nil {
		fmt.Println(err)
		return
	}
	defer conn.Close()

	dt := createDT()

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
		result := handler(dt, args)

		writer.Write(result)
	}
}