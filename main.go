package main

import (
	"net"
	"fmt"
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

	for {
		reader := NewRespReader(conn)
		value, err := reader.Read()
		if err != nil {
			fmt.Println(err)
			return
		}

		_ = value

		writer := NewRespWriter(conn)
		
		writer.Write(Value{typ: "string", str: "OK"})
	}
}