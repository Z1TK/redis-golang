package main

import (
	"net"
	"fmt"
	"io"
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
		buf := make([]byte, 1024)
		_, err = conn.Read(buf)
		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Println("Error reading from clieng: ", err)
		}
		
		conn.Write([]byte("+OK\r\n"))
	}
}