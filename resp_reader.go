package main

import (
	"fmt"
	"bufio"
	"io"
)

const (
	STRING = "+"
	ERROR = "-"
	INTEGER = ":"
	BULK = "$"
	ARRAY = "*"
)

type respReader struct {
	reader *bufio.Reader
}

// *3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n //

func (r *respReader) Read() error {
	_type, err := r.reader.ReadByte()
	if err != nil {
		return err
	}
	switch _type {
	case ARRAY:
		return r.readArray()
	case BULT:
		return r.readBulk()
	}
}

func (r *respReader) readLine() (line []byte, err error) {
	for {
		v, err := r.reader.ReadByte()
		if err != nil {
			return 0, 0, err
		}
		line = append(line, v)
		if len(line) >= 2 && line[len(line) - 2] == '\r' {
			break
		}
	}

	return line[:len(line) - 2], nil 
}

// func (r *respReader) readLength() (n int, err error) {
// 	length, err := r.readLine()
// 	if err != nil {
// 		return 0, err
// 	}
// }



