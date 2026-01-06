package main

import (
	"fmt"
	"bufio"
	"strconv"
	"io"
)

const (
	STRING = '+'
	ERROR = '-'
	INTEGER = ':'
	BULK = '$'
	ARRAY = '*'
)

type respReader struct {
	reader *bufio.Reader
}

type Value struct {
	typ string
	str string
	num int
	bulk string
	array []Value
}

func NewRespReader(rd io.Reader) *respReader {
	return &respReader{reader: bufio.NewReader(rd)}
}

func (r *respReader) Read() (Value, error) {
	typ, err := r.reader.ReadByte()
	if err != nil {
		return Value{}, err
	}
	switch typ {
	case ARRAY:
		return r.readArray()
	case BULK:
		return r.readBulk()
	default:
		fmt.Printf("unknow type: %v", typ)
		return Value{}, err
	}
}

func (r *respReader) readLine() (line []byte, err error) {
	for {
		v, err := r.reader.ReadByte()
		if err != nil {
			return nil, err
		}
		line = append(line, v)
		if len(line) >= 2 && line[len(line) - 2] == '\r' {
			break
		}
	}

	return line[:len(line) - 2], nil 
}

func (r *respReader) readLength() (n int, err error) {
	length, err := r.readLine()
	if err != nil {
		return 0, err
	}
	i64, err := strconv.ParseInt(string(length), 10, 64)
	if err != nil {
		return 0, err
	}

	return int(i64), nil
}

func (r *respReader) readArray() (Value, error) {
	v := Value{}
	v.typ = "array"

	length, err := r.readLength()
	if err != nil {
		return v, err
	}

	v.array = make([]Value, 0, length)
	for i := 0; i < length; i++ {
		val, err := r.Read()
		if err != nil {
			return val, err
		}
		v.array = append(v.array, val)
	}

	return v, err
}

func (r *respReader) readBulk() (Value, error) {
	v := Value{}
	v.typ = "bulk"

	length, err := r.readLength()
	if err != nil {
		return v, err
	}

	bulk := make([]byte, length)

	_, err = r.reader.Read(bulk)
	if err != nil {
		return v, err
	}

	v.bulk = string(bulk) 

	r.readLine()

	return v, nil
}