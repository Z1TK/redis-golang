package main

import (
	"strconv"
	"io"
)

type respWriter struct {
	writer io.Writer 
}

func NewRespWriter(w io.Writer) *respWriter {
	return &respWriter{w}
}

func (w *respWriter) Write(v Value) error {
	bytes := v.replyValue()
	
	_, err := w.writer.Write(bytes)
	if err != nil {
		return err
	}

	return nil
}

func (v Value) replyValue() []byte {
	switch v.typ {
	case "array":
		return v.replyArray()
	case "bulk":
		return v.replyBulk()
	case "string":
		return v.replyString()
	case "error":
		return v.replyError()
	case "integer":
		return v.replyInteger()
	case "null":
		return v.replyNull()
	default:
		return []byte{}
	}
}

func (v Value) replyString() []byte {
	var bytes []byte
	bytes = append(bytes, STRING)
	bytes = append(bytes, v.str...)
	bytes = append(bytes, '\r', '\n')
	return bytes
}

func (v Value) replyArray() []byte {
	var bytes []byte
	length := len(v.array)
	bytes = append(bytes, ARRAY)
	bytes = append(bytes, strconv.Itoa(length)...)
	bytes = append(bytes, '\r', '\n')

	for i := 0; i < length; i++ {
		bytes = append(bytes, v.array[i].replyValue()...)
	}

	return bytes
}

func (v Value) replyBulk() []byte {
	var bytes []byte
	length := len(v.bulk)
	bytes = append(bytes, BULK)
	bytes = append(bytes, strconv.Itoa(length)...)
	bytes = append(bytes, '\r', '\n')
	bytes = append(bytes, v.bulk...)
	bytes = append(bytes, '\r', '\n')
	return bytes
}

func (v Value) replyInteger() []byte {
	var bytes []byte
	bytes = append(bytes, INTEGER)
	bytes = append(bytes, strconv.Itoa(v.num)...)
	bytes = append(bytes, '\r', '\n')
	return bytes
}

func (v Value) replyError() []byte {
	var bytes []byte
	bytes = append(bytes, ERROR)
	bytes = append(bytes, v.str...)
	bytes = append(bytes, '\r', '\n')
	return bytes
}

func (v Value) replyNull() []byte {
	return []byte("$-1\r\n")
}