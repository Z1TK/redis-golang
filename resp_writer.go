package main

import (
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
	// case "array":
	// 	return v.replyArray()
	// case "bulk":
	// 	return v.replyBulk()
	case "string":
		return v.replyString()
	// case "error":
	// 	return v.replyError()
	// case "integer":
	// 	return v.replyInteger()
	// case "null":
	// 	return v.replyNull()
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