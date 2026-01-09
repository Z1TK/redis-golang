package main

import (
	"os"
	"bufio"
	"sync"
	"time"
)

type Aof struct {
	file *os.File
	rd *bufio.Reader
	mu sync.Mutex
}

func NewAof(path string) (*Aof, error) {
	f, err := os.OpenFile(path, os.O_CREATE | os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}

	aof := &Aof{
		file: f,
		rd: bufio.NewReader(f),
	}

	go func() {
		for {
			aof.mu.Lock()

			aof.file.Sync()

			aof.mu.Unlock()

			time.Sleep(time.Second)
		}
	} ()

	return aof, nil
}

func (aof *Aof) AofClose() error {
	aof.mu.Lock()
	defer aof.mu.Unlock()

	return aof.file.Close()
}

func (aof *Aof) AofWrite(value Value) error {
	aof.mu.Lock()
	defer aof.mu.Unlock()

	if _, err := aof.file.Write(value.replyValue()); err != nil {
		return err
	}

	return nil
}