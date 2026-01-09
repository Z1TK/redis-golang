package main

import (
	"os"
	"log"
)

type Log struct {
	file *os.File
	logger *log.Logger
}

func NewLogger(path string, pref string) (*Log, error) {
	f, err := os.OpenFile(path, os.O_CREATE | os.O_WRONLY |os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}

	l := &Log{
		file: f,
		logger: log.New(f, pref, log.LstdFlags),
	}

	return l, nil
}

func (l *Log) Info(mes string) {
	l.logger.Println("[INFO]", mes)
}

func (l *Log) Error(err error) {
	l.logger.Println("[ERROR]", err)
}

func (l *Log) Close() error {
	return l.file.Close()
}