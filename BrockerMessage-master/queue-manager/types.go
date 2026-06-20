package main

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
)

type Message struct {
	Offset  int    `json:"offset"`
	Payload string `json:"payload"`
}

type State struct {
	CommittedOffsets map[string]map[string]int           `json:"committed_offsets"`
	PendingLeases    map[string]map[string]map[int]int64 `json:"-"`
}

var (
	DataDir = getDataDir()
	StateMu sync.RWMutex
)

func getDataDir() string {
	dir := os.Getenv("DATA_DIR")
	if dir == "" {
		dir = "./data"
	}
	return dir
}

func InitStorage() {
	os.MkdirAll(DataDir, 0755)
}

func GetTopicFile(topic string) string {
	return filepath.Join(DataDir, topic+".log")
}

// getLineCount считает количество строк в файле
func getLineCount(filename string) (int, error) {
	file, err := os.Open(filename)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	// Увеличиваем буфер для обработки длинных строк
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	lines := 0
	for scanner.Scan() {
		lines++
	}
	if err := scanner.Err(); err != nil {
		return lines, err
	}
	return lines, nil
}

func AppendMessage(topic string, payload string) (int, error) {
	topicFile := GetTopicFile(topic)

	// Считаем текущее количество строк
	offset := 0
	if lines, err := getLineCount(topicFile); err == nil {
		offset = lines
	}

	msg := Message{Offset: offset, Payload: payload}
	data, err := json.Marshal(msg)
	if err != nil {
		return -1, err
	}

	f, err := os.OpenFile(topicFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return -1, err
	}
	defer f.Close()

	if _, err := f.Write(append(data, '\n')); err != nil {
		return -1, err
	}

	if err := f.Sync(); err != nil {
		return -1, err
	}

	return offset, nil
}

func ReadMessages(topic string, startOffset, count int) ([]Message, error) {
	file, err := os.Open(GetTopicFile(topic))
	if os.IsNotExist(err) {
		return []Message{}, nil
	}
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var messages []Message
	// Используем scanner вместо decoder для надёжности
	scanner := bufio.NewScanner(file)
	// Увеличиваем буфер для обработки длинных строк
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	currentLine := 0

	for scanner.Scan() {
		var msg Message
		if err := json.Unmarshal(scanner.Bytes(), &msg); err != nil {
			currentLine++
			continue // пропускаем битые строки
		}
		if currentLine >= startOffset {
			messages = append(messages, msg)
			if len(messages) >= count {
				break
			}
		}
		currentLine++
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return messages, nil
}
