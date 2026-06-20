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
	DataDir = "./data"
	StateMu sync.RWMutex
)

func InitStorage() {
	os.MkdirAll(DataDir, 0755)
}

func GetTopicFile(topic string) string {
	return filepath.Join(DataDir, topic+".log")
}

func AppendMessage(topic string, payload string) (int, error) {
	topicFile := GetTopicFile(topic)

	// Получаем текущий размер файла (количество строк)
	offset := 0
	file, err := os.Open(topicFile)
	if err == nil {
		scanner := bufio.NewScanner(file)
		// Увеличиваем буфер для больших строк
		buf := make([]byte, 0, 64*1024)
		scanner.Buffer(buf, 1024*1024)

		for scanner.Scan() {
			offset++
		}
		file.Close()

		if err := scanner.Err(); err != nil {
			// Если ошибка сканирования, пробуем другой способ
			stat, _ := os.Stat(topicFile)
			if stat != nil && stat.Size() == 0 {
				offset = 0
			}
		}
	}

	// Записываем сообщение
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
	topicFile := GetTopicFile(topic)

	file, err := os.Open(topicFile)
	if os.IsNotExist(err) {
		return []Message{}, nil
	}
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var messages []Message
	scanner := bufio.NewScanner(file)
	// Увеличиваем буфер для больших строк
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)

	currentLine := 0

	for scanner.Scan() {
		var msg Message
		if err := json.Unmarshal(scanner.Bytes(), &msg); err != nil {
			// Пропускаем битые строки
			currentLine++
			continue
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
