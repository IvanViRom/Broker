package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

func HealthHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

var GlobalState = State{
	CommittedOffsets: make(map[string]map[string]int),
	PendingLeases:    make(map[string]map[string]map[int]int64),
}

func LoadState() {
	data, err := os.ReadFile(filepath.Join(DataDir, "state.json"))
	if err == nil {
		json.Unmarshal(data, &GlobalState.CommittedOffsets)
	}
	for t := range GlobalState.CommittedOffsets {
		if GlobalState.PendingLeases[t] == nil {
			GlobalState.PendingLeases[t] = make(map[string]map[int]int64)
		}
		for g := range GlobalState.CommittedOffsets[t] {
			if GlobalState.PendingLeases[t][g] == nil {
				GlobalState.PendingLeases[t][g] = make(map[int]int64)
			}
		}
	}
}

func SaveState() {
	data, _ := json.MarshalIndent(GlobalState.CommittedOffsets, "", "  ")
	os.WriteFile(filepath.Join(DataDir, "state.json"), data, 0644)
}

func QMFetchHandler(w http.ResponseWriter, r *http.Request) {
	topic := r.URL.Query().Get("topic")
	group := r.URL.Query().Get("group")
	countStr := r.URL.Query().Get("count")
	count := 10
	if c, err := strconv.Atoi(countStr); err == nil && c > 0 {
		count = c
	}

	StateMu.Lock()
	defer StateMu.Unlock()

	if GlobalState.CommittedOffsets[topic] == nil {
		GlobalState.CommittedOffsets[topic] = make(map[string]int)
	}
	if GlobalState.PendingLeases[topic] == nil {
		GlobalState.PendingLeases[topic] = make(map[string]map[int]int64)
	}
	if GlobalState.PendingLeases[topic][group] == nil {
		GlobalState.PendingLeases[topic][group] = make(map[int]int64)
	}

	offset := GlobalState.CommittedOffsets[topic][group]
	msgs, _ := ReadMessages(topic, offset, count)

	availableMsgs := make([]Message, 0)
	now := time.Now().Unix()

	for _, msg := range msgs {
		leaseExp, exists := GlobalState.PendingLeases[topic][group][msg.Offset]
		if !exists || now > leaseExp {
			availableMsgs = append(availableMsgs, msg)
			// Аренда всего на 2 секунды
			GlobalState.PendingLeases[topic][group][msg.Offset] = now + 2
			if len(availableMsgs) >= count {
				break
			}
		}
	}
	json.NewEncoder(w).Encode(availableMsgs)
}

func QMAckHandler(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Topic  string `json:"topic"`
		Group  string `json:"group"`
		Offset int    `json:"offset"`
	}
	json.NewDecoder(r.Body).Decode(&req)

	StateMu.Lock()
	defer StateMu.Unlock()

	if GlobalState.PendingLeases[req.Topic] != nil && GlobalState.PendingLeases[req.Topic][req.Group] != nil {
		delete(GlobalState.PendingLeases[req.Topic][req.Group], req.Offset)
	}

	current := GlobalState.CommittedOffsets[req.Topic][req.Group]
	if req.Offset >= current {
		GlobalState.CommittedOffsets[req.Topic][req.Group] = req.Offset + 1
	}

	SaveState()
	w.WriteHeader(http.StatusOK)
}

func QMStatusHandler(w http.ResponseWriter, r *http.Request) {
	StateMu.RLock()
	defer StateMu.RUnlock()
	json.NewEncoder(w).Encode(GlobalState.CommittedOffsets)
}

func StartQueueManager() {
	LoadState()
	http.HandleFunc("/health", HealthHandler)
	http.HandleFunc("/qm/fetch", QMFetchHandler)
	http.HandleFunc("/qm/ack", QMAckHandler)
	http.HandleFunc("/qm/status", QMStatusHandler)
	fmt.Printf("Queue Manager started on :8081, DataDir: %s\n", DataDir)
	go http.ListenAndServe(":8081", nil)
}
