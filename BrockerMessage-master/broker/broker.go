package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
)

// ===== Вспомогательные функции =====

func HealthHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

// ===== Брокер: публикация сообщения =====

func BrokerPublishHandler(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Topic   string `json:"topic"`
		Payload string `json:"payload"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	if req.Topic == "" || req.Payload == "" {
		http.Error(w, "Topic and Payload are required", http.StatusBadRequest)
		return
	}

	offset, err := AppendMessage(req.Topic, req.Payload)
	if err != nil {
		fmt.Printf("ERROR: Failed to append message: %v\n", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	fmt.Printf("Message saved: topic=%s, offset=%d\n", req.Topic, offset)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]int{"offset": offset})
}

// ===== Брокер: получение сообщений =====

func BrokerPollHandler(w http.ResponseWriter, r *http.Request) {
	topic := r.URL.Query().Get("topic")
	group := r.URL.Query().Get("group")
	count := r.URL.Query().Get("count")

	if topic == "" || group == "" {
		http.Error(w, "Topic and Group are required", http.StatusBadRequest)
		return
	}

	if count == "" {
		count = "10"
	}

	qmURL := os.Getenv("QM_URL")
	if qmURL == "" {
		qmURL = "http://localhost:8081"
	}

	url := fmt.Sprintf("%s/qm/fetch?topic=%s&group=%s&count=%s", qmURL, topic, group, count)
	resp, err := http.Get(url)
	if err != nil {
		fmt.Printf("ERROR: Failed to fetch from QM: %v\n", err)
		http.Error(w, "QM unavailable", http.StatusServiceUnavailable)
		return
	}
	defer resp.Body.Close()

	w.Header().Set("Content-Type", "application/json")
	io.Copy(w, resp.Body)
}

// ===== Брокер: подтверждение =====

func BrokerAckHandler(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Topic  string `json:"topic"`
		Group  string `json:"group"`
		Offset int    `json:"offset"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	if req.Topic == "" || req.Group == "" {
		http.Error(w, "Topic and Group are required", http.StatusBadRequest)
		return
	}

	qmURL := os.Getenv("QM_URL")
	if qmURL == "" {
		qmURL = "http://localhost:8081"
	}

	data, _ := json.Marshal(req)
	resp, err := http.Post(fmt.Sprintf("%s/qm/ack", qmURL), "application/json", strings.NewReader(string(data)))
	if err != nil {
		fmt.Printf("ERROR: Failed to ACK to QM: %v\n", err)
		http.Error(w, "ACK failed", http.StatusInternalServerError)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		fmt.Printf("ERROR: QM returned status %d\n", resp.StatusCode)
		http.Error(w, "ACK failed", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// ===== Запуск брокера =====

func StartBroker() {
	http.HandleFunc("/health", HealthHandler)
	http.HandleFunc("/broker/publish", BrokerPublishHandler)
	http.HandleFunc("/broker/poll", BrokerPollHandler)
	http.HandleFunc("/broker/ack", BrokerAckHandler)

	fmt.Printf("Service Broker started on :8080, DataDir: %s\n", DataDir)

	if err := http.ListenAndServe(":8080", nil); err != nil {
		fmt.Printf("ERROR: Failed to start broker: %v\n", err)
	}
}
