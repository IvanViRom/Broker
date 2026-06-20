package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

type Publisher struct {
	BrokerURL string
}

func (p *Publisher) Publish(topic, payload string) error {
	reqBody, err := json.Marshal(map[string]string{"topic": topic, "payload": payload})
	if err != nil {
		return err
	}

	resp, err := http.Post(p.BrokerURL+"/broker/publish", "application/json", bytes.NewBuffer(reqBody))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("publish failed: status %d", resp.StatusCode)
	}
	return nil
}

type Subscriber struct {
	BrokerURL string
	Topic     string
	Group     string
}

func (s *Subscriber) Subscribe(handler func(msg Message)) {
	for {
		url := fmt.Sprintf("%s/broker/poll?topic=%s&group=%s&count=1", s.BrokerURL, s.Topic, s.Group)
		resp, err := http.Get(url)
		if err != nil {
			time.Sleep(1 * time.Second)
			continue
		}

		var msgs []Message
		if err := json.NewDecoder(resp.Body).Decode(&msgs); err != nil {
			resp.Body.Close()
			time.Sleep(500 * time.Millisecond)
			continue
		}
		resp.Body.Close()

		for _, msg := range msgs {
			// Вызываем обработчик
			handler(msg)

			// Отправляем ACK
			ackBody, err := json.Marshal(map[string]interface{}{
				"topic":  s.Topic,
				"group":  s.Group,
				"offset": msg.Offset,
			})
			if err != nil {
				continue
			}

			ackResp, err := http.Post(s.BrokerURL+"/broker/ack", "application/json", bytes.NewBuffer(ackBody))
			if err == nil {
				ackResp.Body.Close()
			}

			// Небольшая задержка для балансировки
			time.Sleep(100 * time.Millisecond)
		}

		if len(msgs) == 0 {
			time.Sleep(500 * time.Millisecond)
		}
	}
}
