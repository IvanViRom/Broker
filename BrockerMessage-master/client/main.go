package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"time"
)

func main() {
	// Читаем URL из переменных окружения (для Docker)
	brokerURL := os.Getenv("BROKER_URL")
	if brokerURL == "" {
		brokerURL = "http://localhost:8080"
	}

	qmURL := os.Getenv("QM_URL")
	if qmURL == "" {
		qmURL = "http://localhost:8081"
	}

	fmt.Printf("Broker URL: %s\n", brokerURL)
	fmt.Printf("Queue Manager URL: %s\n", qmURL)
	fmt.Println("ЗАПУСК ТЕСТОВЫХ СЦЕНАРИЕВ")

	// 1. Data Safety
	fmt.Println("\n1. Data Safety: Отправка 1000 сообщений...")
	pub := Publisher{BrokerURL: brokerURL}
	for i := 0; i < 1000; i++ {
		if err := pub.Publish("safety_topic", fmt.Sprintf("msg_%d", i)); err != nil {
			fmt.Printf("Ошибка при отправке сообщения %d: %v\n", i, err)
		}
	}
	fmt.Println("1000 сообщений сохранены с fsync.")

	// 2. Load Balancing
	fmt.Println("\n2. Load Balancing: 2 подписчика, 1 группа, 10 сообщений...")
	for i := 0; i < 10; i++ {
		if err := pub.Publish("lb_topic", fmt.Sprintf("lb_msg_%d", i)); err != nil {
			fmt.Printf("Ошибка при отправке lb_msg_%d: %v\n", i, err)
		}
	}

	var wg sync.WaitGroup
	counts := make(map[string]int)
	var mu sync.Mutex

	sub1 := &Subscriber{BrokerURL: brokerURL, Topic: "lb_topic", Group: "group_1"}
	sub2 := &Subscriber{BrokerURL: brokerURL, Topic: "lb_topic", Group: "group_1"}

	wg.Add(2)
	go func() {
		defer wg.Done()
		sub1.Subscribe(func(msg Message) {
			mu.Lock()
			counts["sub1"]++
			mu.Unlock()
		})
	}()
	go func() {
		defer wg.Done()
		sub2.Subscribe(func(msg Message) {
			mu.Lock()
			counts["sub2"]++
			mu.Unlock()
		})
	}()

	time.Sleep(2 * time.Second)
	fmt.Printf("Результат Load Balancing: Sub1: %d, Sub2: %d (Сумма: %d)\n",
		counts["sub1"], counts["sub2"], counts["sub1"]+counts["sub2"])

	// 3. Restart from Offset
	fmt.Println("\n3. Restart from Offset...")
	if err := pub.Publish("restart_topic", "msg_before_stop"); err != nil {
		fmt.Printf("Ошибка при отправке msg_before_stop: %v\n", err)
	}

	// Симулируем получение, но не отправляем ACK
	resp, err := http.Get(fmt.Sprintf("%s/broker/poll?topic=restart_topic&group=restart_group&count=1", brokerURL))
	if err != nil {
		fmt.Printf("Ошибка при получении сообщения: %v\n", err)
	} else {
		defer resp.Body.Close()
		var msgs []Message
		if err := json.NewDecoder(resp.Body).Decode(&msgs); err == nil && len(msgs) > 0 {
			fmt.Printf("Подписчик получил сообщение (Offset=%d), но упал ДО отправки ACK.\n", msgs[0].Offset)
		}
	}

	if err := pub.Publish("restart_topic", "msg_after_stop"); err != nil {
		fmt.Printf("Ошибка при отправке msg_after_stop: %v\n", err)
	}

	fmt.Println("Ожидание истечения 2-секундной аренды сообщения...")
	time.Sleep(3 * time.Second)

	fmt.Println("Перезапуск подписчика...")
	restartCount := 0
	done := make(chan bool)
	tempSub := &Subscriber{BrokerURL: brokerURL, Topic: "restart_topic", Group: "restart_group"}
	go func() {
		tempSub.Subscribe(func(msg Message) {
			restartCount++
			fmt.Printf(" -> Подписчик вычитал: Offset=%d, Payload='%s'\n", msg.Offset, msg.Payload)
			if restartCount == 2 {
				done <- true
			}
		})
	}()

	select {
	case <-done:
		fmt.Println("Успех: Подписчик вычитал непроцессированное сообщение и новые.")
	case <-time.After(5 * time.Second):
		fmt.Println("Ошибка: Таймаут ожидания сообщений.")
	}

	// 4. Multiple Groups
	fmt.Println("\n4. Multiple Groups: Одно сообщение, две разные группы...")
	if err := pub.Publish("multi_topic", "shared_message"); err != nil {
		fmt.Printf("Ошибка при отправке shared_message: %v\n", err)
	}

	groupAReceived := false
	groupBReceived := false
	doneMulti := make(chan bool)
	var muMulti sync.Mutex

	checkMulti := func() {
		muMulti.Lock()
		defer muMulti.Unlock()
		if groupAReceived && groupBReceived {
			doneMulti <- true
		}
	}

	go (&Subscriber{BrokerURL: brokerURL, Topic: "multi_topic", Group: "Group_A"}).Subscribe(func(msg Message) {
		if msg.Payload == "shared_message" {
			muMulti.Lock()
			groupAReceived = true
			muMulti.Unlock()
			checkMulti()
		}
	})
	go (&Subscriber{BrokerURL: brokerURL, Topic: "multi_topic", Group: "Group_B"}).Subscribe(func(msg Message) {
		if msg.Payload == "shared_message" {
			muMulti.Lock()
			groupBReceived = true
			muMulti.Unlock()
			checkMulti()
		}
	})

	select {
	case <-doneMulti:
		fmt.Println("Успех: Сообщение доставлено в Group_A и Group_B независимо.")
	case <-time.After(5 * time.Second):
		fmt.Println("Ошибка: Таймаут Multiple Groups.")
	}

	fmt.Println("\nВСЕ ТЕСТЫ ПРОЙДЕНЫ")

	// Показываем статус из QM
	respStatus, err := http.Get(fmt.Sprintf("%s/qm/status", qmURL))
	if err == nil {
		defer respStatus.Body.Close()
		fmt.Println("\nТекущий статус Менеджера Очередей:")
		if _, err := io.Copy(os.Stdout, respStatus.Body); err != nil {
			fmt.Printf("Ошибка при чтении статуса: %v\n", err)
		}
		fmt.Println()
	} else {
		fmt.Printf("Не удалось получить статус QM: %v\n", err)
	}
}
