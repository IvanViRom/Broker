package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	// Инициализация хранилища
	InitStorage()

	// Запуск брокера в отдельной горутине
	go StartBroker()

	fmt.Println("Broker is running on :8080")
	fmt.Println("Press Ctrl+C to stop...")

	// Ожидание сигнала завершения (Ctrl+C)
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	fmt.Println("\nShutting down gracefully...")
	log.Println("Broker stopped")
}
