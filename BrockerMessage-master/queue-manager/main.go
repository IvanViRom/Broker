package main

import (
	"fmt"
)

func main() {
	InitStorage()
	StartQueueManager()
	fmt.Println("Queue Manager is running on :8081")

	select {}
}
