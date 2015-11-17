package main

import (
	"log"
	"time"

	"github.com/rodrigosaito/amqpworker"
)

const AMQP_URI = "amqp://guest:guest@10.0.215.50:5672"

func MyWorkerFunc(msg *amqpworker.Message) {
	log.Println(msg.ConsumerTag())
	log.Println(string(msg.Body()))
	time.Sleep(500 * time.Millisecond)
	msg.Ack()
}

func main() {
	worker := amqpworker.NewAmqpWorker(AMQP_URI)
	defer worker.Stop()

	worker.RegisterExchange(&amqpworker.Exchange{
		Name:       "test_exchange",
		Kind:       "topic",
		Durable:    false,
		AutoDelete: true,
	})

	worker.RegisterConsumer(&amqpworker.Consumer{
		WorkerFunc:  MyWorkerFunc,
		Concurrency: 4,
		Queue: &amqpworker.Queue{
			Name:       "test_maluco",
			Durable:    false,
			AutoDelete: true,
			Exclusive:  false,
			NoWait:     false,
			Args:       map[string]interface{}{},
		},
	})

	if err := worker.Start(); err != nil {
		log.Fatal(err)
	}
}
