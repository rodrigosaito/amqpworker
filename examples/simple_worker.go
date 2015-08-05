package main

import (
	"log"

	"github.com/rodrigosaito/amqpworker"
)

const AMQP_URI = "amqp://admin:admin@localhost:5672"

type MyWorker struct {
	Msg chan string
}

func (self *MyWorker) Work(msg *amqpworker.Message) {
	log.Println(msg.ConsumerTag())
	log.Println(string(msg.Body()))
	msg.Ack()
}

func main() {
	worker := amqpworker.NewAmqpWorker(AMQP_URI)
	defer worker.Stop()
	worker.RegisterConsumer(&amqpworker.Consumer{
		Worker:      &MyWorker{},
		Concurrency: 4,
		Queue: &amqpworker.Queue{
			Name:       "test_queue",
			Durable:    false,
			AutoDelete: true,
			Exclusive:  false,
			NoWait:     false,
			Args:       map[string]string{},
		},
	})

	worker.Start()
}
