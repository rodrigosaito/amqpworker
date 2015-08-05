package amqpworker

import (
	"log"
	"time"

	"github.com/streadway/amqp"
)

type AmqpWorker struct {
	uri       string
	done      chan bool
	Consumers []*Consumer
}

func (self *AmqpWorker) RegisterConsumer(consumer *Consumer) {
	self.Consumers = append(self.Consumers, consumer)
}

func (self *AmqpWorker) Start() error {
	log.Printf("Opening amqp connection uri=%v", self.uri)

	for {
		conn, err := amqp.Dial(self.uri)
		if err != nil {
			log.Println("Error conecting to rabbitmq, retrying. Message:", err)
			time.Sleep(1 * time.Second)
			continue
		}
		defer conn.Close()

		errorListener := make(chan *amqp.Error)
		conn.NotifyClose(errorListener)

		for _, c := range self.Consumers {
			c.Start(conn)
		}

		<-errorListener
		log.Println("Connection error detected, reconnecting...")
	}

	return nil
}

func (self *AmqpWorker) Stop() {
	for _, c := range self.Consumers {
		c.Cancel()
	}
}

func NewAmqpWorker(uri string) *AmqpWorker {
	return &AmqpWorker{
		uri:  uri,
		done: make(chan bool),
	}
}
