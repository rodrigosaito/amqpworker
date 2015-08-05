package amqpworker

import (
	"log"
	"time"

	"github.com/streadway/amqp"
)

type AmqpWorker struct {
	uri       string
	done      chan bool
	Exchanges []*Exchange
	Consumers []*Consumer
}

func (a *AmqpWorker) RegisterExchange(exchange *Exchange) {
	a.Exchanges = append(a.Exchanges, exchange)
}

func (self *AmqpWorker) RegisterConsumer(consumer *Consumer) {
	self.Consumers = append(self.Consumers, consumer)
}

func (a *AmqpWorker) prepareTopology(conn *amqp.Connection) error {
	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	for _, e := range a.Exchanges {
		if err := ch.ExchangeDeclare(
			e.Name,
			e.Kind,
			e.Durable,
			e.AutoDelete,
			false,
			false,
			nil,
		); err != nil {
			return err
		}
	}

	return nil
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

		if err := self.prepareTopology(conn); err != nil {
			return err
		}

		errorListener := make(chan *amqp.Error, 10)
		conn.NotifyClose(errorListener)

		for _, c := range self.Consumers {
			if err := c.Start(conn); err != nil {
				return err
			}
		}

		<-errorListener
		log.Println("Connection error detected, reconnecting...")
	}

	return nil
}

func (self *AmqpWorker) Stop() {
	/*
		for _, c := range self.Consumers {
			c.Cancel()
		}
	*/
}

func NewAmqpWorker(uri string) *AmqpWorker {
	return &AmqpWorker{
		uri:  uri,
		done: make(chan bool),
	}
}
