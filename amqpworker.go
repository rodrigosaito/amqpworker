package amqpworker

import (
	log "github.com/Sirupsen/logrus"
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
	log.WithFields(log.Fields{"amqp": self.uri}).Info("Opening amqp connection")

	for {
		conn, err := amqp.Dial(self.uri)
		if err != nil {
			return err
		}
		defer conn.Close()

		errorListener := make(chan *amqp.Error)
		conn.NotifyClose(errorListener)

		for _, c := range self.Consumers {
			c.Start(conn)
		}

		<-errorListener
		log.Info("Connection error detected, reconnecting...")
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
