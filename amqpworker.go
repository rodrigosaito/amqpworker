package amqpworker

import (
	"log"
	"os"
	"time"

	"github.com/streadway/amqp"
)

type AmqpWorker struct {
	uri         string
	done        chan bool
	config      Config
	Consumers   []*Consumer
	PrepareFunc func(admin *AmqpAdmin) error
}

func NewAmqpWorker(uri string, config Config) *AmqpWorker {
	if config.Logger == nil {
		config.Logger = log.New(os.Stdout, "[amqpworker] ", log.LstdFlags)
	}

	return &AmqpWorker{
		uri:    uri,
		done:   make(chan bool),
		config: config,
	}
}

func (self *AmqpWorker) RegisterConsumer(consumer *Consumer) {
	self.Consumers = append(self.Consumers, consumer)
}

func (a *AmqpWorker) prepare(conn *amqp.Connection) error {
	if a.PrepareFunc == nil {
		return nil
	}

	return a.PrepareFunc(&AmqpAdmin{conn})
}

func (self *AmqpWorker) Start() error {
	self.config.Logger.Printf("Opening amqp connection uri=%v", self.uri)

	for {
		conn, err := amqp.Dial(self.uri)
		if err != nil {
			self.config.Logger.Println("Connection error detected, reconnecting...")
			time.Sleep(1 * time.Second)
			continue
		}
		defer conn.Close()

		if err := self.prepare(conn); err != nil {
			return err
		}

		errorListener := make(chan *amqp.Error, 10)
		conn.NotifyClose(errorListener)

		for _, c := range self.Consumers {
			if err := c.Start(conn, self.config); err != nil {
				return err
			}
		}

		<-errorListener
		self.config.Logger.Println("Connection error detected, reconnecting...")
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

type Config struct {
	ChannelPrefetch int
	Logger          Logger
}
