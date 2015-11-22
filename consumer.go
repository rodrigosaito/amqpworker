package amqpworker

import (
	"log"
	"sync"

	"github.com/streadway/amqp"
)

type WorkerFunc func(msg *Message)

type ConfigurerFunc func(admin *AmqpAdmin) error

type Consumer struct {
	WorkerFunc     WorkerFunc
	ConfigurerFunc ConfigurerFunc
	Concurrency    int
	Queue          *Queue
	stop           chan bool
	wg             sync.WaitGroup
}

func NewConsumer(worker WorkerFunc, concurrency int, queue *Queue) *Consumer {
	c := &Consumer{
		WorkerFunc:  worker,
		Concurrency: concurrency,
		Queue:       queue,
	}

	c.init()

	return c
}

func (c *Consumer) init() {
	c.stop = make(chan bool, c.Concurrency)
}

func (c *Consumer) Cancel() {
	log.Println("Cancelling...")
	for con := 0; con < c.Concurrency; con++ {
		c.stop <- true
	}
}

func (c *Consumer) Start(conn *amqp.Connection, config Config) error {
	for concurrent := 0; concurrent < c.Concurrency; concurrent++ {
		ch, err := conn.Channel()
		if err != nil {
			return err
		}
		ch.Qos(config.ChannelPrefetch, 0, false)

		if c.ConfigurerFunc != nil {
			admin := &AmqpAdmin{conn}
			if err := c.ConfigurerFunc(admin); err != nil {
				return err
			}
		}

		msgs, err := ch.Consume(c.Queue.Name, "", false, false, false, false, nil)
		if err != nil {
			return err
		}

		c.wg.Add(1)
		go c.Run(msgs, config)
	}

	return nil
}

func (c *Consumer) WaitReady() {
	c.wg.Wait()
}

func (c *Consumer) Run(msgs <-chan amqp.Delivery, config Config) {

	config.Logger.Println("Consumer started")
	c.sendReady()

	for {
		select {
		case m := <-msgs:
			if m.Acknowledger == nil {
				// Possible connection error, stop
				return
			}

			c.WorkerFunc(&Message{m})
		case <-c.stop:
			log.Println("Stoping consumer")
			return
		}
	}
}

func (c *Consumer) sendReady() {
	c.wg.Done()
}
