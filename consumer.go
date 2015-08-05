package amqpworker

import (
	"errors"
	"log"
	"sync"

	"github.com/streadway/amqp"
)

type WorkerFunc func(msg *Message)

type Queue struct {
	Name       string
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	NoWait     bool
	Args       map[string]string
}

func (q *Queue) Declare(ch *amqp.Channel) error {
	_, err := ch.QueueDeclare(
		q.Name,
		q.Durable,
		q.AutoDelete,
		q.Exclusive,
		q.NoWait,
		nil)

	return err
}

type Consumer struct {
	WorkerFunc  WorkerFunc
	Concurrency int
	Queue       *Queue
	stop        chan bool
	wg          sync.WaitGroup
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

func (c *Consumer) Start(conn *amqp.Connection) {
	for concurrent := 0; concurrent < c.Concurrency; concurrent++ {
		c.wg.Add(1)
		go c.Run(conn)
	}
}

func (c *Consumer) WaitReady() {
	c.wg.Wait()
}

func (c *Consumer) Run(conn *amqp.Connection) error {
	ch, err := conn.Channel()
	defer ch.Close()
	if err != nil {
		return err
	}

	if err = c.Queue.Declare(ch); err != nil {
		return err
	}

	msgs, err := ch.Consume(c.Queue.Name, "", false, false, false, false, nil)
	if err != nil {
		return err
	}

	log.Println("Consumer started")
	c.sendReady()

	for {
		select {
		case m := <-msgs:
			if m.Acknowledger == nil {
				// Possible connection error, stop
				return errors.New("Error has happened, possible connection error.")
			}

			c.WorkerFunc(&Message{m})
		case <-c.stop:
			log.Println("Stoping consumer")
			return nil
		}
	}
}

func (c *Consumer) sendReady() {
	c.wg.Done()
}
