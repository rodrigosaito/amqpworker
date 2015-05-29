package amqpworker

import (
	"fmt"

	"github.com/streadway/amqp"
)

type Worker interface {
	Work(msg amqp.Delivery)
}

func queueDeclare(ch *amqp.Channel, q Queue) error {
	_, err := ch.QueueDeclare(
		q.Name,
		q.Durable,
		q.AutoDelete,
		q.Exclusive,
		q.NoWait,
		nil)

	return err
}

type AmqpWorker struct {
	uri       string
	done      chan bool
	ready     chan bool
	Consumers []*Consumer
}

func (self *AmqpWorker) RegisterConsumer(consumer *Consumer) {
	self.Consumers = append(self.Consumers, consumer)
}

func (self *AmqpWorker) Start() error {
	conn, err := amqp.Dial(self.uri)
	if err != nil {
		return err
	}
	defer conn.Close()

	for _, c := range self.Consumers {
		go self.startListening(conn, c)
	}

	<-self.done

	return nil
}

func (self *AmqpWorker) startListening(conn *amqp.Connection, c *Consumer) {
	ch, err := conn.Channel()
	defer ch.Close()
	if err != nil {
		panic(err)
	}

	err = queueDeclare(ch, c.Queue)
	if err != nil {
		panic(err)
	}

	msgs, err := ch.Consume(c.Queue.Name, "", true, false, false, false, nil)
	if err != nil {
		panic(err)
	}

	self.ready <- true

	for {
		select {
		case m := <-msgs:
			c.Worker.Work(m)
		case <-self.done:
			return
			//default:
			//	fmt.Println("default")
		}
	}

	fmt.Println("End")
}

func (self *AmqpWorker) Stop() {
	self.done <- true
}

func (self *AmqpWorker) Ready() {
	consumers := len(self.Consumers)
	counter := 0
	for _ = range self.ready {
		counter++
		if counter == consumers {
			return
		}
	}
}

func NewAmqpWorker(uri string) *AmqpWorker {
	return &AmqpWorker{
		uri:   uri,
		done:  make(chan bool),
		ready: make(chan bool),
	}
}

type Consumer struct {
	Worker      Worker
	Concurrency int
	Queue       Queue
}

type Queue struct {
	Name       string
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	NoWait     bool
	Args       map[string]string
}
