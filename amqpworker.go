package amqpworker

import (
	log "github.com/Sirupsen/logrus"
	"github.com/streadway/amqp"
)

func queueDeclare(ch *amqp.Channel, q *Queue) error {
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

	self.ready = make(chan bool, len(self.Consumers))
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
			go self.startListening(conn, c)
		}

		<-errorListener
		log.Info("Connection error detected, reconnecting...")
	}

	return nil
}

func (self *AmqpWorker) startListening(conn *amqp.Connection, c *Consumer) {
	ch, err := conn.Channel()
	defer ch.Close()
	if err != nil {
		panic(err)
	}
	log.Debug("Channel created")

	err = queueDeclare(ch, c.Queue)
	if err != nil {
		panic(err)
	}
	log.Debug("Queue declared")

	msgs, err := ch.Consume(c.Queue.Name, "", true, false, false, false, nil)
	if err != nil {
		panic(err)
	}

	//self.ready <- true

	log.WithFields(log.Fields{
		"queue": c.Queue.Name,
	}).Info("Worker started")

	for {
		select {
		case m := <-msgs:
			if m.Acknowledger == nil {
				// Possible connection error, stop goroutine
				return
			}

			c.Worker.Work(&Message{m})
		case <-self.done:
			return
			//default:
			//fmt.Println("default")
		}
	}
}

func (self *AmqpWorker) Stop() {
	self.done <- true
}

func (self *AmqpWorker) Ready() {
	consumers := len(self.Consumers)
	counter := 0
	log.Println("consumers", consumers)
	for _ = range self.ready {
		log.Println("count")
		counter++
		if counter == consumers {
			return
		}
	}
}

func NewAmqpWorker(uri string) *AmqpWorker {
	return &AmqpWorker{
		uri:  uri,
		done: make(chan bool),
	}
}
