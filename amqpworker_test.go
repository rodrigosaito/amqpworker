package amqpworker

import (
	"testing"

	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
)

const AMQP_URI = "amqp://admin:admin@localhost:5672"

type MyWorker struct {
	Msg chan string
}

func (self *MyWorker) Work(msg *Message) {
	// Sends the received message body to Msg channel
	self.Msg <- string(msg.Body())
}

func connectRabbitMQ(uri string) *amqp.Connection {
	conn, err := amqp.Dial(AMQP_URI)
	if err != nil {
		panic(err)
	}

	return conn
}

func TestWorker(t *testing.T) {
	received := make(chan string)

	worker := NewAmqpWorker(AMQP_URI)
	defer worker.Stop()
	worker.RegisterConsumer(&Consumer{
		Worker:      &MyWorker{received},
		Concurrency: 4,
		Queue: Queue{
			Name:       "amqpworker.first_queue",
			Durable:    false,
			AutoDelete: true,
			Exclusive:  false,
			NoWait:     false,
			Args:       map[string]string{},
		},
	})

	// Start worker in goroutine so the test main thread is not blocked
	go worker.Start()

	// Wait for worker to be ready
	worker.Ready()

	PublishMessage("amqpworker.first_queue", "Works")

	// Assert that the message sent to the queue is the same as MyWorker received
	assert.Equal(t, "Works", <-received)

	worker.Stop()
}

func TestMultipleConsumers(t *testing.T) {
	received1 := make(chan string)
	received2 := make(chan string)

	worker := NewAmqpWorker(AMQP_URI)
	defer worker.Stop()
	worker.RegisterConsumer(&Consumer{
		Worker:      &MyWorker{received1},
		Concurrency: 1,
		Queue: Queue{
			Name:       "amqpworker.first_queue",
			Durable:    false,
			AutoDelete: true,
			Exclusive:  false,
			NoWait:     false,
		},
	})
	worker.RegisterConsumer(&Consumer{
		Worker:      &MyWorker{received2},
		Concurrency: 1,
		Queue: Queue{
			Name:       "amqpworker.second_queue",
			Durable:    false,
			AutoDelete: true,
			Exclusive:  false,
			NoWait:     false,
		},
	})

	go worker.Start()

	// Wait for worker to be ready
	worker.Ready()

	PublishMessage("amqpworker.first_queue", "Works 1")
	PublishMessage("amqpworker.second_queue", "Works 2")

	// Assert that the message sent to the queue is the same as MyWorker received
	assert.Equal(t, "Works 1", <-received1)
	assert.Equal(t, "Works 2", <-received2)

	worker.Stop()
}

func PublishMessage(queue, message string) error {
	conn, err := amqp.Dial(AMQP_URI)
	if err != nil {
		return err
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	ch.Publish(
		"",    // publish to an exchange
		queue, // routing to 0 or more queues
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			Headers:         amqp.Table{},
			ContentType:     "text/plain",
			ContentEncoding: "",
			Body:            []byte(message),
			DeliveryMode:    1, // 1=non-persistent, 2=persistent
			Priority:        0, // 0-9
			// a bunch of application/implementation-specific fields
		},
	)

	return nil
}
