package amqpworker

import (
	"testing"

	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
)

const URI = "amqp://admin:admin@localhost:5672"

type MyWorker struct {
	received chan string
}

func (w *MyWorker) Work(msg *Message) {
	w.received <- string(msg.Body())
}

func TestStartAndConsume(t *testing.T) {
	conn, err := amqp.Dial(URI)
	assert.Nil(t, err)

	queue := &Queue{"some_test_queue", false, true, false, false, map[string]string{}}

	received := make(chan string)
	consumer := NewConsumer(
		func(msg *Message) {
			received <- string(msg.Body())
		},
		1,
		queue,
	)

	consumer.Start(conn)

	// Wait until the consumers are ready
	consumer.WaitReady()

	pub := &Publisher{
		Conn:  conn,
		Queue: queue,
	}

	pub.Publish([]byte("Some Test Message"))

	msg := <-received

	assert.Equal(t, "Some Test Message", msg)

	consumer.Cancel()
}

func TestStartWithConcurrency(t *testing.T) {
	conn, err := amqp.Dial(URI)
	assert.Nil(t, err)

	queue := &Queue{"some_test_queue", false, true, false, false, map[string]string{}}

	consumer := NewConsumer(
		func(msg *Message) {},
		10,
		queue,
	)

	consumer.Start(conn)

	// Wait until the consumers are ready
	consumer.WaitReady()

	ch, err := conn.Channel()
	assert.Nil(t, err)

	q, err := ch.QueueInspect("some_test_queue")
	assert.Nil(t, err)

	assert.Equal(t, 10, q.Consumers)

	consumer.Cancel()
}
