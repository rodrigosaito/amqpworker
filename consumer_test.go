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
		&MyWorker{received},
		1,
		queue,
	)

	ready := make(chan bool)
	consumer.NotifyReady(ready)

	consumer.Start(conn)

	// Wait until the consumers are ready
	<-ready

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
		&MyWorker{},
		10,
		queue,
	)

	ready := make(chan bool)
	consumer.NotifyReady(ready)

	consumer.Start(conn)

	// Wait until the consumers are ready
	<-ready

	ch, err := conn.Channel()
	assert.Nil(t, err)

	q, err := ch.QueueInspect("some_test_queue")
	assert.Nil(t, err)

	assert.Equal(t, 10, q.Consumers)
}
