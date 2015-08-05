package amqpworker

import "github.com/streadway/amqp"

type Message struct {
	delivery amqp.Delivery
}

// Useful for loggin, will return the ConsumerTag of the consumer receiving this message
func (m *Message) ConsumerTag() string {
	return m.delivery.ConsumerTag
}

func (self *Message) Body() []byte {
	return self.delivery.Body
}

func (m *Message) Ack() {
	m.delivery.Ack(false)
}

func (m *Message) Nack() {
	m.delivery.Nack(false, false)
}

func (m *Message) NackAndRequeue() {
	m.delivery.Nack(false, true)
}
