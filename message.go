package amqpworker

import "github.com/streadway/amqp"

type Message struct {
	delivery amqp.Delivery
}

func (self *Message) Body() []byte {
	return self.delivery.Body
}
