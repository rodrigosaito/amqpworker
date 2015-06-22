package amqpworker

import "github.com/streadway/amqp"

type Publisher struct {
	Conn     *amqp.Connection
	Exchange string
	Queue    *Queue
}

func (p *Publisher) Publish(message []byte) error {
	ch, err := p.Conn.Channel()
	if err != nil {
		return err
	}

	return ch.Publish(
		p.Exchange,
		p.Queue.Name,
		false,
		false,
		amqp.Publishing{
			Headers:         amqp.Table{},
			ContentType:     "text/plain",
			ContentEncoding: "",
			Body:            message,
			DeliveryMode:    1, // 1=non-persistent, 2=persistent
			Priority:        0, // 0-9
			// a bunch of application/implementation-specific fields
		},
	)
}
