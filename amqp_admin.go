package amqpworker

import "github.com/streadway/amqp"

type Exchange struct {
	Name       string
	Kind       string
	Durable    bool
	AutoDelete bool
	Args       map[string]interface{}
}

type Queue struct {
	Name       string
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	NoWait     bool
	Args       map[string]interface{}
}

type Binding struct {
	Exchange   string
	Queue      string
	RoutingKey string
	Args       map[string]interface{}
}

type AmqpAdmin struct {
	Conn *amqp.Connection
}

func (a *AmqpAdmin) DeclareExchange(exchange Exchange) error {
	ch, err := a.Conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	return ch.ExchangeDeclare(
		exchange.Name,
		exchange.Kind,
		exchange.Durable,
		exchange.AutoDelete,
		false,
		false,
		amqp.Table(exchange.Args),
	)
}

func (a *AmqpAdmin) DeclareBinding(binding Binding) error {
	ch, err := a.Conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	return ch.QueueBind(
		binding.Queue,
		binding.RoutingKey,
		binding.Exchange,
		false,
		amqp.Table(binding.Args),
	)
}

func (a *AmqpAdmin) DeclareQueue(queue Queue) error {
	ch, err := a.Conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	_, err = ch.QueueDeclare(
		queue.Name,
		queue.Durable,
		queue.AutoDelete,
		queue.Exclusive,
		queue.NoWait,
		amqp.Table(queue.Args),
	)
	return err
}
