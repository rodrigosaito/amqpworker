package amqpworker

type Worker interface {
	Work(msg *Message)
}
