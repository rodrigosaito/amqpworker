package amqpworker

import (
	"os"
	"testing"

	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
)

var amqpURL string

func init() {
	amqpURL = "amqp://guest:guest@localhost:5672"

	if os.Getenv("AMQP_URL") != "" {
		amqpURL = os.Getenv("AMQP_URL")
	}
}

func TestDeclareExchange(t *testing.T) {
	conn, err := amqp.Dial(amqpURL)
	assert.Nil(t, err)
	defer conn.Close()

	admin := AmqpAdmin{conn}

	err = admin.DeclareExchange(Exchange{
		Name:       "some_exchange_test_name",
		Kind:       "topic",
		Durable:    true,
		AutoDelete: true,
		Args:       map[string]interface{}{},
	})

	// Couldn't find a better way to assert this, there is no way to check Exchange on amqp package
	assert.Nil(t, err)
}

func TestDeclareQueue(t *testing.T) {
	conn, err := amqp.Dial(amqpURL)
	assert.Nil(t, err)
	defer conn.Close()

	admin := AmqpAdmin{conn}

	err = admin.DeclareQueue(Queue{
		Name:       "some_queue_test_name",
		Durable:    false,
		AutoDelete: true,
		Exclusive:  false,
		NoWait:     false,
		Args:       map[string]interface{}{},
	})

	// Couldn't find a better way to assert this, QueueInspect returns only the number of Messages and Consumers
	assert.Nil(t, err)
}

func TestDeclareBinding(t *testing.T) {
	// Skipped until I find a better way to test all this stuff
	t.Skip()

	conn, err := amqp.Dial(amqpURL)
	assert.Nil(t, err)
	defer conn.Close()

	admin := AmqpAdmin{conn}

	err = admin.DeclareBinding(Binding{})

	// Couldn't find a better way to assert this, there is no way to check Binding on amqp package
	assert.Nil(t, err)
}
