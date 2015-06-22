package amqpworker

import (
	"testing"

	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
)

func TestMessageBody(t *testing.T) {
	body := []byte("Some Message")

	m := Message{
		delivery: amqp.Delivery{
			Body: body,
		},
	}

	assert.Equal(t, body, m.Body())
}
