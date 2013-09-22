package celery

import (
	"strings"
	"time"
	"errors"
)

var (
	TwoSeconds = 2 * time.Second
	MaxiumRetriesError = errors.New("Maximum retries exceeded")
)

type Broker interface {
	Connect() error
	ConnectMaxRetries(uint64) error
	Consume(int) <-chan *Task
}

type Responder interface {
	Reply(string, interface{})
	Ack()
	Requeue()
	Reject()
}

func NewBroker(broker, queue string) Broker {
	if strings.HasPrefix(broker, "amqp://") {
		return &AMQPBroker{
			url: broker,
			queue: queue,
		}
	}
	panic("Unknown broker")
}
