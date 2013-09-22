package celery

import (
	"strings"
	"time"
	"errors"
	"fmt"
	"encoding/json"
)

var (
	TwoSeconds = 2 * time.Second
	MaximumRetriesError = errors.New("Maximum retries exceeded")
)

type Deliveries chan *Task

type Broker struct {
	conn *Connection
}

func (b *Broker) StartConsuming(q *Queue, rate int) Deliveries {
	b.conn.DeclareQueue(q)
	deliveries := make(Deliveries)
	go func() {
		for {
			messages, err := b.conn.Consume(q, rate)
			if err != nil {
				logger.Error(err)
				time.Sleep(TwoSeconds)
				continue
			}
			for msg := range messages {
				go func(msg *Message) {
					task := &Task{
						Receipt: msg.Receipt,
					}
					switch msg.ContentType {
					case "application/json":
						json.Unmarshal(msg.Body, &task)
					default:
						logger.Warn("Unsupported content-type [%s]", msg.ContentType)
						// msg.Reject(false)
						return
					}
					deliveries <- task
				}(msg)
			}
		}
	}()
	return deliveries
}

func NewBroker(uri string) *Broker {
	var scheme = strings.SplitN(uri, "://", 2)[0]

	if transport, ok := transportRegistry[scheme]; ok {
		driver := transport.Open(uri)
		conn := NewConnection(driver)
		return &Broker{conn}
	}

	panic(fmt.Sprintf("Unknown transport [%s]", scheme))
}
