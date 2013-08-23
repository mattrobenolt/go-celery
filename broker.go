package celery

import (
	"log"
	"strings"
	"github.com/streadway/amqp"
	"math/rand"
	"time"
	"encoding/json"
)

type Broker interface {
	Connect() error
	Consume() <-chan *Task
}

type Responder interface {
	Ack()
	Requeue()
	Reject()
}

type AMQPResponder struct {
	d amqp.Delivery
}
func (r *AMQPResponder) Ack() {
	r.d.Ack(false)
}
func (r *AMQPResponder) Requeue() {
	r.d.Reject(true)
}
func (r *AMQPResponder) Reject() {
	r.d.Reject(false)
}

type AMQPBroker struct {
	url, queue string
	conn *amqp.Connection
	channel *amqp.Channel
	deliveries <-chan amqp.Delivery
}

func (b *AMQPBroker) Connect() error {
	var first = true
	var err error

	urls := strings.Split(b.url, ";")
	i := rand.Intn(len(urls))

	for {
		if !first {
			time.Sleep(time.Second)
		}
		first = false

		url := urls[i]
		log.Printf("dialing %s", url)
		b.conn, err = amqp.Dial(url)
		if err != nil {
			log.Printf("Dial: %s", err)
			continue
		}

		log.Printf("Joining channel")
		b.channel, err = b.conn.Channel()
		if err != nil {
			log.Printf("Channel: %s", err)
			continue
		}

		log.Printf("Joining queue")
		b.deliveries, err = b.channel.Consume(
			b.queue,  // queue name
			"",       // consumerTag
			false,    // auto ack
			false,     // exclusive
			false,    // noLocal
			false,    // noWait
			nil,      // arguments
		)
		if err != nil {
			log.Printf("Consume %s", err)
			continue
		}
		log.Printf("Ready")

		return nil
	}
}

func (b *AMQPBroker) Consume() <-chan *Task {
	tasks := make(chan *Task)
	go func() {
		for d := range b.deliveries {
			task := &Task{}

			switch d.ContentType {
			case "application/json":
				fallthrough
			default:
				json.Unmarshal(d.Body, &task)
			}

			task.responder = &AMQPResponder{d}
			tasks <- task
		}
	}()
	return tasks
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
