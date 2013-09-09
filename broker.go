package celery

import (
	"log"
	"strings"
	"github.com/streadway/amqp"
	"math/rand"
	"time"
	"encoding/json"
	"fmt"
)

type Broker interface {
	Connect() error
	Consume() <-chan *Task
}

type Responder interface {
	Reply(string, interface{})
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
func (r *AMQPResponder) Reply(id string, data interface{}) {
	result := &Result{
		Status: StatusSuccess,
		Result: data,
		Id: id,
	}
	payload, err := json.Marshal(result)
	fmt.Println(string(payload), err)
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

		exchange := "celery"

		log.Printf("got Channel, declaring Exchange (%q)", exchange)
		if err = b.channel.ExchangeDeclare(
			exchange,     // name of the exchange
			"direct",     // type
			true,         // durable
			false,        // delete when complete
			false,        // internal
			false,        // noWait
			nil,          // arguments
		); err != nil {
			log.Print(fmt.Errorf("Exchange Declare: %s", err))
			continue
		}

		log.Printf("declared Exchange, declaring Queue %q", b.queue)
		queue, err := b.channel.QueueDeclare(
			b.queue, // name of the queue
			true,      // durable
			false,     // delete when usused
			false,     // exclusive
			false,     // noWait
			nil,       // arguments
		)
		if err != nil {
			log.Print(fmt.Errorf("Queue Declare: %s", err))
			continue
		}

		key := "celery"

		log.Printf("declared Queue (%q %d messages, %d consumers), binding to Exchange (key %q)",
			queue.Name, queue.Messages, queue.Consumers, key)

		if err = b.channel.QueueBind(
			queue.Name, // name of the queue
			key,        // bindingKey
			exchange,   // sourceExchange
			false,      // noWait
			nil,        // arguments
		); err != nil {
			log.Print(fmt.Errorf("Queue Bind: %s", err))
			continue
		}

		log.Printf("Joining queue")
		b.deliveries, err = b.channel.Consume(
			queue.Name,  // queue name
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
				json.Unmarshal(d.Body, &task)
			default:
				log.Printf("Unsupported content-type [%s]", d.ContentType)
				d.Reject(false)
				continue
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
