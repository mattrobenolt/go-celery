package celery

import (
	"strings"
	"github.com/streadway/amqp"
	"math"
	"math/rand"
	"encoding/json"
	"time"
)

type AMQPResponder struct {
	d amqp.Delivery
	b *AMQPBroker
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

	id = strings.Replace(id, "-", "", -1)

	payload, err := json.Marshal(result)
	logger.Debug("declared Exchange, declaring Queue %q", id)
	_, err = r.b.channel.QueueDeclare(
		id, // name of the queue
		false,      // durable
		true,     // delete when usused
		false,     // exclusive
		false,     // noWait
		amqp.Table{"x-expires": int32(86400000)},       // arguments
	)
	if err != nil {
		logger.Error("Queue Declare: %s", err)
		return
	}

	msg := amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		Timestamp:    time.Now(),
		ContentType:  "application/json",
		Body:         payload,
	}

	r.b.channel.Publish(
		"",  // exchange
		id,  // key
		false, // mandatory
		false,  // immediate
		msg,  // body
	)
}

type AMQPBroker struct {
	url, queue string
	conn *amqp.Connection
	channel *amqp.Channel
	deliveries <-chan amqp.Delivery
}

func (b *AMQPBroker) Connect() error {
	return b.ConnectMaxRetries(math.MaxUint64-1)
}

func (b *AMQPBroker) ConnectMaxRetries(retries uint64) (err error) {
	urls := strings.Split(b.url, ";")
	i := rand.Intn(len(urls))
	backoff := 0 * time.Second

	for retries++; retries > 0; retries-- {
		if backoff != 0 {
			logger.Error("Retrying in %s...", backoff)
			time.Sleep(backoff)
			backoff += TwoSeconds
		} else {
			backoff = TwoSeconds
		}

		url := urls[i]
		logger.Debug("dialing %s", url)
		b.conn, err = amqp.Dial(url)
		if err != nil {
			logger.Error("Dial: %s", err)
			continue
		}

		logger.Debug("Joining channel")
		b.channel, err = b.conn.Channel()
		if err != nil {
			logger.Error("Channel: %s", err)
			continue
		}

		exchange := "celery"

		logger.Debug("got Channel, declaring Exchange (%q)", exchange)
		if err = b.channel.ExchangeDeclare(
			exchange,     // name of the exchange
			"direct",     // type
			true,         // durable
			false,        // delete when complete
			false,        // internal
			false,        // noWait
			nil,          // arguments
		); err != nil {
			logger.Error("Exchange Declare: %s", err)
			continue
		}

		logger.Debug("declared Exchange, declaring Queue %q", b.queue)
		queue, err := b.channel.QueueDeclare(
			b.queue, // name of the queue
			true,      // durable
			false,     // delete when usused
			false,     // exclusive
			false,     // noWait
			nil,       // arguments
		)
		if err != nil {
			logger.Error("Queue Declare: %s", err)
			continue
		}

		key := "celery"

		logger.Debug("declared Queue (%q %d messages, %d consumers), binding to Exchange (key %q)",
			queue.Name, queue.Messages, queue.Consumers, key)

		if err = b.channel.QueueBind(
			b.queue, // name of the queue
			key,        // bindingKey
			exchange,   // sourceExchange
			false,      // noWait
			nil,        // arguments
		); err != nil {
			logger.Error("Queue Bind: %s", err)
			continue
		}

		return nil
	}

	return MaxiumRetriesError
}

func (b *AMQPBroker) Consume(rate int) <-chan *Task {
	var err error
	b.channel.Qos(rate, 0, false)
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
		logger.Error("Consume %s", err)
		return nil
	}

	tasks := make(chan *Task)
	go func() {
		for d := range b.deliveries {
			go func(d amqp.Delivery) {
				task := &Task{}

				switch d.ContentType {
				case "application/json":
					json.Unmarshal(d.Body, &task)
				default:
					logger.Warn("Unsupported content-type [%s]", d.ContentType)
					d.Reject(false)
					return
				}

				task.responder = &AMQPResponder{d, b}
				tasks <- task
			}(d)
		}
	}()
	return tasks
}