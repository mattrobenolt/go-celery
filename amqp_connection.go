package celery

import (
	"strings"
	"github.com/streadway/amqp"
	"time"
	"encoding/json"
)

type AMQPReceipt struct {
	driver *AMQPDriver
	delivery amqp.Delivery
}

func (r *AMQPReceipt) Ack() {
	r.delivery.Ack(false)
}

func (r *AMQPReceipt) Requeue() {
	r.delivery.Reject(true)
}

func (r *AMQPReceipt) Reject() {
	r.delivery.Reject(false)
}

func (r *AMQPReceipt) Reply(id string, data interface{}) {
	result := &Result{
		Status: StatusSuccess,
		Result: data,
		Id: id,
	}

	id = strings.Replace(id, "-", "", -1)

	payload, err := json.Marshal(result)
	if err != nil {
		logger.Error("Error marshalling reply [%s]", err)
		return
	}

	r.driver.Connect()
	err = r.driver.DeclareQueue(NewExpiringQueue(id, 86400000))

	publishing := &Publishing{
		Key: id,
		Exchange: NewDurableExchange(""),
		Body: payload,
	}
	err = r.driver.Publish(publishing)
	if err != nil {
		logger.Error("Error publishing [%s]", err)
		return
	}
}

type AMQPDriver struct {
	uris []string
	alive bool
	i int
	channel *amqp.Channel
}

func (c *AMQPDriver) Connect() (err error) {
	if c.alive {
		return
	}
	defer func() {
		// On next connect, use the next uri
		c.i = (c.i + 1) % len(c.uris)
	}()

	logger.Info("Dialing [%s]", c.uris[c.i])
	conn, err := amqp.Dial(c.uris[c.i])
	if err != nil {
		return
	}

	c.channel, err = conn.Channel()
	if err != nil {
		return
	}
	c.alive = true
	return
}

func (c *AMQPDriver) IsConnected() bool {
	return c.alive
}

func (c *AMQPDriver) DeclareExchange(e *Exchange) error {
	return c.channel.ExchangeDeclare(
		e.Name,
		e.Type,
		e.Durable,
		e.DeleteWhenComplete,
		false,  // internal
		false,  // noWait
		nil,
	)
}

func (c *AMQPDriver) DeclareQueue(q *Queue) error {
	var (
		arguments amqp.Table
	)
	if q.Ttl > 0 {
		arguments = amqp.Table{"x-expires": int32(q.Ttl)}
	}
	_, err := c.channel.QueueDeclare(
		q.Name,
		q.Durable,
		q.DeleteWhenUnused,
		false,  // exclusive
		false,  // noWait
		arguments,
	)
	return err
}

func (c *AMQPDriver) Bind(b *Binding) error {
	return c.channel.QueueBind(
		b.Queue.Name,
		b.Name,
		b.Exchange.Name,
		false,  // noWait
		nil,  // arguments
	)
}

func (c *AMQPDriver) Publish(p *Publishing) error {
	msg := amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		Timestamp: time.Now(),
		ContentType: "application/json",
		Body: p.Body,
	}
	return c.channel.Publish(
		p.Exchange.Name,
		p.Key,
		false,
		false,
		msg,
	)
}

func (c *AMQPDriver) GetMessages(q *Queue, rate int) (<-chan *Message, error) {
	c.channel.Qos(rate, 0, false)
	deliveries, err := c.channel.Consume(
		q.Name,
		"",     // consumerTag
		false,  // autoAck
		false,  // exclusive
		false,  // noLocal
		false,  // noWait
		nil,    // arguments
	)
	if err != nil {
		c.alive = false
		return nil, err
	}

	messages := make(chan *Message)
	go func() {
		for d := range deliveries {
			messages <- &Message{
				ContentType: d.ContentType,
				Body: d.Body,
				Receipt: &AMQPReceipt{
					driver: c,
					delivery: d,
				},
			}
		}
		// connection was lost
		c.alive = false
		close(messages)
	}()
	return messages, nil
}

type AMQP struct {}
func (a *AMQP) Open(uri string) Driver {
	return &AMQPDriver{
		uris: strings.Split(uri, ";"),
	}
}

func init() {
	RegisterTransport("amqp", &AMQP{})
}
