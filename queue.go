package celery

type Exchange struct {
	Name string
	Type string
	Durable bool
	DeleteWhenComplete bool
}

type Queue struct {
	Name string
	Durable bool
	DeleteWhenUnused bool
	Ttl int
}

type Binding struct {
	Name string
	Queue *Queue
	Exchange *Exchange
}

type Publishing struct {
	Key string
	Exchange *Exchange
	Body []byte
}

func NewExchange(name string, durable bool) *Exchange {
	return &Exchange{
		Name: name,
		Type: "direct",  // not sure when we'd ever want anything else
		Durable: durable,
		DeleteWhenComplete: !durable,
	}
}

func NewDurableExchange(name string) *Exchange {
	return NewExchange(name, true)
}

func NewQueue(name string, durable bool, ttl int) *Queue {
	return &Queue{
		Name: name,
		Durable: durable,
		DeleteWhenUnused: !durable,
	}
}

func NewDurableQueue(name string) *Queue {
	return NewQueue(name, true, 0)
}

func NewExpiringQueue(name string, ttl int) *Queue {
	return NewQueue(name, false, ttl)
}

func NewBinding(name string, q *Queue, e *Exchange) *Binding {
	return &Binding{
		Name: name,
		Queue: q,
		Exchange: e,
	}
}
