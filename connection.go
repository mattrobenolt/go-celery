package celery

type Transport interface {
	Open(string) Driver
}

type Driver interface {
	Connect() error
	DeclareExchange(*Exchange) error
	DeclareQueue(*Queue) error
	Bind(*Binding) error
	GetMessages(*Queue, int) (<-chan *Message, error)
	Publish(*Publishing) error
	IsConnected() bool
}

type Connection struct {
	driver Driver
}

func (c *Connection) Ping() (err error) {
	if c.driver.IsConnected() {
		return
	}
	err = c.driver.Connect()
	if err != nil {
		// lol, not sure what we should do here
		logger.Error("Error connecting [%s]", err)
		return
	}
	return
}

func (c *Connection) DeclareExchange(e *Exchange) error {
	err := c.Ping()
	if err != nil {
		return err
	}
	return c.driver.DeclareExchange(e)
}

func (c *Connection) DeclareQueue(q *Queue) error {
	err := c.Ping()
	if err != nil {
		return err
	}
	logger.Info("Declaring queue [%s]", q.Name)
	return c.driver.DeclareQueue(q)
}

func (c *Connection) Bind(b *Binding) error {
	err := c.Ping()
	if err != nil {
		return err
	}
	return c.driver.Bind(b)
}

func (c *Connection) Consume(q *Queue, rate int) (<-chan *Message, error) {
	err := c.Ping()
	if err != nil {
		return nil, err
	}
	logger.Info("Consuming from [%s]", q.Name)
	return c.driver.GetMessages(q, rate)
}

func NewConnection(driver Driver) *Connection {
	return &Connection{driver: driver}
}

var transportRegistry = make(map[string]Transport)

func RegisterTransport(name string, t Transport) {
	transportRegistry[name] = t
}
