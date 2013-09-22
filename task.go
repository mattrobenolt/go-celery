package celery

import (
	"fmt"
	"time"
)

type Receipt interface {
	Reply(string, interface{})
	Ack()
	Requeue()
	Reject()
}

type Task struct {
	Task string
	Id string
	Args []interface{}
	Kwargs map[string]interface{}
	Retries int
	Eta string
	Expires string
	Receipt Receipt
}

func (t *Task) Ack(result interface{}) {
	if result != nil {
		t.Receipt.Reply(t.Id, result)
	}
	t.Receipt.Ack()
}

func (t *Task) Requeue() {
	go func() {
		time.Sleep(time.Second)
		t.Receipt.Requeue()
	}()
}

func (t *Task) Reject() {
	t.Receipt.Reject()
}

func (t *Task) String() string {
	return fmt.Sprintf("%s[%s]", t.Task, t.Id)
}
