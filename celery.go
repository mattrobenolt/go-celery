package celery

import (
	"log"
	"flag"
	"time"
	"os"
	"os/signal"
	"syscall"
	"runtime"
	"errors"
	"github.com/mattrobenolt/semaphore"
	"fmt"
)

var (
	broker = flag.String("broker", "amqp://guest:guest@localhost:5672//", "Broker")
	queue  = flag.String("Q", "celery", "queue")
	concurrency = flag.Int("c", runtime.NumCPU(), "concurrency")
)

type Task struct {
	Task string
	Id string
	Args []interface{}
	Kwargs map[string]interface{}
	Retries int
	Eta string
	Expires string
	responder Responder
}

func (t *Task) Ack(result interface{}) {
	t.responder.Reply(t.Id, result)
	t.responder.Ack()
}

func (t *Task) Requeue() {
	go func() {
		time.Sleep(time.Second)
		t.responder.Requeue()
	}()
}

func (t *Task) Reject() {
	t.responder.Reject()
}

func (t *Task) String() string {
	return fmt.Sprintf("%s[%s]", t.Task, t.Id)
}

type Worker interface {
	Exec(*Task) (interface{}, error)
}

var registry = make(map[string]Worker)

func RegisterTask(name string, worker Worker) {
	registry[name] = worker
}

var (
	RetryError  = errors.New("Retry task again")
	RejectError = errors.New("Reject task")
)

func Init() {
	flag.Parse()
	fmt.Println("[Tasks]")
	for key, _ := range registry {
		fmt.Printf("  %s\n", key)
	}
	broker := NewBroker(*broker, *queue)
	err := broker.Connect()
	if err != nil {
		panic(err)
	}
	tasks := broker.Consume()
	sem := semaphore.New(*concurrency)
	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGHUP)
		for sig := range c {
			log.Println(sig)
		}
	}()
	for task := range tasks {
		sem.Wait()
		go func(task *Task) {
			if worker, ok := registry[task.Task]; ok {
				log.Printf("Got task from broker: %s", task)
				start := time.Now()
				result, err := worker.Exec(task)
				end := time.Now()
				if err != nil {
					switch err {
					case RetryError:
						task.Requeue()
					default:
						task.Reject()
					}
				} else {
					log.Printf("Task %s succeeded in %s: %s", task, end.Sub(start), result)
					task.Ack(result)
				}
			} else {
				task.Reject()
				log.Printf("Unknown task %s", task.Task)
			}
			sem.Signal()
		}(task)
	}
}
