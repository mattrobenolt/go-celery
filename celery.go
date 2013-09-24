package celery

import (
	"flag"
	"time"
	"os"
	"os/signal"
	"runtime"
	"errors"
	"fmt"
	"encoding/json"
	"sync"
)

var (
	broker = flag.String("broker", "amqp://guest:guest@localhost:5672//", "Broker")
	queue  = flag.String("Q", "celery", "queue")
	concurrency = flag.Int("c", runtime.NumCPU(), "concurrency")
)

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

func shutdown(status int) {
	fmt.Println("\nceleryd: Warm shutdown")
	os.Exit(status)
}

func Init() {
	flag.Parse()
	SetupLogging()

	runtime.GOMAXPROCS(*concurrency)
	broker := NewBroker(*broker)
	fmt.Println("")
	fmt.Println("[Tasks]")
	for key, _ := range registry {
		fmt.Printf("  %s\n", key)
	}
	fmt.Println("")
	hostname, _ := os.Hostname()
	logger.Warn("celery@%s ready.", hostname)

	queue := NewDurableQueue(*queue)
	deliveries := broker.StartConsuming(queue, *concurrency)
	var wg sync.WaitGroup
	draining := false
	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt)
		for _ = range c {
			// If interrupting for the second time,
			// terminate un-gracefully
			if draining {
				shutdown(1)
			}
			fmt.Println("\nceleryd: Hitting Ctrl+C again will terminate all running tasks!")
			// Gracefully shut down
			draining = true
			go func() {
				wg.Wait()
				shutdown(0)
			}()
		}
	}()
	for !draining {
		task := <- deliveries
		wg.Add(1)
		go func(task *Task) {
			defer wg.Done()
			if worker, ok := registry[task.Task]; ok {
				logger.Info("Got task from broker: %s", task)
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
					logger.Info(func()string {
						res, _ := json.Marshal(result)
						return fmt.Sprintf("Task %s succeeded in %s: %s", task, end.Sub(start), res)
					})
					task.Ack(result)
				}
			} else {
				task.Reject()
				logger.Error("Received unregistered task of type '%s'.\nThe message has been ignored and discarded.\n", task.Task)
			}
		}(task)
	}
}
