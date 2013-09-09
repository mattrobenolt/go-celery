package main

import "celery"

type Adder struct {}
func (a *Adder) Exec(task *celery.Task) (result interface{}, err error) {
    sum := float64(0)
    for _, arg := range task.Args {
        sum += arg.(float64)
    }
    result = sum
    return
}

func main() {
    celery.RegisterTask("myapp.add", &Adder{})
    celery.Init()
}
