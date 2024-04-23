package ingester

import (
	"net/http"

	"go.uber.org/atomic"
)

var (
	more  = make(chan chan int64)
	less  = make(chan chan int64)
	total = atomic.NewInt64(0)
)

func init() {
	go func() {
		for load := range more {
			go func(created chan int64) {
				total.Inc()
				created <- total.Load()
				for {
					select {
					case stopped := <-less:
						total.Dec()
						stopped <- total.Load()
						return
					default:
						// do something heavy in this goroutine that uses an entire CPU core.
					}
				}
			}(load)
		}
	}()
}

func MoreLoadHandler(w http.ResponseWriter, req *http.Request) {
	load := make(chan int64, 1)
	more <- load
	total := <-load
	w.Write([]byte("Load created. Total running: " + string(total)))
}

func LessLoadHandler(w http.ResponseWriter, req *http.Request) {
	load := make(chan int64, 1)
	less <- load
	total := <-load
	w.Write([]byte("Load stopped. Total running: " + string(total)))
}
