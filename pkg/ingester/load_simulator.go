package ingester

import (
	"net/http"

	"go.uber.org/atomic"
)

var (
	moreLoad  = make(chan chan int64)
	lessLoad  = make(chan chan int64)
	totalLoad = atomic.NewInt64(0)
)

func MoreLoadHandler(w http.ResponseWriter, _ *http.Request) {
	load := make(chan int64)
	moreLoad <- load
	currentLoad := <-load
	w.Write([]byte("Load created. Total running: " + string(currentLoad)))
}

func LessLoadHandler(w http.ResponseWriter, _ *http.Request) {
	load := make(chan int64)
	lessLoad <- load
	currentLoad := <-load
	w.Write([]byte("Load stopped. Total running: " + string(currentLoad)))
}

func init() {
	go func() {
		for load := range moreLoad {
			go func(createdLoad chan int64) {
				totalLoad.Inc()
				createdLoad <- totalLoad.Load()
				for {
					select {
					case stoppedLoad := <-lessLoad:
						totalLoad.Dec()
						stoppedLoad <- totalLoad.Load()
						return
					default:
						sum := 0
						for i := 0; i < 10000; i++ {
							sum += i
						}
					}
				}
			}(load)
		}
	}()
}
