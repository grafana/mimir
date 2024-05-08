package ingester

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/mux"

	"github.com/grafana/mimir/pkg/util"

	"go.uber.org/atomic"
)

var (
	deadlineStart         = make(chan chan int64)
	deadlineEnd           = make(chan chan int64)
	count                 = atomic.NewInt64(0)
	deadlineExceedEnabled = atomic.NewBool(false)
)

func DeadlineExceededHandler(w http.ResponseWriter, req *http.Request) {
	load := make(chan int64)
	moreLoad <- load
	currentLoad := <-load
	w.Write([]byte(fmt.Sprintf("Load created. Total running: %d", currentLoad)))

	vars := mux.Vars(req)
	durationArg := vars["duration"]
	var (
		duration = 10 * time.Second
		response = make([]string, 0, 5)
		err      error
	)
	if durationArg == "" {
		response = append(response, "DeadlineExceededHandler: duration has not been specified, so the default duration of 10s will be used")
	} else {
		duration, err = time.ParseDuration(durationArg)
		if err != nil {
			response = append(response, fmt.Sprintf("DeadlineExceededHandler: invalid duration '%s' has been specified, so the default duration of 10s will be used", durationArg))
		}
	}

	start := make(chan int64)
	deadlineStart <- start
	count := <-start
	response = append(response, fmt.Sprintf("DeadlineExceededHandler started %dth time", count))

	time.Sleep(duration)

	end := make(chan int64)
	deadlineEnd <- end
	count = <-end
	msg := fmt.Sprintf("DeadlineExceededHandler stopped %dth time after %v", count, duration)
	response = append(response, msg)
	util.WriteTextResponse(w, strings.Join(response, "\n"))
}

func init() {
	go func() {
		for started := range deadlineStart {
			go func(started chan int64) {
				count.Inc()
				started <- count.Load()
				deadlineExceedEnabled.Store(true)
				for {
					select {
					case ended := <-deadlineEnd:
						ended <- count.Load()
						deadlineExceedEnabled.Store(false)
						return
					default:
						fibonacci(40)
					}
				}
			}(started)
		}
	}()
}
