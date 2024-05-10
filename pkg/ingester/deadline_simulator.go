package ingester

import (
	"fmt"
	"net/http"
	"strconv"
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
	frequency             = atomic.NewInt64(1)
	pushCounter           = atomic.NewInt64(0)
)

func getDuration(req *http.Request) (time.Duration, string) {
	vars := mux.Vars(req)
	durationArg := vars["duration"]
	var (
		duration = 10 * time.Second
		response string
		err      error
	)
	if durationArg == "" {
		response = fmt.Sprintf("DeadlineExceededHandler: duration has not been specified, so the default duration of %s will be used", duration.String())
	} else {
		duration, err = time.ParseDuration(durationArg)
		if err == nil {
			response = fmt.Sprintf("DeadlineExceededHandler: duration of %s will be used", duration.String())
		} else {
			response = fmt.Sprintf("DeadlineExceededHandler: invalid duration '%s' has been specified, so the default duration of 10s will be used", durationArg)
		}
	}
	return duration, response
}

func getFailureFrequency(req *http.Request) (int, string) {
	vars := mux.Vars(req)
	failurePercentageArg := vars["failurePercentage"]
	var (
		failureFrequency = 1
		response         string
	)
	if failurePercentageArg == "" {
		response = fmt.Sprintf("DeadlineExceededHandler: failure percentage has not been specified, so the default failure frequency of %d will be used", failureFrequency)
	} else {
		failurePercentage, err := strconv.Atoi(failurePercentageArg)
		if err == nil {
			failureFrequency = 100 / failurePercentage
			response = fmt.Sprintf("DeadlineExceededHandler: failure frequency of %d corresponding to failure percentage of %d will be used", failureFrequency, failurePercentage)
		} else {
			response = fmt.Sprintf("DeadlineExceededHandler: invalid failure percentage '%s' has been specified, so the default failure frequency %d will be used", failurePercentageArg, failureFrequency)
		}
	}
	return failureFrequency, response
}

func shouldDelayPushRequest() bool {
	if !deadlineExceedEnabled.Load() {
		return false
	}
	pushCounter.Inc()
	pushCount := pushCounter.Load()
	freq := frequency.Load()
	return pushCount%freq == 0
}

func DeadlineExceededHandler(w http.ResponseWriter, req *http.Request) {
	if deadlineExceedEnabled.Load() {
		util.WriteTextResponse(w, "DeadlineExceededHandler is already active. This request will fail.")
		return
	}

	response := make([]string, 0, 4)

	duration, status := getDuration(req)
	response = append(response, status)

	failureFrequency, status := getFailureFrequency(req)
	response = append(response, status)
	frequency.Store(int64(failureFrequency))

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
				pushCounter.Store(0)
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
