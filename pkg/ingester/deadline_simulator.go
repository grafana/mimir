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
	requestDelay          atomic.Duration
	additionalCPUUsage    = atomic.Bool{}
)

func getTimeDuration(arg string, defaultDuration time.Duration, req *http.Request) (time.Duration, string) {
	vars := mux.Vars(req)
	durationArg := vars[arg]
	var (
		duration = defaultDuration
		response string
		err      error
	)
	if durationArg == "" {
		response = fmt.Sprintf("DeadlineExceededHandler: duration has not been specified, so the default duration of %s will be used for \"%s\"", defaultDuration.String(), arg)
		return defaultDuration, response
	}

	duration, err = time.ParseDuration(durationArg)
	if err == nil {
		response = fmt.Sprintf("DeadlineExceededHandler: duration of %s will be used for \"%s\"", duration.String(), arg)
		return duration, response
	}
	response = fmt.Sprintf("DeadlineExceededHandler: invalid duration '%s' has been specified, so the default duration of %s will be used for \"%s\"", defaultDuration.String(), durationArg)
	return defaultDuration, response
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

func getCPUUsage(req *http.Request) (bool, string) {
	vars := mux.Vars(req)
	cpuUsageArg := vars["cpuUsage"]
	if cpuUsageArg == "" {
		response := "DeadlineExceededHandler: cpu usage has not been specified, so the default value false"
		return false, response
	}
	if cpuUsage, err := strconv.ParseBool(cpuUsageArg); err != nil {
		response := fmt.Sprintf("DeadlineExceededHandler: cpu usage %v will be used", cpuUsage)
		return cpuUsage, response
	}
	response := fmt.Sprintf("DeadlineExceededHandler: invalid cpu usage '%s' has been specified, so the default value false", cpuUsageArg)
	return false, response
}

func getNextPushRequestDelay() time.Duration {
	if !deadlineExceedEnabled.Load() {
		return 0
	}
	pushCounter.Inc()
	pushCount := pushCounter.Load()
	freq := frequency.Load()
	if pushCount%freq == 0 {
		return requestDelay.Load()
	}
	return 0
}

func DeadlineExceededHandler(w http.ResponseWriter, req *http.Request) {
	if deadlineExceedEnabled.Load() {
		util.WriteTextResponse(w, "DeadlineExceededHandler is already active. This request will fail.")
		return
	}

	response := make([]string, 0, 6)

	duration, status := getTimeDuration("duration", 10*time.Second, req)
	response = append(response, status)

	delay, status := getTimeDuration("delay", 5*time.Second, req)
	response = append(response, status)
	requestDelay.Store(delay)

	failureFrequency, status := getFailureFrequency(req)
	response = append(response, status)
	frequency.Store(int64(failureFrequency))

	cpuUsage, status := getCPUUsage(req)
	response = append(response, status)
	additionalCPUUsage.Store(cpuUsage)

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
						if additionalCPUUsage.Load() {
							fibonacci(40)
						}
					}
				}
			}(started)
		}
	}()
}
