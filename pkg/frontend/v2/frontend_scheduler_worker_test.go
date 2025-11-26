// SPDX-License-Identifier: AGPL-3.0-only

package v2

import "testing"

// helper new frontendSchedulerWorker
// helper new loop?

func Test_SchedulerLoop(t *testing.T) {
	// bunch of t.Run()

	// table tests for:
	// 1. init fails
	// 2. receive message ok
	// 3. receive message ok then receive cancel
	// 4. receive message err
	// 5. receive message err | status shutting down
	// 5. receive message err | status other

	// for each test
	// create the frontendSchedulerWorker   // func newFrontendSchedulerWorker(
	// create the scheduler that we can control
	// start schedulerloop in goroutine - capture error/other/ way to confirm message processed ok?
	// send the appropriate message(s) from scheduler.
	// assert the things.
}

// func (w *frontendSchedulerWorker) schedulerLoop(loop schedulerpb.SchedulerForFrontend_FrontendLoopClient) error {
