// SPDX-License-Identifier: AGPL-3.0-only

package mimir

import (
	"context"
	"errors"
	"io"

	"github.com/grafana/dskit/services"
)

func closeWithError(err error, cleanup io.Closer) error {
	if cleanup == nil {
		return err
	}

	return errors.Join(err, cleanup.Close())
}

func wrapRulerServiceWithCleanup(rulerService services.Service, cleanup io.Closer) services.Service {
	rulerFailureCh := make(chan error, 1)
	var unregisterListener func()
	var rulerFailure error

	unregister := func() {
		if unregisterListener != nil {
			unregisterListener()
			unregisterListener = nil
		}
	}

	closeOnStartupFailure := func(startErr error) error {
		rawFailure := rulerService.FailureCase()
		stopErr := services.StopAndAwaitTerminated(context.Background(), rulerService)
		unregister()
		cleanupErr := cleanup.Close()
		return combineRulerCleanupErrors(startErr, rawFailure, stopErr, cleanupErr)
	}

	return services.NewBasicService(func(serviceContext context.Context) error {
		unregisterListener = rulerService.AddListener(services.NewListener(nil, nil, nil, nil, func(_ services.State, failure error) {
			select {
			case rulerFailureCh <- failure:
			default:
			}
		}))

		if err := rulerService.StartAsync(serviceContext); err != nil {
			return closeOnStartupFailure(err)
		}
		if err := rulerService.AwaitRunning(serviceContext); err != nil {
			return closeOnStartupFailure(err)
		}
		return nil
	}, func(serviceContext context.Context) error {
		select {
		case <-serviceContext.Done():
			return nil
		case rulerFailure = <-rulerFailureCh:
			return nil
		}
	}, func(error) error {
		stopErr := services.StopAndAwaitTerminated(context.Background(), rulerService)
		unregister()
		cleanupErr := cleanup.Close()
		return combineRulerCleanupErrors(nil, rulerFailure, stopErr, cleanupErr)
	})
}

func combineRulerCleanupErrors(primaryErr, rulerFailure, stopErr, cleanupErr error) error {
	errs := []error{primaryErr}
	errs = addIfNotDuplicate(errs, rulerFailure, primaryErr)
	errs = addIfNotDuplicate(errs, stopErr, primaryErr, rulerFailure)
	errs = addIfNotDuplicate(errs, cleanupErr, primaryErr, rulerFailure, stopErr)
	return errors.Join(errs...)
}

func addIfNotDuplicate(errs []error, err error, existing ...error) []error {
	if err == nil {
		return errs
	}
	for _, existingErr := range existing {
		if existingErr != nil && (errors.Is(err, existingErr) || errors.Is(existingErr, err)) {
			return errs
		}
	}
	return append(errs, err)
}
