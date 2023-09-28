// SPDX-License-Identifier: AGPL-3.0-only

package pushpb

import (
	"context"
	"errors"
	"testing"

	"github.com/gogo/status"
	"github.com/grafana/dskit/middleware"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	"github.com/grafana/mimir/pkg/util/log"
)

const (
	msg = "error occurred"
)

func TestPushErrorDetails(t *testing.T) {
	errorDetails1 := newPushErrorDetails(INGESTION_RATE_LIMIT_ERROR)
	require.NotNil(t, errorDetails1)
	require.Equal(t, INGESTION_RATE_LIMIT_ERROR, errorDetails1.GetErrorType())
	require.Empty(t, errorDetails1.GetOptionalFlags())

	errorDetails2 := newPushErrorDetails(REQUEST_RATE_LIMIT_ERROR, SERVICE_OVERLOAD_STATUS_ON_RATE_LIMIT_ENABLED)
	require.NotNil(t, errorDetails2)
	require.Equal(t, REQUEST_RATE_LIMIT_ERROR, errorDetails2.GetErrorType())
	require.Len(t, errorDetails2.GetOptionalFlags(), 1)
	require.Equal(t, SERVICE_OVERLOAD_STATUS_ON_RATE_LIMIT_ENABLED, errorDetails2.GetOptionalFlags()[0])
}

func TestPushErrorDetailsContains(t *testing.T) {
	pushErrorDetails := newPushErrorDetails(REQUEST_RATE_LIMIT_ERROR, SERVICE_OVERLOAD_STATUS_ON_RATE_LIMIT_ENABLED)
	require.True(t, pushErrorDetails.ContainsOptionalFlag(SERVICE_OVERLOAD_STATUS_ON_RATE_LIMIT_ENABLED))
}

func TestGetPushErrorDetails(t *testing.T) {
	originalErr := errors.New(msg)
	errorDetails, ok := GetPushErrorDetails(originalErr)
	require.False(t, ok)
	require.Nil(t, errorDetails)

	eWS := newErrorWithStatus(
		codes.Unavailable,
		originalErr,
	)
	errorDetails, ok = GetPushErrorDetails(eWS)
	require.False(t, ok)
	require.Nil(t, errorDetails)

	pushErr := NewPushError(codes.Unavailable, originalErr, REQUEST_RATE_LIMIT_ERROR, SERVICE_OVERLOAD_STATUS_ON_RATE_LIMIT_ENABLED)
	errorDetails, ok = GetPushErrorDetails(pushErr)
	require.True(t, ok)
	require.Equal(t, REQUEST_RATE_LIMIT_ERROR, errorDetails.GetErrorType())
	require.Len(t, errorDetails.GetOptionalFlags(), 1)
	require.Equal(t, SERVICE_OVERLOAD_STATUS_ON_RATE_LIMIT_ENABLED, errorDetails.GetOptionalFlags()[0])
}

func TestNewErrorWithStatus(t *testing.T) {
	originalErr := log.DoNotLogError{
		Err: errors.New(msg),
	}
	eWS := newErrorWithStatus(
		codes.Unavailable,
		originalErr,
	)

	require.ErrorIs(t, eWS, originalErr)

	var optional middleware.OptionalLogging
	require.ErrorAs(t, eWS, &optional)
	require.False(t, optional.ShouldLog(context.Background(), 0))

	stat := eWS.GRPCStatus()
	require.NotNil(t, stat)
	require.Equal(t, codes.Unavailable, stat.Code())
}

func TestNewPushError(t *testing.T) {
	originalErr := errors.New(msg)
	pushErr := NewPushError(codes.Unavailable, originalErr, REQUEST_RATE_LIMIT_ERROR, SERVICE_OVERLOAD_STATUS_ON_RATE_LIMIT_ENABLED)
	require.ErrorIs(t, pushErr, originalErr)
	var eWS errorWithStatus
	require.ErrorAs(t, pushErr, &eWS)

	statWithDetails, ok := status.FromError(pushErr)
	require.True(t, ok)
	require.Equal(t, codes.Unavailable, statWithDetails.Code())
	require.Equal(t, msg, statWithDetails.Message())

	errorDetails, ok := GetPushErrorDetails(pushErr)
	require.True(t, ok)
	require.NotNil(t, errorDetails)
	require.Equal(t, REQUEST_RATE_LIMIT_ERROR, errorDetails.GetErrorType())
	require.Len(t, errorDetails.GetOptionalFlags(), 1)
	require.Equal(t, SERVICE_OVERLOAD_STATUS_ON_RATE_LIMIT_ENABLED, errorDetails.GetOptionalFlags()[0])

	anotherPushErr := NewPushError(codes.Unavailable, pushErr, REQUEST_RATE_LIMIT_ERROR, SERVICE_OVERLOAD_STATUS_ON_RATE_LIMIT_ENABLED)
	require.Equal(t, pushErr, anotherPushErr)
}

func TestNewPushErrorWithoutDetails(t *testing.T) {
	originalErr := errors.New(msg)
	pushError := NewPushError(codes.Unavailable, originalErr)
	require.Error(t, pushError)
	stat, ok := status.FromError(pushError)
	require.True(t, ok)
	require.Equal(t, codes.Unavailable, stat.Code())
	require.EqualError(t, originalErr, stat.Message())

	errorDetails, ok := GetPushErrorDetails(pushError)
	require.False(t, ok)
	require.Nil(t, errorDetails)
}
