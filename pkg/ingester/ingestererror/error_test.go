package ingestererror

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	userID = "test"
)

func TestSafeToWrapError(t *testing.T) {
	err := safeToWrapError("this is a safe to wrap error")
	require.Error(t, err)
	var safe safeToWrap
	require.ErrorAs(t, err, &safe)
}

func TestWrapOrAnnotateWithUser(t *testing.T) {
	userID := "1"
	unsafeErr := errors.New("this is an unsafe error")
	expectedAnnotatedErrMsg := fmt.Sprintf("user=%s: %s", userID, unsafeErr.Error())
	annotatedUnsafeErr := WrapOrAnnotateWithUser(unsafeErr, userID)
	require.Error(t, annotatedUnsafeErr)
	require.EqualError(t, annotatedUnsafeErr, expectedAnnotatedErrMsg)
	require.NotErrorIs(t, annotatedUnsafeErr, unsafeErr)
	require.Nil(t, errors.Unwrap(annotatedUnsafeErr))

	safeErr := safeToWrapError("this is a safe error")
	expectedWrappedErrMsg := fmt.Sprintf("user=%s: %s", userID, safeErr.Error())
	wrappedSafeErr := WrapOrAnnotateWithUser(safeErr, userID)
	require.Error(t, wrappedSafeErr)
	require.EqualError(t, wrappedSafeErr, expectedWrappedErrMsg)
	require.ErrorIs(t, wrappedSafeErr, safeErr)
	require.Equal(t, safeErr, errors.Unwrap(wrappedSafeErr))
}

func TestUnavailableError(t *testing.T) {
	state := "stopping"
	err := NewUnavailableError(state)
	require.Error(t, err)
	expectedMsg := fmt.Sprintf(integerUnavailableMsgFormat, state)
	require.EqualError(t, err, expectedMsg)
	checkSafeToWrap(t, err)

	wrappedErr := WrapOrAnnotateWithUser(err, userID)
	require.ErrorIs(t, wrappedErr, err)
}

func TestInstanceLimitReachedError(t *testing.T) {
	limitErrorMessage := "this is a limit error message"
	err := NewInstanceLimitReachedError(limitErrorMessage)
	require.Error(t, err)
	require.EqualError(t, err, limitErrorMessage)
	checkSafeToWrap(t, err)

	wrappedErr := WrapOrAnnotateWithUser(err, userID)
	require.ErrorIs(t, wrappedErr, err)
}

func TestNewTSDBUnavailableError(t *testing.T) {
	tsdbErr := errors.New("TSDB Head forced compaction in progress and no write request is currently allowed")
	err := NewTSDBUnavailableError(tsdbErr)
	require.Error(t, err)
	require.NotErrorIs(t, err, tsdbErr)
	checkSafeToWrap(t, err)
	require.EqualError(t, err, tsdbErr.Error())
	checkSafeToWrap(t, err)

	wrappedErr := WrapOrAnnotateWithUser(err, userID)
	require.ErrorIs(t, wrappedErr, err)
}

func checkSafeToWrap(t *testing.T, err error) {
	var safeToWrapErr safeToWrap
	require.ErrorAs(t, err, &safeToWrapErr)
}
