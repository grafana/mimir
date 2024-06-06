package blockbuilder

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestKafkaCommitMetaMarshalling(t *testing.T) {
	lastOffset := int64(892734)
	currEnd := int64(598237948)

	lo, ce, err := unmarshallCommitMeta(marshallCommitMeta(lastOffset, currEnd))
	require.NoError(t, err)
	require.Equal(t, lastOffset, lo)
	require.Equal(t, currEnd, ce)

	// Unsupported version
	_, _, err = unmarshallCommitMeta("2,2,3")
	require.Error(t, err)
	require.Equal(t, "unsupported commit meta version 2", err.Error())
}
