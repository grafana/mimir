// SPDX-License-Identifier: AGPL-3.0-only

package util

import (
	"flag"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTrackRegisteredFlags(t *testing.T) {
	const (
		prefix   = "testing."
		flagName = "flag"
	)

	fs := flag.NewFlagSet("test", flag.PanicOnError)
	var previous, registered, nonPrefixed string
	fs.StringVar(&previous, "previous.flag", "previous", "")

	rf := TrackRegisteredFlags(prefix, fs, func(prefix string, _ *flag.FlagSet) {
		fs.StringVar(&registered, prefix+flagName, "registered", "")
		fs.StringVar(&nonPrefixed, flagName, "non-prefixed", "")
	})

	require.Equal(t, prefix, rf.Prefix)
	require.Equal(t, 1, len(rf.Flags))
	require.NotNil(t, rf.Flags[flagName])
	require.Equal(t, prefix+flagName, rf.Flags[flagName].Name)

	// Check that we're referencing the correct flag.
	require.NoError(t, rf.Flags[flagName].Value.Set("changed"))
	require.Equal(t, "changed", registered)

}
