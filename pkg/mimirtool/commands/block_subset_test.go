package commands

import (
	"context"
	"errors"
	"path"
	"strconv"
	"testing"
	"time"

	"github.com/prometheus/common/promslog"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/require"
)

func TestBlockSubsetCommand(t *testing.T) {
	cases := []struct {
		name           string
		err            error
		sampleAppender func(a1, a2 storage.Appender)
	}{
		{
			name: "same block",
			sampleAppender: func(a1, a2 storage.Appender) {
				for i := 0; i < 10; i++ {
					for j := 0; j < 10; j++ {
						_, err := a2.Append(0, labels.FromStrings("foo", "bar"+strconv.Itoa(i)), int64(j), float64(j))
						require.NoError(t, err)
						_, err = a1.Append(0, labels.FromStrings("foo", "bar"+strconv.Itoa(i)), int64(j), float64(j))
						require.NoError(t, err)
					}
				}
			},
		},
		{
			name: "samples missing in superset",
			err:  errors.New("subset block has more samples than the superset block"),
			sampleAppender: func(a1, a2 storage.Appender) {
				for i := 0; i < 10; i++ {
					for j := 0; j < 10; j++ {
						if j == 0 || j == 9 || j%2 == 0 {
							_, err := a2.Append(0, labels.FromStrings("foo", "bar"+strconv.Itoa(i)), int64(j), float64(j))
							require.NoError(t, err)
						}
						_, err := a1.Append(0, labels.FromStrings("foo", "bar"+strconv.Itoa(i)), int64(j), float64(j))
						require.NoError(t, err)
					}
				}
			},
		},
		{
			name: "samples missing in subset",
			sampleAppender: func(a1, a2 storage.Appender) {
				for i := 0; i < 10; i++ {
					for j := 0; j < 10; j++ {
						if j%2 == 0 {
							_, err := a1.Append(0, labels.FromStrings("foo", "bar"+strconv.Itoa(i)), int64(j), float64(j))
							require.NoError(t, err)
						}
						_, err := a2.Append(0, labels.FromStrings("foo", "bar"+strconv.Itoa(i)), int64(j), float64(j))
						require.NoError(t, err)
					}
				}
			},
		},
		{
			name: "series missing in superset but having same number of series and samples",
			err:  errors.New("subset block has series that are not in the superset block"),
			sampleAppender: func(a1, a2 storage.Appender) {
				for i := 0; i < 10; i++ {
					for j := 0; j < 10; j++ {
						_, err := a1.Append(0, labels.FromStrings("foo", "bar"+strconv.Itoa(i)), int64(j), float64(j))
						require.NoError(t, err)
						if i != 5 {
							_, err = a2.Append(0, labels.FromStrings("foo", "bar"+strconv.Itoa(i)), int64(j), float64(j))
							require.NoError(t, err)
						}
					}
				}
				for j := 0; j < 10; j++ {
					_, err := a2.Append(0, labels.FromStrings("foo", "barbar"), int64(j), float64(j))
					require.NoError(t, err)
				}
			},
		},
		{
			name: "series missing in subset",
			sampleAppender: func(a1, a2 storage.Appender) {
				for i := 0; i < 10; i++ {
					for j := 0; j < 10; j++ {
						if i%2 == 0 {
							_, err := a1.Append(0, labels.FromStrings("foo", "bar"+strconv.Itoa(i)), int64(j), float64(j))
							require.NoError(t, err)
						}
						_, err := a2.Append(0, labels.FromStrings("foo", "bar"+strconv.Itoa(i)), int64(j), float64(j))
						require.NoError(t, err)
					}
				}
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			b1Dir := t.TempDir()
			b2Dir := t.TempDir()
			w1, err := tsdb.NewBlockWriter(promslog.NewNopLogger(), b1Dir, 2*time.Hour.Milliseconds())
			require.NoError(t, err)
			t.Cleanup(func() {
				require.NoError(t, w1.Close())
			})
			w2, err := tsdb.NewBlockWriter(promslog.NewNopLogger(), b2Dir, 2*time.Hour.Milliseconds())
			require.NoError(t, err)
			t.Cleanup(func() {
				require.NoError(t, w2.Close())
			})

			a1 := w1.Appender(context.Background())
			a2 := w2.Appender(context.Background())

			c.sampleAppender(a1, a2)

			require.NoError(t, a1.Commit())
			require.NoError(t, a2.Commit())

			b1ID, err := w1.Flush(context.Background())
			require.NoError(t, err)
			b2ID, err := w2.Flush(context.Background())
			require.NoError(t, err)

			bc := BlockSubsetCommand{
				subsetBlock:   path.Join(b1Dir, b1ID.String()),
				supersetBlock: path.Join(b2Dir, b2ID.String()),
			}

			err = bc.checkSubset(nil)
			if c.err == nil {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.Equal(t, err.Error(), c.err.Error())
			}
		})
	}
}
