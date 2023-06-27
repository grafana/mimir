// SPDX-License-Identifier: AGPL-3.0-only

package atomicfs

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRun(t *testing.T) {
	type args struct {
		path string
		data string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "test fsync.CreateFile with WriteString",
			args: args{
				path: filepath.Join(t.TempDir(), "test"),
				data: "test1",
			},
			wantErr: false,
		},
		{
			name: "test fsync.CreateFile with Write",
			args: args{
				path: filepath.Join(t.TempDir(), "test"),
				data: "test2",
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := CreateFile(tt.args.path, tt.args.data); (err != nil) != tt.wantErr {
				t.Errorf("CreateFile() error = %v, wantErr %v", err, tt.wantErr)
			}
			data, err := os.ReadFile(tt.args.path)
			require.NoError(t, err)
			require.Equal(t, tt.args.data, string(data))
		})
	}
}
