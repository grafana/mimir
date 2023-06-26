package fsync

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCreateFile(t *testing.T) {
	fileCreator := func() *os.File {
		file, err := os.CreateTemp("", "")
		require.NoError(t, err)
		return file
	}
	type args struct {
		file      *os.File
		writeFunc func(f *os.File) (int, error)
	}
	tests := []struct {
		name              string
		args              args
		wantErr           bool
		assertFileContent func(fileName string)
	}{
		{
			name: "test create with WriteString",
			args: args{
				file: fileCreator(),
				writeFunc: func(f *os.File) (int, error) {
					return f.WriteString("test")
				},
			},
			wantErr: false,
			assertFileContent: func(fileName string) {
				data, err := os.ReadFile(fileName)
				require.NoError(t, err)
				require.Equal(t, "test", string(data))
			},
		},
		{
			name: "test create with Write",
			args: args{
				file: fileCreator(),
				writeFunc: func(f *os.File) (int, error) {
					return f.Write([]byte("test2"))
				},
			},
			wantErr: false,
			assertFileContent: func(fileName string) {
				data, err := os.ReadFile(fileName)
				require.NoError(t, err)
				require.Equal(t, "test2", string(data))
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := CreateFile(tt.args.file, tt.args.writeFunc); (err != nil) != tt.wantErr {
				t.Errorf("CreateFile() error = %v, wantErr %v", err, tt.wantErr)
			}
			tt.assertFileContent(tt.args.file.Name())
		})
	}
}
