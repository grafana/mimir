// SPDX-License-Identifier: AGPL-3.0-only

package encoding

import (
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReaders_Read(t *testing.T) {
	testReaders(t, func(t *testing.T, r Reader) {
		firstRead, err := r.Read(5)
		require.NoError(t, err)
		require.Equal(t, []byte("abcde"), firstRead, "first read")

		secondRead, err := r.Read(5)
		require.NoError(t, err)
		require.Equal(t, []byte("fghij"), secondRead, "second read")

		readBeyondEnd, err := r.Read(12)
		require.NoError(t, err)
		require.Equal(t, []byte("1234567890"), readBeyondEnd, "read beyond end")

		readAfterEnd, err := r.Read(1)
		require.NoError(t, err)
		require.Empty(t, readAfterEnd, "read after end")
	})
}

func TestReaders_Peek(t *testing.T) {
	testReaders(t, func(t *testing.T, r Reader) {
		firstPeek, err := r.Peek(5)
		require.NoError(t, err)
		require.Equal(t, []byte("abcde"), firstPeek, "peek (first call)")

		secondPeek, err := r.Peek(5)
		require.NoError(t, err)
		require.Equal(t, []byte("abcde"), secondPeek, "peek (second call)")

		readAfterPeek, err := r.Read(5)
		require.Equal(t, []byte("abcde"), readAfterPeek, "first read call")

		peekAfterRead, err := r.Peek(5)
		require.NoError(t, err)
		require.Equal(t, []byte("fghij"), peekAfterRead, "peek after read")

		peekBeyondEnd, err := r.Peek(20)
		require.NoError(t, err)
		require.Equal(t, []byte("fghij1234567890"), peekBeyondEnd, "peek beyond end")

		r.Read(15)
		peekAfterEnd, err := r.Peek(1)
		require.NoError(t, err)
		require.Empty(t, peekAfterEnd, "peek after end")
	})
}

func TestReaders_Reset(t *testing.T) {
	testReaders(t, func(t *testing.T, r Reader) {
		r.Read(5)
		require.NoError(t, r.Reset())

		readAfterReset, err := r.Read(5)
		require.NoError(t, err)
		require.Equal(t, []byte("abcde"), readAfterReset)
	})
}

func TestReaders_ResetAt(t *testing.T) {
	testReaders(t, func(t *testing.T, r Reader) {
		require.NoError(t, r.ResetAt(5))
		readAfterReset, err := r.Read(5)

		require.Equal(t, []byte("fghij"), readAfterReset, "read after reset to non-zero offset")

		require.NoError(t, r.ResetAt(0))
		readAfterResetToBeginning, err := r.Read(5)
		require.NoError(t, err)
		require.Equal(t, []byte("abcde"), readAfterResetToBeginning, "read after reset to zero offset")

		require.NoError(t, r.ResetAt(19))
		readAfterResetToLastByte, err := r.Read(1)
		require.NoError(t, err)
		require.Equal(t, []byte("0"), readAfterResetToLastByte, "read after reset to last byte")

		require.ErrorIs(t, r.ResetAt(20), ErrInvalidSize)
	})
}

func TestReaders_Len(t *testing.T) {
	testReaders(t, func(t *testing.T, r Reader) {
		require.Equal(t, 20, r.Len(), "initial length")

		r.Read(5)
		require.Equal(t, 15, r.Len(), "after first read")

		r.Read(2)
		require.Equal(t, 13, r.Len(), "after second read")

		_, err := r.Peek(3)
		require.NoError(t, err)
		require.Equal(t, 13, r.Len(), "after peek")

		r.Read(14)
		require.Equal(t, 0, r.Len(), "after read beyond end")

		require.NoError(t, r.Reset())
		require.Equal(t, 20, r.Len(), "after reset to beginning")

		require.NoError(t, r.ResetAt(3))
		require.Equal(t, 17, r.Len(), "after reset to offset")
	})
}

func testReaders(t *testing.T, test func(t *testing.T, r Reader)) {
	testReaderContents := []byte("abcdefghij1234567890")

	t.Run("BufReader", func(t *testing.T) {
		r := NewBufReader(realByteSlice(testReaderContents))
		test(t, r)
	})

	t.Run("FileReaderWithZeroOffset", func(t *testing.T) {
		dir := t.TempDir()
		filePath := path.Join(dir, "test-file")
		require.NoError(t, os.WriteFile(filePath, testReaderContents, 0700))

		f, err := os.Open(filePath)
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, f.Close())
		})

		r, err := NewFileReader(f, 0, len(testReaderContents))
		require.NoError(t, err)

		test(t, r)
	})

	t.Run("FileReaderWithNonZeroOffset", func(t *testing.T) {
		offsetBytes := []byte("ABCDE")
		fileBytes := append(offsetBytes, testReaderContents...)

		dir := t.TempDir()
		filePath := path.Join(dir, "test-file")
		require.NoError(t, os.WriteFile(filePath, fileBytes, 0700))

		f, err := os.Open(filePath)
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, f.Close())
		})

		r, err := NewFileReader(f, len(offsetBytes), len(testReaderContents))
		require.NoError(t, err)

		test(t, r)
	})
}
