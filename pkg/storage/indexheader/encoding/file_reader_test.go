// SPDX-License-Identifier: AGPL-3.0-only

package encoding

import (
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/require"
)

type closer struct{}

func (c closer) put(file *os.File) error {
	return file.Close()
}

func TestReaders_Read(t *testing.T) {
	testReaders(t, func(t *testing.T, r *fileReader) {
		firstRead, err := r.read(5)
		require.NoError(t, err)
		require.Equal(t, []byte("abcde"), firstRead, "first read")

		secondRead, err := r.read(5)
		require.NoError(t, err)
		require.Equal(t, []byte("fghij"), secondRead, "second read")

		readBeyondEnd, err := r.read(12)
		require.ErrorIs(t, err, ErrInvalidSize)
		require.Empty(t, readBeyondEnd, "read beyond end")

		readAfterEnd, err := r.read(1)
		require.ErrorIs(t, err, ErrInvalidSize)
		require.Empty(t, readAfterEnd, "read after end")
	})
}

func TestReaders_ReadInto(t *testing.T) {
	testReaders(t, func(t *testing.T, r *fileReader) {
		firstBuf := make([]byte, 5)
		err := r.readInto(firstBuf)
		require.NoError(t, err)
		require.Equal(t, []byte("abcde"), firstBuf, "first read")

		secondBuf := make([]byte, 5)
		err = r.readInto(secondBuf)
		require.NoError(t, err)
		require.Equal(t, []byte("fghij"), secondBuf, "second read")

		beyondEndBuf := make([]byte, 12)
		err = r.readInto(beyondEndBuf)
		require.ErrorIs(t, err, ErrInvalidSize)

		afterEndBuf := make([]byte, 1)
		err = r.readInto(afterEndBuf)
		require.ErrorIs(t, err, ErrInvalidSize)
	})
}

func TestReaders_Peek(t *testing.T) {
	testReaders(t, func(t *testing.T, r *fileReader) {
		firstPeek, err := r.peek(5)
		require.NoError(t, err)
		require.Equal(t, []byte("abcde"), firstPeek, "peek (first call)")

		secondPeek, err := r.peek(5)
		require.NoError(t, err)
		require.Equal(t, []byte("abcde"), secondPeek, "peek (second call)")

		readAfterPeek, err := r.read(5)
		require.NoError(t, err)
		require.Equal(t, []byte("abcde"), readAfterPeek, "first read call")

		peekAfterRead, err := r.peek(5)
		require.NoError(t, err)
		require.Equal(t, []byte("fghij"), peekAfterRead, "peek after read")

		peekBeyondEnd, err := r.peek(20)
		require.NoError(t, err)
		require.Equal(t, []byte("fghij1234567890"), peekBeyondEnd, "peek beyond end")

		_, err = r.read(15)
		require.NoError(t, err)

		peekAfterEnd, err := r.peek(1)
		require.NoError(t, err)
		require.Empty(t, peekAfterEnd, "peek after end")
	})
}

func TestReaders_Reset(t *testing.T) {
	testReaders(t, func(t *testing.T, r *fileReader) {
		_, err := r.read(5)
		require.NoError(t, err)
		require.NoError(t, r.reset())

		readAfterReset, err := r.read(5)
		require.NoError(t, err)
		require.Equal(t, []byte("abcde"), readAfterReset)
	})
}

func TestReaders_ResetAt(t *testing.T) {
	testReaders(t, func(t *testing.T, r *fileReader) {
		require.NoError(t, r.resetAt(5))
		readAfterReset, err := r.read(5)
		require.NoError(t, err)
		require.Equal(t, []byte("fghij"), readAfterReset, "read after reset to non-zero offset")

		require.NoError(t, r.resetAt(0))
		readAfterResetToBeginning, err := r.read(5)
		require.NoError(t, err)
		require.Equal(t, []byte("abcde"), readAfterResetToBeginning, "read after reset to zero offset")

		require.NoError(t, r.resetAt(19))
		readAfterResetToLastByte, err := r.read(1)
		require.NoError(t, err)
		require.Equal(t, []byte("0"), readAfterResetToLastByte, "read after reset to last byte")

		require.NoError(t, r.resetAt(20))
		require.ErrorIs(t, r.resetAt(21), ErrInvalidSize)
	})
}

func TestReaders_Skip(t *testing.T) {
	testReaders(t, func(t *testing.T, r *fileReader) {
		peek, err := r.peek(5)
		require.NoError(t, err)
		require.Equal(t, []byte("abcde"), peek, "peek before skip")
		require.Equal(t, 20, r.len())

		require.NoError(t, r.skip(5))
		readAfterSkip, err := r.read(5)
		require.NoError(t, err)
		require.Equal(t, []byte("fghij"), readAfterSkip, "read after skip")
		require.Equal(t, 10, r.len())

		require.NoError(t, r.skip(5))
		peekAfterSkip, err := r.peek(5)
		require.NoError(t, err)
		require.Equal(t, []byte("67890"), peekAfterSkip, "peek after skip")
		require.Equal(t, 5, r.len())

		// skip to exactly the end, then skip beyond it
		require.NoError(t, r.skip(5))
		require.Equal(t, 0, r.len())
		require.ErrorIs(t, r.skip(1), ErrInvalidSize)

	})
}

func TestReaders_Len(t *testing.T) {
	testReaders(t, func(t *testing.T, r *fileReader) {
		require.Equal(t, 20, r.len(), "initial length")

		_, err := r.read(5)
		require.NoError(t, err)
		require.Equal(t, 15, r.len(), "after first read")

		_, err = r.read(2)
		require.NoError(t, err)
		require.Equal(t, 13, r.len(), "after second read")

		_, err = r.peek(3)
		require.NoError(t, err)
		require.Equal(t, 13, r.len(), "after peek")

		_, err = r.read(14)
		require.ErrorIs(t, err, ErrInvalidSize)
		require.Equal(t, 0, r.len(), "after read beyond end")

		require.NoError(t, r.reset())
		require.Equal(t, 20, r.len(), "after reset to beginning")

		require.NoError(t, r.resetAt(3))
		require.Equal(t, 17, r.len(), "after reset to offset")
	})
}

func TestReaders_Position(t *testing.T) {
	testReaders(t, func(t *testing.T, r *fileReader) {
		require.Equal(t, 0, r.position(), "initial position")

		_, err := r.read(5)
		require.NoError(t, err)
		require.Equal(t, 5, r.position(), "after first read")

		_, err = r.read(2)
		require.NoError(t, err)
		require.Equal(t, 7, r.position(), "after second read")

		_, err = r.peek(3)
		require.NoError(t, err)
		require.Equal(t, 7, r.position(), "after peek")

		_, err = r.read(14)
		require.ErrorIs(t, err, ErrInvalidSize)
		require.Equal(t, 20, r.position(), "after read beyond end")

		require.NoError(t, r.reset())
		require.Equal(t, 0, r.position(), "after reset to beginning")

		require.NoError(t, r.resetAt(3))
		require.Equal(t, 3, r.position(), "after reset to offset")
	})
}

func TestReaders_CreationWithEmptyContents(t *testing.T) {
	t.Run("fileReader", func(t *testing.T) {
		dir := t.TempDir()
		filePath := path.Join(dir, "test-file")
		require.NoError(t, os.WriteFile(filePath, nil, 0700))

		f, err := os.Open(filePath)
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, f.Close())
		})

		r, err := newFileReader(f, 0, 0, &closer{})
		require.NoError(t, err)
		require.ErrorIs(t, r.skip(1), ErrInvalidSize)
		require.ErrorIs(t, r.resetAt(1), ErrInvalidSize)
	})
}

func testReaders(t *testing.T, test func(t *testing.T, r *fileReader)) {
	testReaderContents := []byte("abcdefghij1234567890")

	t.Run("FileReaderWithZeroOffset", func(t *testing.T) {
		dir := t.TempDir()
		filePath := path.Join(dir, "test-file")
		require.NoError(t, os.WriteFile(filePath, testReaderContents, 0700))

		f, err := os.Open(filePath)
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, f.Close())
		})

		r, err := newFileReader(f, 0, len(testReaderContents), &closer{})
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

		r, err := newFileReader(f, len(offsetBytes), len(testReaderContents), &closer{})
		require.NoError(t, err)

		test(t, r)
	})
}
