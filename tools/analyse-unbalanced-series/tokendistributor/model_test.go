package tokendistributor

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestToken_Distance(t *testing.T) {
	usecases := []struct {
		token    Token
		next     Token
		expected uint32
	}{
		{
			token:    10,
			next:     20,
			expected: 10,
		},
		{
			token:    maxTokenValue - 1000,
			next:     10,
			expected: 1010,
		},
		{
			token:    1,
			next:     maxTokenValue,
			expected: maxTokenValue - 1,
		},
	}

	for _, usecase := range usecases {
		distance := usecase.token.distance(usecase.next, maxTokenValue)
		require.Equal(t, distance, usecase.expected)
	}
}

func TestToken_Split(t *testing.T) {
	usecases := []struct {
		token    Token
		next     Token
		expected Token
	}{
		{
			token:    10,
			next:     20,
			expected: Token(15),
		},
		{
			token:    maxTokenValue - 1000,
			next:     10,
			expected: Token(maxTokenValue - 495),
		},
		{
			token:    maxTokenValue - 10,
			next:     100,
			expected: Token(45),
		},
		{
			token:    1,
			next:     10,
			expected: Token(6),
		},
	}

	for _, usecase := range usecases {
		split := usecase.token.split(usecase.next, maxTokenValue)
		require.Equal(t, split, usecase.expected)
	}
}
