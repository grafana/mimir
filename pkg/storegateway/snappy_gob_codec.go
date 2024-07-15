// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"bytes"
	"encoding/gob"

	"github.com/golang/snappy"
	"github.com/pkg/errors"
)

const gobCodecPrefix = "gob:"

var snappyEncodingCheckFn = snappy.MaxEncodedLen

func encodeSnappyGob(value interface{}) ([]byte, error) {
	buf := bytes.Buffer{}
	buf.WriteString(gobCodecPrefix)
	err := gob.NewEncoder(&buf).Encode(value)
	if err != nil {
		return nil, err
	}

	if maxEncodedLen := snappyEncodingCheckFn(len(buf.Bytes())); maxEncodedLen == -1 {
		return nil, errors.New("data too large")
	}
	encoded := snappy.Encode(nil, buf.Bytes())
	return encoded, nil
}

func decodeSnappyGob(input []byte, value interface{}) error {
	decoded, err := snappy.Decode(nil, input)
	if err != nil {
		return errors.Wrap(err, "snappy decode")
	}

	if len(decoded) < len(gobCodecPrefix) || string(decoded[:len(gobCodecPrefix)]) != gobCodecPrefix {
		return errors.New("no gob codec prefix")
	}

	decoder := gob.NewDecoder(bytes.NewBuffer(decoded[len(gobCodecPrefix):]))
	if err := decoder.Decode(value); err != nil {
		return errors.Wrap(err, "gob decode")
	}
	return nil
}
