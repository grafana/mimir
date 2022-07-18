// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/grafana-tools/sdk/blob/master/custom-types.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: 2016 Alexander I.Grafov <grafov@gmail.com>.
// Provenance-includes-copyright: 2016-2019 The Grafana SDK authors

package minisdk

import (
	"bytes"
	"errors"
	"strconv"
)

type BoolInt struct {
	Flag  bool
	Value *int64
}

func (s *BoolInt) UnmarshalJSON(raw []byte) error {
	if raw == nil || bytes.Equal(raw, []byte(`"null"`)) {
		return nil
	}
	var (
		tmp int64
		err error
	)
	if tmp, err = strconv.ParseInt(string(raw), 10, 64); err != nil {
		if bytes.Equal(raw, []byte("true")) {
			s.Flag = true
			return nil
		}
		if bytes.Equal(raw, []byte("false")) {
			return nil
		}
		return errors.New("bad value provided")
	}
	s.Value = &tmp
	return nil
}

func (s BoolInt) MarshalJSON() ([]byte, error) {
	if s.Value != nil {
		return strconv.AppendInt([]byte{}, *s.Value, 10), nil
	}
	return strconv.AppendBool([]byte{}, s.Flag), nil
}
