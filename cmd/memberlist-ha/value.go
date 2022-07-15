package main

import (
	"fmt"
	"strconv"
	"time"

	"github.com/grafana/dskit/kv/memberlist"
)

type valueCodec struct{}

func (c valueCodec) Decode(bytes []byte) (interface{}, error) {
	number, err := strconv.Atoi(string(bytes))
	return &value{number: int64(number)}, err
}

func (c valueCodec) Encode(i interface{}) ([]byte, error) {
	return []byte(i.(*value).String()), nil
}

func (c valueCodec) CodecID() string {
	return "valueCodec"
}

type value struct {
	number int64
}

func (v *value) Merge(other memberlist.Mergeable, localCAS bool) (change memberlist.Mergeable, error error) {
	if v == nil {
		return nil, fmt.Errorf("value is nil, cannot merge with anything")
	}
	otherVal := other.(*value)
	if otherVal.number > v.number {
		v.number = otherVal.number
		return otherVal, nil
	}
	return nil, nil
}

func (v *value) MergeContent() []string {
	return []string{v.String()}
}

func (v *value) RemoveTombstones(time.Time) (int, int) {
	return 0, 0
}

func (v *value) Clone() memberlist.Mergeable {
	return &value{number: v.number}
}

func (v *value) String() string {
	if v == nil {
		return ""
	}
	return strconv.Itoa(int(v.number))
}
