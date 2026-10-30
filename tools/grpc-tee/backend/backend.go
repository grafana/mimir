// SPDX-License-Identifier: AGPL-3.0-only

package backend

import "github.com/gogo/protobuf/proto"

type BackendType struct {
	name       string
	codecTable *MethodCodecTable
}

func NewBackendType(name string, codecTable *MethodCodecTable) *BackendType {
	return &BackendType{
		name:       name,
		codecTable: codecTable,
	}
}

func (bt *BackendType) DecodeRequest(fullMethod string, data []byte) (proto.Message, error) {
	return bt.codecTable.DecodeRequest(fullMethod, data)
}

func (bt *BackendType) DecodeResponse(fullMethod string, data []byte) (proto.Message, error) {
	return bt.codecTable.DecodeResponse(fullMethod, data)
}
