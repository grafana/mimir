// SPDX-License-Identifier: AGPL-3.0-only

package configdoc

var hiddenOverrides = map[string]bool{
	// Defined in grafana/dskit/server.Config
	"server.grpc.stats-tracking-enabled":    true,
	"server.grpc.recv-buffer-pools-enabled": true,
}

func GetHiddenOverride(fieldName string) (isHidden, ok bool) {
	isHidden, ok = hiddenOverrides[fieldName]
	return
}
