package server

import (
	fmt "fmt"
	"strings"
)

// ClusterCheckEnum is an enumeration of supported cluster verification label check modes.
type ClusterCheckEnum string

func (v *ClusterCheckEnum) Alternatives() string {
	return "none,all,grpc,http"
}

func (v *ClusterCheckEnum) HTTPEnabled() bool {
	switch v.String() {
	case "all", "http":
		return true
	default:
		return false
	}
}

func (v *ClusterCheckEnum) GRPCEnabled() bool {
	switch v.String() {
	case "all", "grpc":
		return true
	default:
		return false
	}
}

// String implements flag.Value
func (v *ClusterCheckEnum) String() string {
	ret := string(*v)
	if ret == "" {
		// Default.
		return "none"
	}
	return ret
}

// Set implements flag.Value
func (v *ClusterCheckEnum) Set(s string) error {
	if s == "" {
		s = "none"
	}
	switch strings.ToLower(s) {
	case "none", "all", "grpc", "http":
		*v = ClusterCheckEnum(s)
	default:
		return fmt.Errorf(`not one of none,all,grpc,http: %q`, s)
	}
	return nil
}

// UnmarshalYAML implements yaml.Unmarshaler.
func (v *ClusterCheckEnum) UnmarshalYAML(unmarshal func(any) error) error {
	var s string
	if err := unmarshal(&s); err != nil {
		return err
	}
	return v.Set(s)
}

// MarshalYAML implements yaml.Marshaler.
func (v *ClusterCheckEnum) MarshalYAML() (any, error) {
	return v.String(), nil
}
