package server

import (
	fmt "fmt"
	"strings"
)

type clusterCheckEnum string

func (v *clusterCheckEnum) Alternatives() string {
	return "none,all,grpc,http"
}

func (v *clusterCheckEnum) HTTPEnabled() bool {
	switch v.String() {
	case "all", "http":
		return true
	default:
		return false
	}
}

func (v *clusterCheckEnum) GRPCEnabled() bool {
	switch v.String() {
	case "all", "grpc":
		return true
	default:
		return false
	}
}

// String implements flag.Value
func (v *clusterCheckEnum) String() string {
	ret := string(*v)
	if ret == "" {
		// Default.
		return "none"
	}
	return ret
}

// Set implements flag.Value
func (v *clusterCheckEnum) Set(s string) error {
	switch strings.ToLower(s) {
	case "none", "all", "grpc", "http":
		*v = clusterCheckEnum(s)
	default:
		return fmt.Errorf(`not one of none,all,grpc,http: %q`, s)
	}
	return nil
}

// UnmarshalYAML implements yaml.Unmarshaler.
func (v *clusterCheckEnum) UnmarshalYAML(unmarshal func(any) error) error {
	var s string
	if err := unmarshal(&s); err != nil {
		return err
	}
	return v.Set(s)
}

// MarshalYAML implements yaml.Marshaler.
func (v *clusterCheckEnum) MarshalYAML() (any, error) {
	return v.String(), nil
}
