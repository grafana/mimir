package clusterutil

import (
	"flag"
	"fmt"
)

type ClusterValidationConfig struct {
	Label string
	GRPC  ClusterValidationProtocolConfig
}

type ClusterValidationProtocolConfig struct {
	Enabled        bool
	SoftValidation bool
}

func (cfg *ClusterValidationConfig) Validate() error {
	return cfg.GRPC.Validate("grpc", cfg.Label)
}

func (cfg *ClusterValidationConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	clusterValidationPrefix := prefix + ".cluster-validation"
	f.StringVar(&cfg.Label, clusterValidationPrefix+".label", "", "Optionally define server's cluster validation label.")
	cfg.GRPC.RegisterFlagsWithPrefix(clusterValidationPrefix+".grpc", f)
}

func (cfg *ClusterValidationProtocolConfig) Validate(prefix string, label string) error {
	if label == "" {
		if cfg.Enabled || cfg.SoftValidation {
			return fmt.Errorf("%s: validation cannot be enabled if cluster validation label is not configured", prefix)
		}
		return nil
	}

	if !cfg.Enabled && cfg.SoftValidation {
		return fmt.Errorf("%s: soft validation can be enabled only if cluster validation is enabled", prefix)
	}
	return nil
}

func (cfg *ClusterValidationProtocolConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	softValidationFlag := prefix + ".soft-validation"
	enabledFlag := prefix + ".enabled"
	f.BoolVar(&cfg.SoftValidation, softValidationFlag, false, fmt.Sprintf("When enabled, soft cluster label validation will be executed. Can be enabled only together with %s", enabledFlag))
	f.BoolVar(&cfg.Enabled, enabledFlag, false, "When enabled, cluster label validation will be executed.")
}
