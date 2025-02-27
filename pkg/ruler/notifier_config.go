package ruler

import (
	"encoding/json"
	"fmt"
)

type AlertmanagerClientConfig struct {
	AlertmanagerURL string         `yaml:"alertmanager_url"`
	NotifierConfig  NotifierConfig `yaml:",inline" json:",inline"`
}

func (acc *AlertmanagerClientConfig) String() string {
	out, err := json.Marshal(acc)
	if err != nil {
		return fmt.Sprintf("failed to marshal: %v", err)
	}
	return string(out)
}

func (acc *AlertmanagerClientConfig) Set(s string) error {
	new := AlertmanagerClientConfig{}
	if err := json.Unmarshal([]byte(s), &new); err != nil {
		return err
	}
	*acc = new
	return nil
}
