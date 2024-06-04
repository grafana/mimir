package internal

import (
	"encoding/json"
	"fmt"
	"os"
	"path"
	"strings"
)

func GetStringValueFromJSON(fpath string, key string) (string, error) {
	byteValue, err := os.ReadFile(fpath)
	if err != nil {
		return "", err
	}

	var result map[string]interface{}
	err = json.Unmarshal(byteValue, &result)
	if err != nil {
		return "", err
	}
	executable := result[key]
	name, ok := executable.(string)
	if !ok || name == "" {
		return "", fmt.Errorf("plugin.json is missing: %s", key)
	}
	return name, nil
}

func GetExecutableFromPluginJSON(dir string) (string, error) {
	exe, err := GetStringValueFromJSON(path.Join(dir, "plugin.json"), "executable")
	if err != nil {
		// In app plugins, the exe may be nested
		exe, err2 := GetStringValueFromJSON(path.Join(dir, "datasource", "plugin.json"), "executable")
		if err2 == nil {
			if !strings.HasPrefix(exe, "../") {
				return "", fmt.Errorf("datasource should reference executable in root folder")
			}
			return exe[3:], nil
		}
	}
	return exe, err
}
