package receivers

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/grafana/alerting/receivers/schema"
)

type DecryptFunc func(key string, fallback string) (string, bool)

// Get calls the DecryptFunc and returns only the decrypted value, discarding the availability flag.
func (fn DecryptFunc) Get(key string, fallback string) string {
	v, _ := fn(key, fallback)
	return v
}

func (fn DecryptFunc) GetPath(path schema.IntegrationFieldPath) (string, bool) {
	return fn(path.String(), "")
}

// DecryptSecret resolves a Secret field by decrypting the value for the given
// key, falling back to the current field value.
func (fn DecryptFunc) DecryptSecret(key string) (Secret, bool) {
	result, ok := fn(key, "")
	if !ok {
		return "", false
	}
	return Secret(result), true
}

// DecryptSecretURL resolves a SecretURL field by decrypting the value for the
// given key, parsing it as a URL, and returning the result. The current field
// value is used as the fallback. Returns nil when both the decrypted and
// fallback values are empty.
func (fn DecryptFunc) DecryptSecretURL(key string) (SecretURL, bool, error) {
	var fb string
	raw, ok := fn(key, fb)
	if !ok {
		return SecretURL{}, false, nil
	}
	parsedURL, err := url.Parse(raw)
	if err != nil {
		return SecretURL{}, true, fmt.Errorf("invalid URL for %s: %w", key, err)
	}
	return SecretURL{URL: parsedURL}, true, nil
}

type CommaSeparatedStrings []string

func (r *CommaSeparatedStrings) UnmarshalJSON(b []byte) error {
	var str string
	if err := json.Unmarshal(b, &str); err != nil {
		return err
	}
	if len(str) > 0 {
		res := CommaSeparatedStrings(splitCommaDelimitedString(str))
		*r = res
	}
	return nil
}

func (r *CommaSeparatedStrings) MarshalJSON() ([]byte, error) {
	if r == nil {
		return nil, nil
	}
	str := strings.Join(*r, ",")
	return json.Marshal(str)
}

func (r *CommaSeparatedStrings) UnmarshalYAML(b []byte) error {
	var str string
	if err := yaml.Unmarshal(b, &str); err != nil {
		return err
	}
	if len(str) > 0 {
		res := CommaSeparatedStrings(splitCommaDelimitedString(str))
		*r = res
	}
	return nil
}

func (r *CommaSeparatedStrings) MarshalYAML() ([]byte, error) {
	if r == nil {
		return nil, nil
	}
	str := strings.Join(*r, ",")
	return yaml.Marshal(str)
}

func splitCommaDelimitedString(str string) []string {
	split := strings.Split(str, ",")
	res := make([]string, 0, len(split))
	for _, s := range split {
		if tr := strings.TrimSpace(s); tr != "" {
			res = append(res, tr)
		}
	}
	return res
}
