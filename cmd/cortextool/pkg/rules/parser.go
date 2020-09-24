package rules

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"

	"github.com/grafana/loki/pkg/ruler/manager"
	"github.com/prometheus/prometheus/pkg/rulefmt"
	log "github.com/sirupsen/logrus"
	yaml "gopkg.in/yaml.v3"
)

const (
	CortexBackend = "cortex"
	LokiBackend   = "loki"
)

var (
	errFileReadError  = errors.New("file read error")
	errInvalidBackend = errors.New("invalid backend type")
)

// ParseFiles returns a formatted set of prometheus rule groups
func ParseFiles(backend string, files []string) (map[string]RuleNamespace, error) {
	ruleSet := map[string]RuleNamespace{}
	var parseFn func(f string) (*RuleNamespace, []error)
	switch backend {
	case CortexBackend:
		parseFn = Parse
	case LokiBackend:
		parseFn = ParseLoki
	default:
		return nil, errInvalidBackend
	}

	for _, f := range files {
		ns, errs := parseFn(f)
		for _, err := range errs {
			log.WithError(err).WithField("file", f).Errorln("unable parse rules file")
			return nil, errFileReadError
		}
		ns.Filepath = f

		// Determine if the namespace is explicitly set. If not
		// the file name without the extension is used.
		namespace := ns.Namespace
		if namespace == "" {
			namespace = strings.TrimSuffix(filepath.Base(f), filepath.Ext(f))
			ns.Namespace = namespace
		}

		_, exists := ruleSet[namespace]
		if exists {
			log.WithFields(log.Fields{
				"namespace": namespace,
				"file":      f,
			}).Errorln("repeated namespace attempted to be loaded")
			return nil, errFileReadError
		}
		ruleSet[namespace] = *ns
	}
	return ruleSet, nil
}

// Parse parses and validates a set of rules.
func Parse(f string) (*RuleNamespace, []error) {
	content, err := loadFile(f)
	if err != nil {
		log.WithError(err).WithField("file", f).Errorln("unable load rules file")
		return nil, []error{errFileReadError}
	}

	var ns RuleNamespace
	decoder := yaml.NewDecoder(bytes.NewReader(content))
	decoder.KnownFields(true)
	if err := decoder.Decode(&ns); err != nil {
		return nil, []error{err}
	}
	return &ns, ns.Validate()
}

func ParseLoki(f string) (*RuleNamespace, []error) {
	content, err := loadFile(f)
	if err != nil {
		log.WithError(err).WithField("file", f).Errorln("unable load rules file")
		return nil, []error{errFileReadError}
	}

	var ns RuleNamespace
	decoder := yaml.NewDecoder(bytes.NewReader(content))
	decoder.KnownFields(true)
	if err := decoder.Decode(&ns); err != nil {
		return nil, []error{err}
	}

	// the upstream loki validator only validates the rulefmt rule groups,
	// not the remote write configs this type attaches.
	var grps []rulefmt.RuleGroup
	for _, g := range ns.Groups {
		grps = append(grps, g.RuleGroup)
	}

	return &ns, manager.ValidateGroups(grps...)
}

func loadFile(filename string) ([]byte, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	fileinfo, err := file.Stat()
	if err != nil {
		return nil, err
	}

	filesize := fileinfo.Size()
	buffer := make([]byte, filesize)

	_, err = file.Read(buffer)
	if err != nil {
		return nil, err
	}

	return buffer, nil
}
