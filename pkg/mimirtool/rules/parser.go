// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/grafana/cortex-tools/blob/main/pkg/rules/parser.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package rules

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/rulefmt"
	log "github.com/sirupsen/logrus"
	yaml "gopkg.in/yaml.v3"
)

const (
	MimirBackend = "mimir"
)

var (
	errFileReadError  = errors.New("file read error")
	errInvalidBackend = errors.New("invalid backend type")
)

// ParseFiles returns a formatted set of prometheus rule groups
func ParseFiles(backend string, files []string, scheme model.ValidationScheme) (map[string]RuleNamespace, error) {
	ruleSet := map[string]RuleNamespace{}
	var parseFn func(f string, scheme model.ValidationScheme) ([]RuleNamespace, []error)
	switch backend {
	case MimirBackend:
		parseFn = Parse
	default:
		return nil, errInvalidBackend
	}

	for _, f := range files {
		nss, errs := parseFn(f, scheme)
		for _, err := range errs {
			log.WithError(err).WithField("file", f).Errorln("unable to parse rules file")
			return nil, errFileReadError
		}

		for _, ns := range nss {
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
			ruleSet[namespace] = ns
		}
	}
	return ruleSet, nil
}

// Parse parses and validates a set of rules.
func Parse(f string, scheme model.ValidationScheme) ([]RuleNamespace, []error) {
	content, err := loadFile(f)
	if err != nil {
		log.WithError(err).WithField("file", f).Errorln("unable to load rules file")
		return nil, []error{errFileReadError}
	}

	return ParseBytes(content, scheme)
}

func ParseBytes(content []byte, scheme model.ValidationScheme) ([]RuleNamespace, []error) {
	decoder := yaml.NewDecoder(bytes.NewReader(content))
	decoder.KnownFields(true)

	var nss []RuleNamespace
	for {
		var ns RuleNamespace
		err := decoder.Decode(&ns)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, []error{err}
		}

		nss = append(nss, ns)
	}

	// And now validate the rule groups, but first parse again, this time to YAML nodes so we have position information for error messages emitted by Validate.
	// See https://github.com/prometheus/prometheus/pull/16252 for more explanation for why we do this.
	decoder = yaml.NewDecoder(bytes.NewReader(content))
	decoder.KnownFields(false)

	for i := range nss {
		ns := &nss[i]

		var node ruleNamespaceNode
		err := decoder.Decode(&node)
		if errors.Is(err, io.EOF) {
			return nil, []error{fmt.Errorf("received unexpected EOF while parsing rules a second time for namespace %v", i)}
		}
		if err != nil {
			return nil, []error{err}
		}

		if errs := ns.Validate(node.GroupNodes, scheme); len(errs) > 0 {
			return nil, errs
		}
	}

	return nss, nil
}

type ruleNamespaceNode struct {
	GroupNodes []rulefmt.RuleGroupNode `yaml:"groups"`
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
