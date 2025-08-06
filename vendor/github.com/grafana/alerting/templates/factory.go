package templates

import (
	"fmt"
	"net/url"
	"sync"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
)

// Factory is a factory that can be used to create templates of specific kind.
type Factory struct {
	templates   map[Kind][]TemplateDefinition
	externalURL *url.URL
	orgID       string
}

// GetTemplate creates a new template of the given kind. If Kind is not known, GrafanaKind automatically assumed
func (tp *Factory) GetTemplate(kind Kind) (*Template, error) {
	if err := ValidateKind(kind); err != nil {
		return nil, err
	}
	definitions := tp.templates[kind]
	content := defaultTemplatesPerKind(kind)
	for _, def := range definitions { // TODO sort the list by name?
		content = append(content, def.Template)
	}
	t, err := fromContent(content, defaultOptionsPerKind(kind, tp.orgID)...)
	if err != nil {
		return nil, err
	}
	if tp.externalURL != nil {
		t.ExternalURL = new(url.URL)
		*t.ExternalURL = *tp.externalURL
	}
	return t, nil
}

// WithTemplate creates a new factory that has the provided TemplateDefinition. If definition with the same name already exists for this kind, it is replaced.
func (tp *Factory) WithTemplate(def TemplateDefinition) (*Factory, error) {
	if err := ValidateKind(def.Kind); err != nil {
		return nil, err
	}

	added := false
	templates := make(map[Kind][]TemplateDefinition, len(tp.templates))
	for kind, definitions := range tp.templates {
		if kind != def.Kind {
			templates[kind] = definitions
			continue
		}
		newDefinitions := make([]TemplateDefinition, 0, len(definitions)+1)
		for _, d := range definitions {
			if d.Name == def.Name {
				newDefinitions = append(newDefinitions, def)
				added = true
				continue
			}
			newDefinitions = append(newDefinitions, d)
		}
		templates[kind] = newDefinitions
	}
	if !added {
		templates[def.Kind] = append(templates[def.Kind], def)
	}

	return &Factory{
		templates:   templates,
		externalURL: tp.externalURL,
	}, nil
}

// NewFactory creates a new template provider. Accepts list of user-defined templates that are added to the kind's default templates.
// Returns error if externalURL is not a valid URL or if TemplateDefinition.Kind is not known.
func NewFactory(t []TemplateDefinition, logger log.Logger, externalURL string, orgID string) (*Factory, error) {
	extURL, err := url.Parse(externalURL)
	if err != nil {
		return nil, err
	}
	type seenKey struct {
		Name string
		Type Kind
	}
	seen := map[seenKey]struct{}{}
	byType := map[Kind][]TemplateDefinition{}

	for _, def := range t {
		if err := ValidateKind(def.Kind); err != nil {
			return nil, fmt.Errorf("invalid template definition %s: %w", def.Name, err)
		}
		if _, ok := seen[seenKey{Name: def.Name, Type: def.Kind}]; ok {
			level.Warn(logger).Log("msg", "template with same name is defined multiple times, skipping...", "template_name", def.Name, "template_type", def.Kind)
			continue
		}
		byType[def.Kind] = append(byType[def.Kind], def)
		seen[seenKey{Name: def.Name, Type: def.Kind}] = struct{}{}
	}
	provider := &Factory{
		templates:   byType,
		externalURL: extURL,
		orgID:       orgID,
	}
	return provider, nil
}

func NewCachedFactory(factory *Factory) *CachedFactory {
	return &CachedFactory{
		factory: factory,
		m:       make(map[Kind]*Template, len(validKinds)),
	}
}

// CachedFactory is responsible for managing template instances grouped by their kind.
// It utilizes a Factory to create templates when requested.
// Templates are cached in-memory to avoid redundant creation.
// Access is synchronized using a mutex to ensure thread-safety.
type CachedFactory struct {
	factory *Factory
	m       map[Kind]*Template
	mtx     sync.Mutex
}

func (cf *CachedFactory) Factory() *Factory {
	return cf.factory
}

func (cf *CachedFactory) GetTemplate(kind Kind) (*Template, error) {
	cf.mtx.Lock()
	defer cf.mtx.Unlock()
	if t, ok := cf.m[kind]; ok {
		return t.Clone()
	}
	t, err := cf.factory.GetTemplate(kind)
	if err != nil {
		return nil, err
	}
	cf.m[kind] = t
	return t.Clone()
}
