package schema

type Version string
type IntegrationType string

const (
	// versions that contain the "mimir" tag in their name are dedicated to integrations supported by Mimir.
	// By default, all mimir integrations should use the V0mimir1 version.
	// Exceptions are Mimir integrations that have multiple configurations for the same Grafana type.

	V0mimir1 Version = "v0mimir1"
	V0mimir2 Version = "v0mimir2"
	V1       Version = "v1"
)

// ElementType is the type of element that can be rendered in the frontend.
type ElementType string

// InputType is the type of input that can be rendered in the frontend.
type InputType string

const (
	// InputTypeText will render a text field in the frontend
	InputTypeText = "text"
	// InputTypePassword will render a password field in the frontend
	InputTypePassword = "password"
)

// IntegrationTypeSchema represents a notifier plugin with multiple versions and detailed configuration options.
// It includes metadata such as type, name, description, and version-specific details.
type IntegrationTypeSchema struct {
	Type           IntegrationType            `json:"type"`
	CurrentVersion Version                    `json:"currentVersion"`
	Name           string                     `json:"name"`
	Heading        string                     `json:"heading,omitempty"`
	Description    string                     `json:"description,omitempty"`
	Info           string                     `json:"info,omitempty"`
	Versions       []IntegrationSchemaVersion `json:"versions"`
}

// GetAllTypes returns a list of all types that are mentioned by the schema.
// Includes the main schema's type and all aliases of its versions
func (p IntegrationTypeSchema) GetAllTypes() []IntegrationType {
	types := []IntegrationType{p.Type}
	for _, version := range p.Versions {
		types = append(types, version.TypeAlias)
	}
	return types
}

// GetVersionByTypeAlias retrieves a specific version of the schema by its type alias. Returns the version and a boolean indicating success.
func (p IntegrationTypeSchema) GetVersionByTypeAlias(alias IntegrationType) (IntegrationSchemaVersion, bool) {
	for _, version := range p.Versions {
		if version.TypeAlias == alias {
			return version, true
		}
	}
	return IntegrationSchemaVersion{}, false
}

// GetVersion retrieves a specific version of the notifier plugin by its version string. Returns the version and a boolean indicating success.
func (p IntegrationTypeSchema) GetVersion(v Version) (IntegrationSchemaVersion, bool) {
	for _, version := range p.Versions {
		if version.Version == v {
			return version, true
		}
	}
	return IntegrationSchemaVersion{}, false
}

// GetCurrentVersion retrieves the current version of the notifier plugin based on the CurrentVersion property.
// Panics if the version specified in CurrentVersion is not found in the configured versions.
func (p IntegrationTypeSchema) GetCurrentVersion() IntegrationSchemaVersion {
	v, ok := p.GetVersion(p.CurrentVersion)
	if !ok {
		panic("version not found for current version: " + p.CurrentVersion)
	}
	return v
}

// IntegrationSchemaVersion represents a version of a notifier plugin, including configuration options and metadata.
type IntegrationSchemaVersion struct {
	// Alternative type name for this particular version
	TypeAlias IntegrationType `json:"typeAlias,omitempty"`
	// Version of the integration schema
	Version Version `json:"version"`
	// Whether new integration of this version can be created by users
	CanCreate bool `json:"canCreate"`
	// Integration fields
	Options []Field `json:"options"`
	// Additional information about the version
	Info string `json:"info,omitempty"`
}

// GetSecretFieldsPaths returns a list of paths for fields marked as secure within the IntegrationSchemaVersion's options.
func (v IntegrationSchemaVersion) GetSecretFieldsPaths() []string {
	return getSecretFields("", v.Options)
}

func getSecretFields(parentPath string, options []Field) []string {
	var secureFields []string
	for _, field := range options {
		name := field.PropertyName
		if parentPath != "" {
			name = parentPath + "." + name
		}
		if field.Secure {
			secureFields = append(secureFields, name)
			continue
		}
		if len(field.SubformOptions) > 0 {
			secureFields = append(secureFields, getSecretFields(name, field.SubformOptions)...)
		}
	}
	return secureFields
}

// Field describes a field of integration configuration.
type Field struct {
	Element        ElementType    `json:"element"`
	InputType      InputType      `json:"inputType"`
	Label          string         `json:"label"`
	Description    string         `json:"description"`
	Placeholder    string         `json:"placeholder"`
	PropertyName   string         `json:"propertyName"`
	SelectOptions  []SelectOption `json:"selectOptions"`
	ShowWhen       ShowWhen       `json:"showWhen"`
	Required       bool           `json:"required"`
	ValidationRule string         `json:"validationRule"`
	Secure         bool           `json:"secure"`
	DependsOn      string         `json:"dependsOn"`
	SubformOptions []Field        `json:"subformOptions"`
}

const (
	// ElementTypeInput will render an input
	ElementTypeInput = "input"
	// ElementTypeSelect will render a select
	ElementTypeSelect = "select"
	// ElementTypeCheckbox will render a checkbox
	ElementTypeCheckbox = "checkbox"
	// ElementTypeTextArea will render a textarea
	ElementTypeTextArea = "textarea"
	// ElementTypeKeyValueMap will render inputs to add arbitrary key-value pairs
	ElementTypeKeyValueMap = "key_value_map"
	// ElementSubformArray will render a sub-form with schema defined in SubformOptions
	ElementTypeSubform = "subform"
	// ElementSubformArray will render a multiple sub-forms with schema defined in SubformOptions
	ElementSubformArray = "subform_array"
	// ElementStringArray will render a set of fields to manage an array of strings.
	ElementStringArray = "string_array"
)

// SelectOption is a simple type for Options that have dropdown options. Should be used when Element is ElementTypeSelect.
type SelectOption struct {
	Value string `json:"value"`
	Label string `json:"label"`
}

// ShowWhen holds information about when options are dependant on other options.
// Should be used when Element is ElementTypeSelect.
// Does not work for ElementTypeCheckbox.
type ShowWhen struct {
	Field string `json:"field"`
	Is    string `json:"is"`
}
