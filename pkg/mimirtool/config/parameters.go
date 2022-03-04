package config

type Parameters interface {
	Delete(path string) error
	GetDefaultValue(path string) (interface{}, error)
	GetFlag(path string) (string, error)
	GetValue(path string) (interface{}, error)
	MustGetDefaultValue(path string) interface{}
	MustGetValue(path string) interface{}
	SetDefaultValue(path string, val interface{}) error
	SetValue(path string, val interface{}) error
	Walk(f func(path string, value interface{}) error) error
}

type defaultValueInspectedEntry struct {
	*InspectedEntry
}

func (i defaultValueInspectedEntry) GetValue(path string) (interface{}, error) {
	return i.InspectedEntry.GetDefaultValue(path)
}

func (i defaultValueInspectedEntry) MustGetValue(path string) interface{} {
	return i.InspectedEntry.MustGetDefaultValue(path)
}

func (i defaultValueInspectedEntry) SetValue(path string, val interface{}) error {
	return i.InspectedEntry.SetDefaultValue(path, val)
}
