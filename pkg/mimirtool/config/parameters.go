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
