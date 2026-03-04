package definition

import (
	"unsafe"

	jsoniter "github.com/json-iterator/go"
	"github.com/modern-go/reflect2"
	amcfg "github.com/prometheus/alertmanager/config"
	commoncfg "github.com/prometheus/common/config"

	"github.com/grafana/alerting/receivers"
)

// secretEncoder encodes Secret to plain text JSON,
// avoiding the default masking behavior of the structure.
type secretEncoder struct{}

func (encoder *secretEncoder) Encode(ptr unsafe.Pointer, stream *jsoniter.Stream) {
	stream.WriteString(getStr(ptr))
}

func (encoder *secretEncoder) IsEmpty(ptr unsafe.Pointer) bool {
	return len(getStr(ptr)) == 0
}

func getStr(ptr unsafe.Pointer) string {
	return *(*string)(ptr)
}

// secretURLEncoder encodes SecretURL to plain text JSON,
// avoiding the default masking behavior of the structure.
type secretURLEncoder struct {
	getURLString func(ptr unsafe.Pointer) *string
}

func (encoder *secretURLEncoder) Encode(ptr unsafe.Pointer, stream *jsoniter.Stream) {
	s := encoder.getURLString(ptr)
	if s != nil {
		stream.WriteString(*s)
	} else {
		stream.WriteNil()
	}
}

func (encoder *secretURLEncoder) IsEmpty(ptr unsafe.Pointer) bool {
	return encoder.getURLString(ptr) == nil
}

func getAmcfgURLString(ptr unsafe.Pointer) *string {
	v := (*amcfg.SecretURL)(ptr)
	u := amcfg.URL(*v)
	if u.URL == nil {
		return nil
	}
	s := u.String()
	return &s
}

func getReceiversURLString(ptr unsafe.Pointer) *string {
	v := (*receivers.SecretURL)(ptr)
	u := receivers.URL(*v)
	if u.URL == nil {
		return nil
	}
	s := u.String()
	return &s
}

func newPlainAPI() jsoniter.API {
	api := jsoniter.ConfigCompatibleWithStandardLibrary

	secretEnc := &secretEncoder{}
	amcfgSecretURLEnc := &secretURLEncoder{getURLString: getAmcfgURLString}
	receiversSecretURLEnc := &secretURLEncoder{getURLString: getReceiversURLString}

	extension := jsoniter.EncoderExtension{
		// Value types
		reflect2.TypeOfPtr((*amcfg.Secret)(nil)).Elem():        secretEnc,
		reflect2.TypeOfPtr((*commoncfg.Secret)(nil)).Elem():    secretEnc,
		reflect2.TypeOfPtr((*receivers.Secret)(nil)).Elem():    secretEnc,
		reflect2.TypeOfPtr((*amcfg.SecretURL)(nil)).Elem():     amcfgSecretURLEnc,
		reflect2.TypeOfPtr((*receivers.SecretURL)(nil)).Elem(): receiversSecretURLEnc,
		// Pointer types
		reflect2.TypeOfPtr((*amcfg.Secret)(nil)):        &jsoniter.OptionalEncoder{ValueEncoder: secretEnc},
		reflect2.TypeOfPtr((*commoncfg.Secret)(nil)):    &jsoniter.OptionalEncoder{ValueEncoder: secretEnc},
		reflect2.TypeOfPtr((*receivers.Secret)(nil)):    &jsoniter.OptionalEncoder{ValueEncoder: secretEnc},
		reflect2.TypeOfPtr((*amcfg.SecretURL)(nil)):     &jsoniter.OptionalEncoder{ValueEncoder: amcfgSecretURLEnc},
		reflect2.TypeOfPtr((*receivers.SecretURL)(nil)): &jsoniter.OptionalEncoder{ValueEncoder: receiversSecretURLEnc},
	}

	api.RegisterExtension(extension)

	return api
}

var (
	plainJSON = newPlainAPI()
)

// MarshalJSONWithSecrets marshals the given value to JSON with secrets in plain text.
//
// alertmanager's and prometheus' Secret and SecretURL types mask their values
// when marshaled with the standard JSON or YAML marshallers. This function
// preserves the values of these types by using a custom JSON encoder.
func MarshalJSONWithSecrets(v any) ([]byte, error) {
	return plainJSON.Marshal(v)
}
