package receivers

import (
	"unsafe"

	jsoniter "github.com/json-iterator/go"
	"github.com/modern-go/reflect2"
	amcfg "github.com/prometheus/alertmanager/config"
	commoncfg "github.com/prometheus/common/config"
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
	v := (*SecretURL)(ptr)
	u := URL(*v)
	if u.URL == nil {
		return nil
	}
	s := u.String()
	return &s
}

func newPlainSecretsAPI() jsoniter.API {
	api := jsoniter.ConfigCompatibleWithStandardLibrary

	secretEnc := &secretEncoder{}
	amcfgSecretURLEnc := &secretURLEncoder{getURLString: getAmcfgURLString}
	receiversSecretURLEnc := &secretURLEncoder{getURLString: getReceiversURLString}

	extension := jsoniter.EncoderExtension{
		// Value types
		reflect2.TypeOfPtr((*amcfg.Secret)(nil)).Elem():     secretEnc,
		reflect2.TypeOfPtr((*commoncfg.Secret)(nil)).Elem(): secretEnc,
		reflect2.TypeOfPtr((*Secret)(nil)).Elem():           secretEnc,
		reflect2.TypeOfPtr((*amcfg.SecretURL)(nil)).Elem():  amcfgSecretURLEnc,
		reflect2.TypeOfPtr((*SecretURL)(nil)).Elem():        receiversSecretURLEnc,
		// Pointer types
		reflect2.TypeOfPtr((*amcfg.Secret)(nil)):     &jsoniter.OptionalEncoder{ValueEncoder: secretEnc},
		reflect2.TypeOfPtr((*commoncfg.Secret)(nil)): &jsoniter.OptionalEncoder{ValueEncoder: secretEnc},
		reflect2.TypeOfPtr((*Secret)(nil)):           &jsoniter.OptionalEncoder{ValueEncoder: secretEnc},
		reflect2.TypeOfPtr((*amcfg.SecretURL)(nil)):  &jsoniter.OptionalEncoder{ValueEncoder: amcfgSecretURLEnc},
		reflect2.TypeOfPtr((*SecretURL)(nil)):        &jsoniter.OptionalEncoder{ValueEncoder: receiversSecretURLEnc},
	}

	api.RegisterExtension(extension)

	return api
}

var (
	PlainSecretsJSON = newPlainSecretsAPI()
)
