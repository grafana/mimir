package backend

import (
	"fmt"
	"net/http"
	"net/textproto"
	"strings"
)

const (
	// OAuthIdentityTokenHeaderName the header name used for forwarding
	// OAuth Identity access token.
	OAuthIdentityTokenHeaderName = "Authorization"

	// OAuthIdentityIDTokenHeaderName the header name used for forwarding
	// OAuth Identity ID token.
	OAuthIdentityIDTokenHeaderName = "X-Id-Token"

	// CookiesHeaderName the header name used for forwarding
	// cookies.
	CookiesHeaderName = "Cookie"

	httpHeaderPrefix = "http_"
)

// ForwardHTTPHeaders interface marking that forward of HTTP headers is supported.
type ForwardHTTPHeaders interface {
	// SetHTTPHeader sets the header entries associated with key to the
	// single element value. It replaces any existing values
	// associated with key. The key is case insensitive; it is
	// canonicalized by textproto.CanonicalMIMEHeaderKey.
	SetHTTPHeader(key, value string)

	// DeleteHTTPHeader deletes the values associated with key.
	// The key is case insensitive; it is canonicalized by
	// CanonicalHeaderKey.
	DeleteHTTPHeader(key string)

	// GetHTTPHeader gets the first value associated with the given key. If
	// there are no values associated with the key, Get returns "".
	// It is case insensitive; textproto.CanonicalMIMEHeaderKey is
	// used to canonicalize the provided key. Get assumes that all
	// keys are stored in canonical form.
	GetHTTPHeader(key string) string

	// GetHTTPHeaders returns HTTP headers.
	GetHTTPHeaders() http.Header
}

func setHTTPHeaderInStringMap(headers map[string]string, key string, value string) {
	if headers == nil {
		headers = map[string]string{}
	}

	headers[fmt.Sprintf("%s%s", httpHeaderPrefix, key)] = value
}

func getHTTPHeadersFromStringMap(headers map[string]string) http.Header {
	httpHeaders := http.Header{}

	for k, v := range headers {
		if textproto.CanonicalMIMEHeaderKey(k) == OAuthIdentityTokenHeaderName {
			httpHeaders.Set(k, v)
		}

		if textproto.CanonicalMIMEHeaderKey(k) == OAuthIdentityIDTokenHeaderName {
			httpHeaders.Set(k, v)
		}

		if textproto.CanonicalMIMEHeaderKey(k) == CookiesHeaderName {
			httpHeaders.Set(k, v)
		}

		if strings.HasPrefix(k, httpHeaderPrefix) {
			hKey := strings.TrimPrefix(k, httpHeaderPrefix)
			httpHeaders.Set(hKey, v)
		}
	}

	return httpHeaders
}

func deleteHTTPHeaderInStringMap(headers map[string]string, key string) {
	for k := range headers {
		if textproto.CanonicalMIMEHeaderKey(k) == textproto.CanonicalMIMEHeaderKey(key) ||
			textproto.CanonicalMIMEHeaderKey(k) == textproto.CanonicalMIMEHeaderKey(fmt.Sprintf("%s%s", httpHeaderPrefix, key)) {
			delete(headers, k)
			break
		}
	}
}
