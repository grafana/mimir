package huma

import (
	"fmt"
	"net/http"
	"strconv"
)

// ErrorDetailer returns error details for responses & debugging. This enables
// the use of custom error types. See `NewError` for more details.
type ErrorDetailer interface {
	ErrorDetail() *ErrorDetail
}

// ErrorDetail provides details about a specific error.
type ErrorDetail struct {
	// Message is a human-readable explanation of the error.
	Message string `json:"message,omitempty" doc:"Error message text"`

	// Location is a path-like string indicating where the error occurred.
	// It typically begins with `path`, `query`, `header`, or `body`. Example:
	// `body.items[3].tags` or `path.thing-id`.
	Location string `json:"location,omitempty" doc:"Where the error occurred, e.g. 'body.items[3].tags' or 'path.thing-id'"`

	// Value is the value at the given location, echoed back to the client
	// to help with debugging. This can be useful for e.g. validating that
	// the client didn't send extra whitespace or help when the client
	// did not log an outgoing request.
	Value any `json:"value,omitempty" doc:"The value at the given location"`
}

// Error returns the error message / satisfies the `error` interface. If a
// location and value are set, they will be included in the error message,
// otherwise just the message is returned.
func (e *ErrorDetail) Error() string {
	if e.Location == "" && e.Value == nil {
		return e.Message
	}
	return fmt.Sprintf("%s (%s: %v)", e.Message, e.Location, e.Value)
}

// ErrorDetail satisfies the `ErrorDetailer` interface.
func (e *ErrorDetail) ErrorDetail() *ErrorDetail {
	return e
}

// ErrorModel defines a basic error message model based on RFC 7807 Problem
// Details for HTTP APIs (https://datatracker.ietf.org/doc/html/rfc7807). It
// is augmented with an `errors` field of `huma.ErrorDetail` objects that
// can help provide exhaustive & descriptive errors.
//
//	err := &huma.ErrorModel{
//		Title: http.StatusText(http.StatusBadRequest),
//		Status http.StatusBadRequest,
//		Detail: "Validation failed",
//		Errors: []*huma.ErrorDetail{
//			&huma.ErrorDetail{
//				Message: "expected required property id to be present",
//				Location: "body.friends[0]",
//				Value: nil,
//			},
//			&huma.ErrorDetail{
//				Message: "expected boolean",
//				Location: "body.friends[1].active",
//				Value: 5,
//			},
//		},
//	}
type ErrorModel struct {
	// Type is a URI to get more information about the error type.
	Type string `json:"type,omitempty" format:"uri" default:"about:blank" example:"https://example.com/errors/example" doc:"A URI reference to human-readable documentation for the error."`

	// Title provides a short static summary of the problem. Huma will default this
	// to the HTTP response status code text if not present.
	Title string `json:"title,omitempty" example:"Bad Request" doc:"A short, human-readable summary of the problem type. This value should not change between occurrences of the error."`

	// Status provides the HTTP status code for client convenience. Huma will
	// default this to the response status code if unset. This SHOULD match the
	// response status code (though proxies may modify the actual status code).
	Status int `json:"status,omitempty" example:"400" doc:"HTTP status code"`

	// Detail is an explanation specific to this error occurrence.
	Detail string `json:"detail,omitempty" example:"Property foo is required but is missing." doc:"A human-readable explanation specific to this occurrence of the problem."`

	// Instance is a URI to get more info about this error occurrence.
	Instance string `json:"instance,omitempty" format:"uri" example:"https://example.com/error-log/abc123" doc:"A URI reference that identifies the specific occurrence of the problem."`

	// Errors provides an optional mechanism of passing additional error details
	// as a list.
	Errors []*ErrorDetail `json:"errors,omitempty" doc:"Optional list of individual error details"`
}

// Error satisfies the `error` interface. It returns the error's detail field.
func (e *ErrorModel) Error() string {
	return e.Detail
}

// Add an error to the `Errors` slice. If passed a struct that satisfies the
// `huma.ErrorDetailer` interface, then it is used, otherwise the error
// string is used as the error detail message.
//
//	err := &ErrorModel{ /* ... */ }
//	err.Add(&huma.ErrorDetail{
//		Message: "expected boolean",
//		Location: "body.friends[1].active",
//		Value: 5
//	})
func (e *ErrorModel) Add(err error) {
	if converted, ok := err.(ErrorDetailer); ok {
		e.Errors = append(e.Errors, converted.ErrorDetail())
		return
	}

	e.Errors = append(e.Errors, &ErrorDetail{Message: err.Error()})
}

// GetStatus returns the HTTP status that should be returned to the client
// for this error.
func (e *ErrorModel) GetStatus() int {
	return e.Status
}

// ContentType provides a filter to adjust response content types. This is
// used to ensure e.g. `application/problem+json` content types defined in
// RFC 7807 Problem Details for HTTP APIs are used in responses to clients.
func (e *ErrorModel) ContentType(ct string) string {
	if ct == "application/json" {
		return "application/problem+json"
	}
	if ct == "application/cbor" {
		return "application/problem+cbor"
	}
	return ct
}

// ContentTypeFilter allows you to override the content type for responses,
// allowing you to return a different content type like
// `application/problem+json` after using the `application/json` marshaller.
// This should be implemented by the response body struct.
type ContentTypeFilter interface {
	ContentType(string) string
}

// StatusError is an error that has an HTTP status code. When returned from
// an operation handler, this sets the response status code before sending it
// to the client.
type StatusError interface {
	GetStatus() int
	Error() string
}

// NewError creates a new instance of an error model with the given status code,
// message, and optional error details. If the error details implement the
// `ErrorDetailer` interface, the error details will be used. Otherwise, the
// error string will be used as the message. This function is used by all the
// error response utility functions, like `huma.Error400BadRequest`.
//
// Replace this function to use your own error type. Example:
//
//	type MyDetail struct {
//		Message string	`json:"message"`
//		Location string	`json:"location"`
//	}
//
//	type MyError struct {
//		status  int
//		Message string	`json:"message"`
//		Errors  []error	`json:"errors"`
//	}
//
//	func (e *MyError) Error() string {
//		return e.Message
//	}
//
//	func (e *MyError) GetStatus() int {
//		return e.status
//	}
//
//	huma.NewError = func(status int, msg string, errs ...error) StatusError {
//		return &MyError{
//			status:  status,
//			Message: msg,
//			Errors:  errs,
//		}
//	}
var NewError = func(status int, msg string, errs ...error) StatusError {
	details := make([]*ErrorDetail, len(errs))
	for i := 0; i < len(errs); i++ {
		if converted, ok := errs[i].(ErrorDetailer); ok {
			details[i] = converted.ErrorDetail()
		} else {
			details[i] = &ErrorDetail{Message: errs[i].Error()}
		}
	}
	return &ErrorModel{
		Status: status,
		Title:  http.StatusText(status),
		Detail: msg,
		Errors: details,
	}
}

// WriteErr writes an error response with the given context, using the
// configured error type and with the given status code and message. It is
// marshaled using the API's content negotiation methods.
func WriteErr(api API, ctx Context, status int, msg string, errs ...error) error {
	var err any = NewError(status, msg, errs...)

	ct, negotiateErr := api.Negotiate(ctx.Header("Accept"))
	if negotiateErr != nil {
		return negotiateErr
	}

	if ctf, ok := err.(ContentTypeFilter); ok {
		ct = ctf.ContentType(ct)
	}

	ctx.SetHeader("Content-Type", ct)
	ctx.SetStatus(status)
	tval, terr := api.Transform(ctx, strconv.Itoa(status), err)
	if terr != nil {
		return terr
	}
	return api.Marshal(ctx.BodyWriter(), ct, tval)
}

// Status304NotModified returns a 304. This is not really an error, but
// provides a way to send non-default responses.
func Status304NotModified() StatusError {
	return NewError(http.StatusNotModified, "")
}

// Error400BadRequest returns a 400.
func Error400BadRequest(msg string, errs ...error) StatusError {
	return NewError(http.StatusBadRequest, msg, errs...)
}

// Error401Unauthorized returns a 401.
func Error401Unauthorized(msg string, errs ...error) StatusError {
	return NewError(http.StatusUnauthorized, msg, errs...)
}

// Error403Forbidden returns a 403.
func Error403Forbidden(msg string, errs ...error) StatusError {
	return NewError(http.StatusForbidden, msg, errs...)
}

// Error404NotFound returns a 404.
func Error404NotFound(msg string, errs ...error) StatusError {
	return NewError(http.StatusNotFound, msg, errs...)
}

// Error405MethodNotAllowed returns a 405.
func Error405MethodNotAllowed(msg string, errs ...error) StatusError {
	return NewError(http.StatusMethodNotAllowed, msg, errs...)
}

// Error406NotAcceptable returns a 406.
func Error406NotAcceptable(msg string, errs ...error) StatusError {
	return NewError(http.StatusNotAcceptable, msg, errs...)
}

// Error409Conflict returns a 409.
func Error409Conflict(msg string, errs ...error) StatusError {
	return NewError(http.StatusConflict, msg, errs...)
}

// Error410Gone returns a 410.
func Error410Gone(msg string, errs ...error) StatusError {
	return NewError(http.StatusGone, msg, errs...)
}

// Error412PreconditionFailed returns a 412.
func Error412PreconditionFailed(msg string, errs ...error) StatusError {
	return NewError(http.StatusPreconditionFailed, msg, errs...)
}

// Error415UnsupportedMediaType returns a 415.
func Error415UnsupportedMediaType(msg string, errs ...error) StatusError {
	return NewError(http.StatusUnsupportedMediaType, msg, errs...)
}

// Error422UnprocessableEntity returns a 422.
func Error422UnprocessableEntity(msg string, errs ...error) StatusError {
	return NewError(http.StatusUnprocessableEntity, msg, errs...)
}

// Error429TooManyRequests returns a 429.
func Error429TooManyRequests(msg string, errs ...error) StatusError {
	return NewError(http.StatusTooManyRequests, msg, errs...)
}

// Error500InternalServerError returns a 500.
func Error500InternalServerError(msg string, errs ...error) StatusError {
	return NewError(http.StatusInternalServerError, msg, errs...)
}

// Error501NotImplemented returns a 501.
func Error501NotImplemented(msg string, errs ...error) StatusError {
	return NewError(http.StatusNotImplemented, msg, errs...)
}

// Error502BadGateway returns a 502.
func Error502BadGateway(msg string, errs ...error) StatusError {
	return NewError(http.StatusBadGateway, msg, errs...)
}

// Error503ServiceUnavailable returns a 503.
func Error503ServiceUnavailable(msg string, errs ...error) StatusError {
	return NewError(http.StatusServiceUnavailable, msg, errs...)
}

// Error504GatewayTimeout returns a 504.
func Error504GatewayTimeout(msg string, errs ...error) StatusError {
	return NewError(http.StatusGatewayTimeout, msg, errs...)
}
