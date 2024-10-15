package images

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/prometheus/alertmanager/types"
)

var (
	ErrImageNotFound = errors.New("image not found")

	// ErrImagesDone is used to stop iteration of subsequent images. It should be
	// returned from forEachFunc when either the intended image has been found or
	// the maximum number of images has been iterated.
	ErrImagesDone = errors.New("images done")

	// ErrImagesNoPath is returned whenever an image is found but has no path on disk.
	ErrImagesNoPath = errors.New("no path for image")

	// ErrImagesNoURL is returned whenever an image is found but has no URL.
	ErrImagesNoURL = errors.New("no URL for image")

	ErrImagesUnavailable = errors.New("alert screenshots are unavailable")

	// ErrNoImageForAlert is returned when no image is associated to a given alert.
	ErrNoImageForAlert = errors.New("no image for alert")
)

type Image struct {
	Token     string
	Path      string
	URL       string
	CreatedAt time.Time
}

func (i Image) HasURL() bool {
	return i.URL != ""
}

type Provider interface {
	// GetImage takes a token (identifier) and returns the image that token belongs to.
	// Returns `ErrImageNotFound` if there's no image for said token.
	//
	// Deprecated: This method will be removed when all integrations use GetImageURL and/or GetRawImage,
	// which allow integrations to get just the data they need for adding images to notifications.
	// Use any of those two methods instead.
	GetImage(ctx context.Context, token string) (*Image, error)

	// GetImageURL returns the URL of an image associated with a given alert.
	//	- Returns `ErrImageNotFound` if no image is found.
	//	- Returns `ErrImagesNoURL` if the image doesn't have a URL.
	GetImageURL(ctx context.Context, alert *types.Alert) (string, error)

	// GetRawImage returns an io.Reader to read the bytes of an image associated with a given alert
	// and a string representing the filename.
	//	- Returns `ErrImageNotFound` if no image is found.
	//	- Returns `ErrImagesNoPath` if the image doesn't have a path on disk.
	GetRawImage(ctx context.Context, alert *types.Alert) (io.ReadCloser, string, error)
}

type UnavailableProvider struct{}

// GetImage returns the image with the corresponding token, or ErrImageNotFound.
func (u *UnavailableProvider) GetImage(context.Context, string) (*Image, error) {
	return nil, ErrImagesUnavailable
}

func (u *UnavailableProvider) GetImageURL(context.Context, *types.Alert) (string, error) {
	return "", ErrImagesUnavailable
}

func (u *UnavailableProvider) GetRawImage(context.Context, *types.Alert) (io.ReadCloser, string, error) {
	return nil, "", ErrImagesUnavailable
}
