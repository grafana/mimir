package pushover

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"mime/multipart"
	"os"
	"strconv"
	"strings"

	"github.com/prometheus/alertmanager/notify"

	"github.com/prometheus/alertmanager/types"
	"github.com/prometheus/common/model"

	"github.com/grafana/alerting/images"
	"github.com/grafana/alerting/logging"
	"github.com/grafana/alerting/receivers"
	"github.com/grafana/alerting/templates"
)

const (
	pushoverMaxFileSize = 1 << 21 // 2MB
	// https://pushover.net/api#limits - 250 characters or runes.
	pushoverMaxTitleLenRunes = 250
	// https://pushover.net/api#limits - 1024 characters or runes.
	pushoverMaxMessageLenRunes = 1024
	// https://pushover.net/api#limits - 512 characters or runes.
	pushoverMaxURLLenRunes = 512
)

var (
	// APIURL of where the notification payload is sent. It is public to be overridable in integration tests.
	APIURL = "https://api.pushover.net/1/messages.json"
)

// Notifier is responsible for sending
// alert notifications to Pushover
type Notifier struct {
	*receivers.Base
	tmpl     *templates.Template
	log      logging.Logger
	images   images.Provider
	ns       receivers.WebhookSender
	settings Config
}

// New is the constructor for the pushover notifier
func New(cfg Config, meta receivers.Metadata, template *templates.Template, sender receivers.WebhookSender, images images.Provider, logger logging.Logger) *Notifier {
	return &Notifier{
		Base:     receivers.NewBase(meta),
		log:      logger,
		ns:       sender,
		images:   images,
		tmpl:     template,
		settings: cfg,
	}
}

// Notify sends an alert notification to Slack.
func (pn *Notifier) Notify(ctx context.Context, as ...*types.Alert) (bool, error) {
	headers, uploadBody, err := pn.genPushoverBody(ctx, as...)
	if err != nil {
		pn.log.Error("Failed to generate body for pushover", "error", err)
		return false, err
	}

	cmd := &receivers.SendWebhookSettings{
		URL:        APIURL,
		HTTPMethod: "POST",
		HTTPHeader: headers,
		Body:       uploadBody.String(),
	}

	if err := pn.ns.SendWebhook(ctx, cmd); err != nil {
		pn.log.Error("failed to send pushover notification", "error", err, "webhook", pn.Name)
		return false, err
	}

	return true, nil
}
func (pn *Notifier) SendResolved() bool {
	return !pn.GetDisableResolveMessage()
}

func (pn *Notifier) genPushoverBody(ctx context.Context, as ...*types.Alert) (map[string]string, bytes.Buffer, error) {
	key, err := notify.ExtractGroupKey(ctx)
	if err != nil {
		return nil, bytes.Buffer{}, err
	}

	b := bytes.Buffer{}
	w := multipart.NewWriter(&b)

	// tests use a non-random boundary separator
	if boundary := receivers.GetBoundary(); boundary != "" {
		err := w.SetBoundary(boundary)
		if err != nil {
			return nil, b, err
		}
	}

	var tmplErr error
	tmpl, _ := templates.TmplText(ctx, pn.tmpl, as, pn.log, &tmplErr)

	if err := w.WriteField("user", tmpl(pn.settings.UserKey)); err != nil {
		return nil, b, fmt.Errorf("failed to write the user: %w", err)
	}

	if err := w.WriteField("token", pn.settings.APIToken); err != nil {
		return nil, b, fmt.Errorf("failed to write the token: %w", err)
	}

	title, truncated := receivers.TruncateInRunes(tmpl(pn.settings.Title), pushoverMaxTitleLenRunes)
	if truncated {
		pn.log.Warn("Truncated title", "incident", key, "max_runes", pushoverMaxTitleLenRunes)
	}
	message := tmpl(pn.settings.Message)
	message, truncated = receivers.TruncateInRunes(message, pushoverMaxMessageLenRunes)
	if truncated {
		pn.log.Warn("Truncated message", "incident", key, "max_runes", pushoverMaxMessageLenRunes)
	}
	message = strings.TrimSpace(message)
	if message == "" {
		// Pushover rejects empty messages.
		message = "(no details)"
	}

	supplementaryURL := receivers.JoinURLPath(pn.tmpl.ExternalURL.String(), "/alerting/list", pn.log)
	supplementaryURL, truncated = receivers.TruncateInRunes(supplementaryURL, pushoverMaxURLLenRunes)
	if truncated {
		pn.log.Warn("Truncated URL", "incident", key, "max_runes", pushoverMaxURLLenRunes)
	}

	status := types.Alerts(as...).Status()
	priority := pn.settings.AlertingPriority
	if status == model.AlertResolved {
		priority = pn.settings.OkPriority
	}
	if err := w.WriteField("priority", strconv.FormatInt(priority, 10)); err != nil {
		return nil, b, fmt.Errorf("failed to write the priority: %w", err)
	}

	if priority == 2 {
		if err := w.WriteField("retry", strconv.FormatInt(pn.settings.Retry, 10)); err != nil {
			return nil, b, fmt.Errorf("failed to write retry: %w", err)
		}

		if err := w.WriteField("expire", strconv.FormatInt(pn.settings.Expire, 10)); err != nil {
			return nil, b, fmt.Errorf("failed to write expire: %w", err)
		}
	}

	if pn.settings.Device != "" {
		if err := w.WriteField("device", tmpl(pn.settings.Device)); err != nil {
			return nil, b, fmt.Errorf("failed to write the device: %w", err)
		}
	}

	if err := w.WriteField("title", title); err != nil {
		return nil, b, fmt.Errorf("failed to write the title: %w", err)
	}

	if err := w.WriteField("url", supplementaryURL); err != nil {
		return nil, b, fmt.Errorf("failed to write the URL: %w", err)
	}

	if err := w.WriteField("url_title", "Show alert rule"); err != nil {
		return nil, b, fmt.Errorf("failed to write the URL title: %w", err)
	}

	if err := w.WriteField("message", message); err != nil {
		return nil, b, fmt.Errorf("failed write the message: %w", err)
	}

	if pn.settings.Upload {
		pn.writeImageParts(ctx, w, as...)
	} else {
		pn.log.Debug("skip uploading image because of the configuration")
	}

	var sound string
	if status == model.AlertResolved {
		sound = tmpl(pn.settings.OkSound)
	} else {
		sound = tmpl(pn.settings.AlertingSound)
	}
	if sound != "default" {
		if err := w.WriteField("sound", sound); err != nil {
			return nil, b, fmt.Errorf("failed to write the sound: %w", err)
		}
	}

	// Mark the message as HTML
	if err := w.WriteField("html", "1"); err != nil {
		return nil, b, fmt.Errorf("failed to mark the message as HTML: %w", err)
	}
	if err := w.Close(); err != nil {
		return nil, b, fmt.Errorf("failed to close the multipart request: %w", err)
	}

	if tmplErr != nil {
		pn.log.Warn("failed to template pushover message", "error", tmplErr.Error())
	}

	headers := map[string]string{
		"Content-Type": w.FormDataContentType(),
	}

	return headers, b, nil
}

func (pn *Notifier) writeImageParts(ctx context.Context, w *multipart.Writer, as ...*types.Alert) {
	// Pushover supports at most one image attachment with a maximum size of pushoverMaxFileSize.
	// If the image is larger than pushoverMaxFileSize then return an error.
	err := images.WithStoredImages(ctx, pn.log, pn.images, func(_ int, image images.Image) error {
		f, err := os.Open(image.Path)
		if err != nil {
			return fmt.Errorf("failed to open the image: %w", err)
		}
		defer func() {
			if err := f.Close(); err != nil {
				pn.log.Error("failed to close the image", "file", image.Path)
			}
		}()

		fileInfo, err := f.Stat()
		if err != nil {
			return fmt.Errorf("failed to stat the image: %w", err)
		}

		if fileInfo.Size() > pushoverMaxFileSize {
			return fmt.Errorf("image would exceeded maximum file size: %d", fileInfo.Size())
		}

		fw, err := w.CreateFormFile("attachment", image.Path)
		if err != nil {
			return fmt.Errorf("failed to create form file for the image: %w", err)
		}

		if _, err = io.Copy(fw, f); err != nil {
			return fmt.Errorf("failed to copy the image to the form file: %w", err)
		}

		return images.ErrImagesDone
	}, as...)
	if err != nil {
		pn.log.Error("failed to fetch image for the notification", "error", err)
	}
}
