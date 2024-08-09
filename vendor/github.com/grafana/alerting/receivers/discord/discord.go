package discord

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
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

// Constants and models are set according to the official documentation https://discord.com/developers/docs/resources/webhook#execute-webhook-jsonform-params

type discordEmbedType string

const (
	discordRichEmbed discordEmbedType = "rich"

	discordMaxEmbeds     = 10
	discordMaxMessageLen = 2000
)

type discordMessage struct {
	Username  string             `json:"username,omitempty"`
	Content   string             `json:"content"`
	AvatarURL string             `json:"avatar_url,omitempty"`
	Embeds    []discordLinkEmbed `json:"embeds,omitempty"`
}

// discordLinkEmbed implements https://discord.com/developers/docs/resources/channel#embed-object
type discordLinkEmbed struct {
	Title string           `json:"title,omitempty"`
	Type  discordEmbedType `json:"type,omitempty"`
	URL   string           `json:"url,omitempty"`
	Color int64            `json:"color,omitempty"`

	Footer *discordFooter `json:"footer,omitempty"`

	Image *discordImage `json:"image,omitempty"`
}

// discordFooter implements https://discord.com/developers/docs/resources/channel#embed-object-embed-footer-structure
type discordFooter struct {
	Text    string `json:"text"`
	IconURL string `json:"icon_url,omitempty"`
}

// discordImage implements https://discord.com/developers/docs/resources/channel#embed-object-embed-footer-structure
type discordImage struct {
	URL string `json:"url"`
}

// discordError implements https://discord.com/developers/docs/reference#error-messages except for Errors field that is not used in the code
type discordError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type Notifier struct {
	*receivers.Base
	log        logging.Logger
	ns         receivers.WebhookSender
	images     images.Provider
	tmpl       *templates.Template
	settings   Config
	appVersion string
}

type discordAttachment struct {
	url       string
	reader    io.ReadCloser
	name      string
	alertName string
	state     model.AlertStatus
}

func New(cfg Config, meta receivers.Metadata, template *templates.Template, sender receivers.WebhookSender, images images.Provider, logger logging.Logger, appVersion string) *Notifier {
	return &Notifier{
		Base:       receivers.NewBase(meta),
		log:        logger,
		ns:         sender,
		images:     images,
		tmpl:       template,
		settings:   cfg,
		appVersion: appVersion,
	}
}

func (d Notifier) Notify(ctx context.Context, as ...*types.Alert) (bool, error) {
	alerts := types.Alerts(as...)

	var msg discordMessage

	if !d.settings.UseDiscordUsername {
		msg.Username = "Grafana"
	}

	var tmplErr error
	tmpl, _ := templates.TmplText(ctx, d.tmpl, as, d.log, &tmplErr)

	msg.Content = tmpl(d.settings.Message)
	if tmplErr != nil {
		d.log.Warn("failed to template Discord notification content", "error", tmplErr.Error())
		// Reset tmplErr for templating other fields.
		tmplErr = nil
	}
	truncatedMsg, truncated := receivers.TruncateInRunes(msg.Content, discordMaxMessageLen)
	if truncated {
		key, err := notify.ExtractGroupKey(ctx)
		if err != nil {
			return false, err
		}
		d.log.Warn("Truncated content", "key", key, "max_runes", discordMaxMessageLen)
		msg.Content = truncatedMsg
	}

	if d.settings.AvatarURL != "" {
		msg.AvatarURL = tmpl(d.settings.AvatarURL)
		if tmplErr != nil {
			d.log.Warn("failed to template Discord Avatar URL", "error", tmplErr.Error(), "fallback", d.settings.AvatarURL)
			msg.AvatarURL = d.settings.AvatarURL
			tmplErr = nil
		}
	}

	footer := &discordFooter{
		Text:    "Grafana v" + d.appVersion,
		IconURL: "https://grafana.com/static/assets/img/fav32.png",
	}

	var linkEmbed discordLinkEmbed

	linkEmbed.Title = tmpl(d.settings.Title)
	if tmplErr != nil {
		d.log.Warn("failed to template Discord notification title", "error", tmplErr.Error())
		// Reset tmplErr for templating other fields.
		tmplErr = nil
	}
	linkEmbed.Footer = footer
	linkEmbed.Type = discordRichEmbed

	color, _ := strconv.ParseInt(strings.TrimLeft(receivers.GetAlertStatusColor(alerts.Status()), "#"), 16, 0)
	linkEmbed.Color = color

	ruleURL := receivers.JoinURLPath(d.tmpl.ExternalURL.String(), "/alerting/list", d.log)
	linkEmbed.URL = ruleURL

	embeds := []discordLinkEmbed{linkEmbed}

	attachments := d.constructAttachments(ctx, as, discordMaxEmbeds-1)
	for _, a := range attachments {
		color, _ := strconv.ParseInt(strings.TrimLeft(receivers.GetAlertStatusColor(alerts.Status()), "#"), 16, 0)
		embed := discordLinkEmbed{
			Image: &discordImage{
				URL: a.url,
			},
			Color: color,
			Title: a.alertName,
		}
		embeds = append(embeds, embed)
	}

	msg.Embeds = embeds

	if tmplErr != nil {
		d.log.Warn("failed to template Discord message", "error", tmplErr.Error())
		tmplErr = nil
	}

	u := tmpl(d.settings.WebhookURL)
	if tmplErr != nil {
		d.log.Warn("failed to template Discord URL", "error", tmplErr.Error(), "fallback", d.settings.WebhookURL)
		u = d.settings.WebhookURL
	}

	body, err := json.Marshal(msg)
	if err != nil {
		return false, err
	}

	cmd, err := d.buildRequest(u, body, attachments)
	if err != nil {
		return false, err
	}

	cmd.Validation = func(body []byte, statusCode int) error {
		if statusCode/100 != 2 {
			d.log.Error("failed to send notification to Discord", "statusCode", statusCode, "responseBody", string(body))
			errBody := discordError{}
			if err := json.Unmarshal(body, &errBody); err == nil {
				return fmt.Errorf("the Discord API responded (status %d) with error code %d: %s", statusCode, errBody.Code, errBody.Message)
			}
			return fmt.Errorf("unexpected status code %d from Discord", statusCode)
		}
		return nil
	}
	if err := d.ns.SendWebhook(ctx, cmd); err != nil {
		return false, err
	}
	return true, nil
}

func (d Notifier) SendResolved() bool {
	return !d.GetDisableResolveMessage()
}

func (d Notifier) constructAttachments(ctx context.Context, alerts []*types.Alert, embedQuota int) []discordAttachment {
	attachments := make([]discordAttachment, 0, embedQuota)
	embedsUsed := 0
	for _, alert := range alerts {
		// Check if the image limit has been reached at the start of each iteration.
		if embedsUsed >= embedQuota {
			d.log.Warn("Discord embed quota reached, not creating more attachments for this notification", "embedQuota", embedQuota)
			break
		}

		attachment, err := d.getAttachment(ctx, alert)
		if err != nil {
			if errors.Is(err, images.ErrNoImageForAlert) {
				// There's no image for this alert, continue.
				continue
			}
			d.log.Error("failed to create an attachment for Discord", "alert", alert, "error", err)
			continue
		}

		// We got an attachment, either using the image URL or bytes.
		attachments = append(attachments, attachment)
		embedsUsed++
	}

	return attachments
}

// getAttachment takes an alert and generates a Discord attachment containing an image for it.
// If the image has no public URL, it uses the raw bytes for uploading directly to Discord.
func (d Notifier) getAttachment(ctx context.Context, alert *types.Alert) (discordAttachment, error) {
	attachment, err := d.getAttachmentFromURL(ctx, alert)
	if errors.Is(err, images.ErrImagesNoURL) {
		// There's an image but it has no public URL, use the bytes for the attachment.
		return d.getAttachmentFromBytes(ctx, alert)
	}

	return attachment, err
}

func (d Notifier) getAttachmentFromURL(ctx context.Context, alert *types.Alert) (discordAttachment, error) {
	url, err := d.images.GetImageURL(ctx, alert)
	if err != nil {
		return discordAttachment{}, err
	}

	return discordAttachment{
		url:       url,
		state:     alert.Status(),
		alertName: alert.Name(),
	}, nil
}

func (d Notifier) getAttachmentFromBytes(ctx context.Context, alert *types.Alert) (discordAttachment, error) {
	r, name, err := d.images.GetRawImage(ctx, alert)
	if err != nil {
		return discordAttachment{}, err
	}
	return discordAttachment{
		url:       "attachment://" + name,
		name:      name,
		reader:    r,
		state:     alert.Status(),
		alertName: alert.Name(),
	}, nil
}

func (d Notifier) buildRequest(url string, body []byte, attachments []discordAttachment) (*receivers.SendWebhookSettings, error) {
	cmd := &receivers.SendWebhookSettings{
		URL:        url,
		HTTPMethod: "POST",
	}
	if len(attachments) == 0 {
		cmd.ContentType = "application/json"
		cmd.Body = string(body)
		return cmd, nil
	}

	var b bytes.Buffer
	w := multipart.NewWriter(&b)
	defer func() {
		if err := w.Close(); err != nil {
			// Shouldn't matter since we already close w explicitly on the non-error path
			d.log.Warn("failed to close multipart writer", "error", err)
		}
	}()

	payload, err := w.CreateFormField("payload_json")
	if err != nil {
		return nil, err
	}

	if _, err := payload.Write(body); err != nil {
		return nil, err
	}

	for _, a := range attachments {
		if a.reader != nil { // We have an image to upload.
			err = func() error {
				defer func() { _ = a.reader.Close() }()
				part, err := w.CreateFormFile("", a.name)
				if err != nil {
					return err
				}
				_, err = io.Copy(part, a.reader)
				return err
			}()
			if err != nil {
				return nil, err
			}
		}
	}

	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("failed to close multipart writer: %w", err)
	}

	cmd.ContentType = w.FormDataContentType()
	cmd.Body = b.String()
	return cmd, nil
}
