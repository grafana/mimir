package slack

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"
	"time"

	amConfig "github.com/prometheus/alertmanager/config"
	"github.com/prometheus/alertmanager/notify"

	"github.com/prometheus/alertmanager/types"

	"github.com/grafana/alerting/images"
	"github.com/grafana/alerting/logging"
	"github.com/grafana/alerting/receivers"
	"github.com/grafana/alerting/templates"
)

const (
	// maxImagesPerThreadTs is the maximum number of images that can be posted as
	// replies to the same thread_ts. It should prevent tokens from exceeding the
	// rate limits for files.upload https://api.slack.com/docs/rate-limits#tier_t2
	maxImagesPerThreadTs        = 5
	maxImagesPerThreadTsMessage = "There are more images than can be shown here. To see the panels for all firing and resolved alerts please check Grafana"
	footerIconURL               = "https://grafana.com/static/assets/img/fav32.png"
)

// APIURL of where the notification payload is sent. It is public to be overridable in integration tests.
var APIURL = "https://slack.com/api/chat.postMessage"

var (
	slackClient = &http.Client{
		Timeout: time.Second * 30,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				Renegotiation: tls.RenegotiateFreelyAsClient,
			},
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout: 30 * time.Second,
			}).DialContext,
			TLSHandshakeTimeout: 5 * time.Second,
		},
	}
)

type sendFunc func(ctx context.Context, req *http.Request, logger logging.Logger) (string, error)

// https://api.slack.com/reference/messaging/attachments#legacy_fields - 1024, no units given, assuming runes or characters.
const slackMaxTitleLenRunes = 1024

// Notifier is responsible for sending
// alert notification to Slack.
type Notifier struct {
	*receivers.Base
	log           logging.Logger
	tmpl          *templates.Template
	images        images.Provider
	webhookSender receivers.WebhookSender
	sendFn        sendFunc
	settings      Config
	appVersion    string
}

// isIncomingWebhook returns true if the settings are for an incoming webhook.
func isIncomingWebhook(s Config) bool {
	return s.Token == ""
}

// uploadURL returns the upload URL for Slack.
func uploadURL(s Config) (string, error) {
	u, err := url.Parse(s.URL)
	if err != nil {
		return "", fmt.Errorf("failed to parse URL: %w", err)
	}
	dir, _ := path.Split(u.Path)
	u.Path = path.Join(dir, "files.upload")
	return u.String(), nil
}

func New(cfg Config, meta receivers.Metadata, template *templates.Template, sender receivers.WebhookSender, images images.Provider, logger logging.Logger, appVersion string) *Notifier {
	return &Notifier{
		Base:     receivers.NewBase(meta),
		settings: cfg,

		images:        images,
		webhookSender: sender,
		sendFn:        sendSlackRequest,
		log:           logger,
		tmpl:          template,
		appVersion:    appVersion,
	}
}

// slackMessage is the slackMessage for sending a slack notification.
type slackMessage struct {
	Channel     string                   `json:"channel,omitempty"`
	Text        string                   `json:"text,omitempty"`
	Username    string                   `json:"username,omitempty"`
	IconEmoji   string                   `json:"icon_emoji,omitempty"`
	IconURL     string                   `json:"icon_url,omitempty"`
	Attachments []attachment             `json:"attachments"`
	Blocks      []map[string]interface{} `json:"blocks,omitempty"`
	ThreadTs    string                   `json:"thread_ts,omitempty"`
}

// attachment is used to display a richly-formatted message block.
type attachment struct {
	Title      string                `json:"title,omitempty"`
	TitleLink  string                `json:"title_link,omitempty"`
	Text       string                `json:"text"`
	ImageURL   string                `json:"image_url,omitempty"`
	Fallback   string                `json:"fallback"`
	Fields     []amConfig.SlackField `json:"fields,omitempty"`
	Footer     string                `json:"footer"`
	FooterIcon string                `json:"footer_icon"`
	Color      string                `json:"color,omitempty"`
	Ts         int64                 `json:"ts,omitempty"`
	Pretext    string                `json:"pretext,omitempty"`
	MrkdwnIn   []string              `json:"mrkdwn_in,omitempty"`
}

// Notify sends an alert notification to Slack.
func (sn *Notifier) Notify(ctx context.Context, alerts ...*types.Alert) (bool, error) {
	sn.log.Debug("Creating slack message", "alerts", len(alerts))

	m, err := sn.createSlackMessage(ctx, alerts)
	if err != nil {
		sn.log.Error("Failed to create Slack message", "err", err)
		return false, fmt.Errorf("failed to create Slack message: %w", err)
	}

	threadTs, err := sn.sendSlackMessage(ctx, m)
	if err != nil {
		sn.log.Error("Failed to send Slack message", "err", err)
		return false, fmt.Errorf("failed to send Slack message: %w", err)
	}

	// Do not upload images if using an incoming webhook as incoming webhooks cannot upload files
	if !isIncomingWebhook(sn.settings) {
		if err := images.WithStoredImages(ctx, sn.log, sn.images, func(index int, image images.Image) error {
			// If we have exceeded the maximum number of images for this threadTs
			// then tell the recipient and stop iterating subsequent images
			if index >= maxImagesPerThreadTs {
				if _, err := sn.sendSlackMessage(ctx, &slackMessage{
					Channel:  sn.settings.Recipient,
					Text:     maxImagesPerThreadTsMessage,
					ThreadTs: threadTs,
				}); err != nil {
					sn.log.Error("Failed to send Slack message", "err", err)
				}
				return images.ErrImagesDone
			}
			comment := initialCommentForImage(alerts[index])
			return sn.uploadImage(ctx, image, sn.settings.Recipient, comment, threadTs)
		}, alerts...); err != nil {
			// Do not return an error here as we might have exceeded the rate limit for uploading files
			sn.log.Error("Failed to upload image", "err", err)
		}
	}

	return true, nil
}

// sendSlackRequest sends a request to the Slack API.
// Stubbable by tests.
var sendSlackRequest = func(_ context.Context, req *http.Request, logger logging.Logger) (string, error) {
	resp, err := slackClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to send request: %w", err)
	}

	defer func() {
		if err := resp.Body.Close(); err != nil {
			logger.Warn("Failed to close response body", "err", err)
		}
	}()

	if resp.StatusCode < http.StatusOK {
		logger.Error("Unexpected 1xx response", "status", resp.StatusCode)
		return "", fmt.Errorf("unexpected 1xx status code: %d", resp.StatusCode)
	} else if resp.StatusCode >= 300 && resp.StatusCode < 400 {
		logger.Error("Unexpected 3xx response", "status", resp.StatusCode)
		return "", fmt.Errorf("unexpected 3xx status code: %d", resp.StatusCode)
	} else if resp.StatusCode >= http.StatusInternalServerError {
		logger.Error("Unexpected 5xx response", "status", resp.StatusCode)
		return "", fmt.Errorf("unexpected 5xx status code: %d", resp.StatusCode)
	}

	content := resp.Header.Get("Content-Type")
	if strings.HasPrefix(content, "application/json") {
		return handleSlackJSONResponse(resp, logger)
	}
	// If the response is not JSON it could be the response to an incoming webhook
	return handleSlackIncomingWebhookResponse(resp, logger)
}

func handleSlackIncomingWebhookResponse(resp *http.Response, logger logging.Logger) (string, error) {
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read response: %w", err)
	}

	// Incoming webhooks return the string "ok" on success
	if bytes.Equal(b, []byte("ok")) {
		logger.Debug("The incoming webhook was successful")
		return "", nil
	}

	logger.Debug("Incoming webhook was unsuccessful", "status", resp.StatusCode, "body", string(b))

	// There are a number of known errors that we can check. The documentation incoming webhooks
	// errors can be found at https://api.slack.com/messaging/webhooks#handling_errors and
	// https://api.slack.com/changelog/2016-05-17-changes-to-errors-for-incoming-webhooks
	if bytes.Equal(b, []byte("user_not_found")) {
		return "", errors.New("the user does not exist or is invalid")
	}

	if bytes.Equal(b, []byte("channel_not_found")) {
		return "", errors.New("the channel does not exist or is invalid")
	}

	if bytes.Equal(b, []byte("channel_is_archived")) {
		return "", errors.New("cannot send an incoming webhook for an archived channel")
	}

	if bytes.Equal(b, []byte("posting_to_general_channel_denied")) {
		return "", errors.New("cannot send an incoming webhook to the #general channel")
	}

	if bytes.Equal(b, []byte("no_service")) {
		return "", errors.New("the incoming webhook is either disabled, removed, or invalid")
	}

	if bytes.Equal(b, []byte("no_text")) {
		return "", errors.New("cannot send an incoming webhook without a message")
	}

	return "", fmt.Errorf("failed incoming webhook: %s", string(b))
}

func handleSlackJSONResponse(resp *http.Response, logger logging.Logger) (string, error) {
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read response: %w", err)
	}

	if len(b) == 0 {
		logger.Error("Expected JSON but got empty response")
		return "", errors.New("unexpected empty response")
	}

	// Slack responds to some requests with a JSON document, that might contain an error.
	result := struct {
		OK  bool   `json:"ok"`
		Ts  string `json:"ts"`
		Err string `json:"error"`
	}{}

	if err := json.Unmarshal(b, &result); err != nil {
		logger.Error("Failed to unmarshal response", "body", string(b), "err", err)
		return "", fmt.Errorf("failed to unmarshal response: %w", err)
	}

	if !result.OK {
		logger.Error("The request was unsuccessful", "body", string(b), "err", result.Err)
		return "", fmt.Errorf("failed to send request: %s", result.Err)
	}

	logger.Debug("The request was successful")
	return result.Ts, nil
}

func (sn *Notifier) commonAlertGeneratorURL(_ context.Context, alerts []*types.Alert) bool {
	if len(alerts[0].GeneratorURL) == 0 {
		return false
	}
	firstURL := alerts[0].GeneratorURL
	for _, a := range alerts {
		if a.GeneratorURL != firstURL {
			return false
		}
	}
	return true
}

func (sn *Notifier) createSlackMessage(ctx context.Context, alerts []*types.Alert) (*slackMessage, error) {
	var tmplErr error
	tmpl, _ := templates.TmplText(ctx, sn.tmpl, alerts, sn.log, &tmplErr)

	ruleURL := receivers.JoinURLPath(sn.tmpl.ExternalURL.String(), "/alerting/list", sn.log)

	// If all alerts have the same GeneratorURL, use that.
	if sn.commonAlertGeneratorURL(ctx, alerts) {
		ruleURL = alerts[0].GeneratorURL
	}

	title, truncated := receivers.TruncateInRunes(tmpl(sn.settings.Title), slackMaxTitleLenRunes)
	if truncated {
		key, err := notify.ExtractGroupKey(ctx)
		if err != nil {
			return nil, err
		}
		sn.log.Warn("Truncated title", "key", key, "max_runes", slackMaxTitleLenRunes)
	}
	if tmplErr != nil {
		sn.log.Warn("failed to template Slack title", "error", tmplErr.Error())
		tmplErr = nil
	}

	req := &slackMessage{
		Channel:   tmpl(sn.settings.Recipient),
		Username:  tmpl(sn.settings.Username),
		IconEmoji: tmpl(sn.settings.IconEmoji),
		IconURL:   tmpl(sn.settings.IconURL),
		// TODO: We should use the Block Kit API instead:
		// https://api.slack.com/messaging/composing/layouts#when-to-use-attachments
		Attachments: []attachment{
			{
				Color:      receivers.GetAlertStatusColor(types.Alerts(alerts...).Status()),
				Title:      title,
				Fallback:   title,
				Footer:     "Grafana v" + sn.appVersion,
				FooterIcon: footerIconURL,
				Ts:         time.Now().Unix(),
				TitleLink:  ruleURL,
				Text:       tmpl(sn.settings.Text),
				Fields:     nil, // TODO. Should be a config.
			},
		},
	}

	if isIncomingWebhook(sn.settings) {
		// Incoming webhooks cannot upload files, instead share images via their URL
		_ = images.WithStoredImages(ctx, sn.log, sn.images, func(_ int, image images.Image) error {
			if image.URL != "" {
				req.Attachments[0].ImageURL = image.URL
				return images.ErrImagesDone
			}
			return nil
		}, alerts...)
	}

	if tmplErr != nil {
		sn.log.Warn("failed to template Slack message", "error", tmplErr.Error())
	}

	mentionsBuilder := strings.Builder{}
	appendSpace := func() {
		if mentionsBuilder.Len() > 0 {
			mentionsBuilder.WriteString(" ")
		}
	}

	mentionChannel := strings.TrimSpace(sn.settings.MentionChannel)
	if mentionChannel != "" {
		mentionsBuilder.WriteString(fmt.Sprintf("<!%s|%s>", mentionChannel, mentionChannel))
	}

	if len(sn.settings.MentionGroups) > 0 {
		appendSpace()
		for _, g := range sn.settings.MentionGroups {
			mentionsBuilder.WriteString(fmt.Sprintf("<!subteam^%s>", tmpl(g)))
		}
	}

	if len(sn.settings.MentionUsers) > 0 {
		appendSpace()
		for _, u := range sn.settings.MentionUsers {
			mentionsBuilder.WriteString(fmt.Sprintf("<@%s>", tmpl(u)))
		}
	}

	if mentionsBuilder.Len() > 0 {
		// Use markdown-formatted pretext for any mentions.
		req.Attachments[0].MrkdwnIn = []string{"pretext"}
		req.Attachments[0].Pretext = mentionsBuilder.String()
	}

	return req, nil
}

func (sn *Notifier) sendSlackMessage(ctx context.Context, m *slackMessage) (string, error) {
	b, err := json.Marshal(m)
	if err != nil {
		return "", fmt.Errorf("failed to marshal Slack message: %w", err)
	}

	sn.log.Debug("sending Slack API request", "url", sn.settings.URL, "data", string(b))
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, sn.settings.URL, bytes.NewReader(b))
	if err != nil {
		return "", fmt.Errorf("failed to create HTTP request: %w", err)
	}

	request.Header.Set("Content-Type", "application/json; charset=utf-8")
	request.Header.Set("User-Agent", "Grafana")
	if sn.settings.Token == "" {
		if sn.settings.URL == APIURL {
			panic("Token should be set when using the Slack chat API")
		}
		sn.log.Debug("Looks like we are using an incoming webhook, no Authorization header required")
	} else {
		sn.log.Debug("Looks like we are using the Slack API, have set the Bearer token for this request")
		request.Header.Set("Authorization", "Bearer "+sn.settings.Token)
	}

	threadTs, err := sn.sendFn(ctx, request, sn.log)
	if err != nil {
		return "", err
	}

	return threadTs, nil
}

// createImageMultipart returns the mutlipart/form-data request and headers for files.upload.
// It returns an error if the image does not exist or there was an error preparing the
// multipart form.
func (sn *Notifier) createImageMultipart(image images.Image, channel, comment, threadTs string) (http.Header, []byte, error) {
	buf := bytes.Buffer{}
	w := multipart.NewWriter(&buf)
	defer func() {
		if err := w.Close(); err != nil {
			sn.log.Error("Failed to close multipart writer", "err", err)
		}
	}()

	f, err := os.Open(image.Path)
	if err != nil {
		return nil, nil, err
	}
	defer func() {
		if err := f.Close(); err != nil {
			sn.log.Error("Failed to close image file reader", "error", err)
		}
	}()

	fw, err := w.CreateFormFile("file", image.Path)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create form file: %w", err)
	}

	if _, err := io.Copy(fw, f); err != nil {
		return nil, nil, fmt.Errorf("failed to copy file to form: %w", err)
	}

	if err := w.WriteField("channels", channel); err != nil {
		return nil, nil, fmt.Errorf("failed to write channels to form: %w", err)
	}

	if err := w.WriteField("initial_comment", comment); err != nil {
		return nil, nil, fmt.Errorf("failed to write initial_comment to form: %w", err)
	}

	if err := w.WriteField("thread_ts", threadTs); err != nil {
		return nil, nil, fmt.Errorf("failed to write thread_ts to form: %w", err)
	}

	if err := w.Close(); err != nil {
		return nil, nil, fmt.Errorf("failed to close multipart writer: %w", err)
	}

	b := buf.Bytes()
	headers := http.Header{}
	headers.Set("Content-Type", w.FormDataContentType())
	return headers, b, nil
}

func (sn *Notifier) sendMultipart(ctx context.Context, headers http.Header, data io.Reader) error {
	sn.log.Debug("Sending multipart request to files.upload")

	u, err := uploadURL(sn.settings)
	if err != nil {
		return fmt.Errorf("failed to get URL for files.upload: %w", err)
	}

	req, err := http.NewRequest(http.MethodPost, u, data)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	for k, v := range headers {
		req.Header[k] = v
	}
	req.Header.Set("Authorization", "Bearer "+sn.settings.Token)

	if _, err := sn.sendFn(ctx, req, sn.log); err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}

	return nil
}

// uploadImage shares the image to the channel names or IDs. It returns an error if the file
// does not exist, or if there was an error either preparing or sending the multipart/form-data
// request.
func (sn *Notifier) uploadImage(ctx context.Context, image images.Image, channel, comment, threadTs string) error {
	sn.log.Debug("Uploadimg image", "image", image.Token)
	headers, data, err := sn.createImageMultipart(image, channel, comment, threadTs)
	if err != nil {
		return fmt.Errorf("failed to create multipart form: %w", err)
	}

	return sn.sendMultipart(ctx, headers, bytes.NewReader(data))
}

func (sn *Notifier) SendResolved() bool {
	return !sn.GetDisableResolveMessage()
}

// initialCommentForImage returns the initial comment for the image.
// Here is an example of the initial comment for an alert called
// AlertName with two labels:
//
//	Resolved|Firing: AlertName, Labels: A=B, C=D
//
// where Resolved|Firing and Labels is in bold text.
func initialCommentForImage(alert *types.Alert) string {
	sb := strings.Builder{}

	if alert.Resolved() {
		sb.WriteString("*Resolved*:")
	} else {
		sb.WriteString("*Firing*:")
	}

	sb.WriteString(" ")
	sb.WriteString(alert.Name())
	sb.WriteString(", ")

	sb.WriteString("*Labels*: ")

	var n int
	for k, v := range alert.Labels {
		sb.WriteString(string(k))
		sb.WriteString(" = ")
		sb.WriteString(string(v))
		if n < len(alert.Labels)-1 {
			sb.WriteString(", ")
			n++
		}
	}

	return sb.String()
}
