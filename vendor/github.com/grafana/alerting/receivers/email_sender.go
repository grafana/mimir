package receivers

import (
	"bytes"
	"context"
	"crypto/tls"
	_ "embed"
	"fmt"
	"html/template"
	"io"
	"net"
	"net/mail"
	"strconv"
	"strings"

	"github.com/Masterminds/sprig/v3"
	gomail "gopkg.in/mail.v2"
)

var (
	//go:embed templates/ng_alert_notification.html
	defaultEmailTemplate string
)

type EmailSenderConfig struct {
	AuthPassword  string
	AuthUser      string
	CertFile      string
	EhloIdentity  string
	ExternalURL   string
	FromName      string
	FromAddress   string
	Host          string
	KeyFile       string
	SkipVerify    bool
	StaticHeaders map[string]string
	Version       string
}

type defaultEmailSender struct {
	cfg  EmailSenderConfig
	tmpl *template.Template
}

// NewEmailSenderFactory takes a configuration and returns a new EmailSender factory function.
func NewEmailSenderFactory(cfg EmailSenderConfig) func(Metadata) (EmailSender, error) {
	return func(n Metadata) (EmailSender, error) {
		tmpl, err := template.New("ng_alert_notification").
			Funcs(template.FuncMap{
				"Subject":                 subjectTemplateFunc,
				"__dangerouslyInjectHTML": __dangerouslyInjectHTML,
			}).
			Funcs(sprig.FuncMap()).
			Parse(defaultEmailTemplate)
		if err != nil {
			return nil, err
		}

		return &defaultEmailSender{
			cfg:  cfg,
			tmpl: tmpl,
		}, nil
	}
}

// Message representats an email message.
type Message struct {
	To            []string
	SingleEmail   bool
	From          string
	Subject       string
	Body          string
	Info          string
	ReplyTo       []string
	EmbeddedFiles []string
	AttachedFiles []*SendEmailAttachedFile
}

// SendEmail implements the EmailSender interface.
func (s *defaultEmailSender) SendEmail(_ context.Context, cmd *SendEmailSettings) error {
	message, err := s.buildEmailMessage(cmd)
	if err != nil {
		return err
	}

	_, err = s.Send(message)
	return err
}

func (s *defaultEmailSender) buildEmailMessage(cmd *SendEmailSettings) (*Message, error) {
	data := cmd.Data
	if data == nil {
		data = make(map[string]any, 10)
	}

	s.setDefaultTemplateData(data)

	var buffer bytes.Buffer
	if err := s.tmpl.ExecuteTemplate(&buffer, cmd.Template, data); err != nil {
		return nil, err
	}

	subject := cmd.Subject
	if cmd.Subject == "" {
		subjectData := data["Subject"].(map[string]any)
		subjectText, hasSubject := subjectData["executed_template"].(string)
		if !hasSubject {
			return nil, fmt.Errorf("missing subject in template %s", cmd.Template)
		}
		subject = subjectText
	}

	addr := mail.Address{Name: s.cfg.FromName, Address: s.cfg.FromAddress}
	return &Message{
		To:            cmd.To,
		SingleEmail:   cmd.SingleEmail,
		From:          addr.String(),
		Subject:       subject,
		Body:          buffer.String(),
		EmbeddedFiles: cmd.EmbeddedFiles,
		AttachedFiles: cmd.AttachedFiles,
		ReplyTo:       cmd.ReplyTo,
	}, nil
}

func (s *defaultEmailSender) setDefaultTemplateData(data map[string]any) {
	data["AppUrl"] = s.cfg.ExternalURL
	data["BuildVersion"] = s.cfg.Version
	data["Subject"] = map[string]any{}
	dataCopy := map[string]any{}
	for k, v := range data {
		dataCopy[k] = v
	}
	data["TemplateData"] = dataCopy
}

func (s *defaultEmailSender) Send(messages ...*Message) (int, error) {
	sentEmailsCount := 0
	dialer, err := s.createDialer()
	if err != nil {
		return sentEmailsCount, err
	}

	for _, msg := range messages {
		m := s.buildEmail(msg)

		innerError := dialer.DialAndSend(m)
		if innerError != nil {
			err = fmt.Errorf("failed to send notification to email addresses: %s: %w", strings.Join(msg.To, ";"), innerError)
			continue
		}

		sentEmailsCount++
	}

	return sentEmailsCount, err
}

func (s *defaultEmailSender) createDialer() (*gomail.Dialer, error) {
	host, port, err := net.SplitHostPort(s.cfg.Host)
	if err != nil {
		return nil, err
	}
	iPort, err := strconv.Atoi(port)
	if err != nil {
		return nil, err
	}

	tlsconfig := &tls.Config{
		InsecureSkipVerify: s.cfg.SkipVerify,
		ServerName:         host,
	}

	if s.cfg.CertFile != "" {
		cert, err := tls.LoadX509KeyPair(s.cfg.CertFile, s.cfg.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("could not load cert or key file: %w", err)
		}
		tlsconfig.Certificates = []tls.Certificate{cert}
	}

	d := gomail.NewDialer(host, iPort, s.cfg.AuthUser, s.cfg.AuthPassword)
	d.TLSConfig = tlsconfig
	d.LocalName = s.cfg.EhloIdentity

	return d, nil
}

// buildEmail converts the Message DTO to a gomail message.
func (s *defaultEmailSender) buildEmail(msg *Message) *gomail.Message {
	m := gomail.NewMessage()
	// Add all static headers to the email message.
	for h, val := range s.cfg.StaticHeaders {
		m.SetHeader(h, val)
	}
	m.SetHeader("From", msg.From)
	m.SetHeader("To", msg.To...)
	m.SetHeader("Subject", msg.Subject)

	setFiles(m, msg)
	replyTo := make([]string, 0, len(msg.ReplyTo))
	for _, address := range msg.ReplyTo {
		replyTo = append(replyTo, m.FormatAddress(address, ""))
	}
	m.SetHeader("Reply-To", strings.Join(replyTo, ", "))
	m.SetBody("text/html", msg.Body)

	return m
}

// setFiles attaches files in various forms.
func setFiles(
	m *gomail.Message,
	msg *Message,
) {
	for _, file := range msg.EmbeddedFiles {
		m.Embed(file)
	}

	for _, file := range msg.AttachedFiles {
		file := file
		m.Attach(file.Name, gomail.SetCopyFunc(func(writer io.Writer) error {
			_, err := writer.Write(file.Content)
			return err
		}))
	}
}

// subjectTemplateFunc does the same thing has hiddenSubjectTemplateFunc, but in addition it executes and returns the subject template using the data represented in `.TemplateData` (data)
// This results in the template being replaced by the subject string.
func subjectTemplateFunc(obj map[string]any, data map[string]any, value string) string {
	obj["value"] = value

	titleTmpl, err := template.New("title").Parse(value)
	if err != nil {
		return ""
	}

	var buf bytes.Buffer
	err = titleTmpl.ExecuteTemplate(&buf, "title", data)
	if err != nil {
		return ""
	}

	subj := buf.String()
	// Since we have already executed the template, save it to subject data so we don't have to do it again later on
	obj["executed_template"] = subj
	return subj
}

// __dangerouslyInjectHTML allows marking areas of am email template as HTML safe, this will _not_ sanitize the string and will allow HTML snippets to be rendered verbatim.
// Use with absolute care as this _could_ allow for XSS attacks when used in an insecure context.
//
// It's safe to ignore gosec warning G203 when calling this function in an HTML template because we assume anyone who has write access
// to the email templates folder is an administrator.
//
// nolint:gosec,revive
func __dangerouslyInjectHTML(s string) template.HTML {
	return template.HTML(s)
}
