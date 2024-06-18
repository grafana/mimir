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
	"github.com/grafana/alerting/templates"
	gomail "gopkg.in/mail.v2"
)

type defaultEmailSender struct {
	cfg  EmailSenderConfig
	tmpl *template.Template
}

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

//go:embed templates/ng_alert_notification.html
var defaultEmailTemplate string

// NewEmailSenderFactory takes a configuration and returns a new EmailSender factory function.
func NewEmailSenderFactory(cfg EmailSenderConfig) func(n Metadata) (EmailSender, error) {
	return func(n Metadata) (EmailSender, error) {
		tmpl, err := template.New("ng_alert_notification").
			Funcs(template.FuncMap{
				"Subject":                 subjectTemplateFunc,
				"HiddenSubject":           hiddenSubjectTemplateFunc,
				"__dangerouslyInjectHTML": __dangerouslyInjectHTML,
			}).
			Funcs(template.FuncMap(templates.DefaultFuncs)).
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

// AttachedFile is a definition of the attached files without path
type AttachedFile struct {
	Name    string
	Content []byte
}

// SendEmailCommand is the command for sending emails
type SendEmailCommand struct {
	To            []string
	SingleEmail   bool
	Template      string
	Subject       string
	Data          map[string]any
	Info          string
	ReplyTo       []string
	EmbeddedFiles []string
	AttachedFiles []*AttachedFile
}

// Message is representation of the email message.
type Message struct {
	To            []string
	SingleEmail   bool
	From          string
	Subject       string
	Body          string
	Info          string
	ReplyTo       []string
	EmbeddedFiles []string
	AttachedFiles []*AttachedFile
}

// SendEmail implements alertingReceivers.EmailSender.
func (s *defaultEmailSender) SendEmail(ctx context.Context, cmd *SendEmailSettings) error {
	var attached []*AttachedFile
	if cmd.AttachedFiles != nil {
		attached = make([]*AttachedFile, 0, len(cmd.AttachedFiles))
		for _, file := range cmd.AttachedFiles {
			attached = append(attached, &AttachedFile{
				Name:    file.Name,
				Content: file.Content,
			})
		}
	}

	return s.SendEmailCommandHandlerSync(ctx, &SendEmailCommand{
		To:            cmd.To,
		SingleEmail:   cmd.SingleEmail,
		Template:      cmd.Template,
		Subject:       cmd.Subject,
		Data:          cmd.Data,
		Info:          cmd.Info,
		ReplyTo:       cmd.ReplyTo,
		EmbeddedFiles: cmd.EmbeddedFiles,
		AttachedFiles: attached,
	})
}

func (s *defaultEmailSender) SendEmailCommandHandlerSync(ctx context.Context, cmd *SendEmailCommand) error {
	message, err := s.buildEmailMessage(&SendEmailCommand{
		Data:          cmd.Data,
		Info:          cmd.Info,
		Template:      cmd.Template,
		To:            cmd.To,
		SingleEmail:   cmd.SingleEmail,
		EmbeddedFiles: cmd.EmbeddedFiles,
		AttachedFiles: cmd.AttachedFiles,
		Subject:       cmd.Subject,
		ReplyTo:       cmd.ReplyTo,
	})

	if err != nil {
		return err
	}

	_, err = s.Send(ctx, message)
	return err
}

func (s *defaultEmailSender) buildEmailMessage(cmd *SendEmailCommand) (*Message, error) {
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
		if hasSubject {
			// first check to see if the template has already been executed in a template func
			subject = subjectText
		} else {
			subjectTemplate, hasSubject := subjectData["value"]

			if !hasSubject {
				return nil, fmt.Errorf("missing subject in template %s", cmd.Template)
			}

			subjectTmpl, err := template.New("subject").Parse(subjectTemplate.(string))
			if err != nil {
				return nil, err
			}

			var subjectBuffer bytes.Buffer
			err = subjectTmpl.ExecuteTemplate(&subjectBuffer, "subject", data)
			if err != nil {
				return nil, err
			}

			subject = subjectBuffer.String()
		}
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

func (s *defaultEmailSender) Send(ctx context.Context, messages ...*Message) (int, error) {
	// TODO: add
	// ctx, span := tracer.Start(ctx, "notifications.SmtpClient.Send",
	// 	trace.WithAttributes(attribute.Int("messages", len(messages))),
	// )
	// defer span.End()

	sentEmailsCount := 0
	dialer, err := s.createDialer()
	if err != nil {
		return sentEmailsCount, err
	}

	for _, msg := range messages {
		// span.SetAttributes(
		// 	attribute.String("smtp.sender", msg.From),
		// 	attribute.StringSlice("smtp.recipients", msg.To),
		// )

		m := s.buildEmail(ctx, msg)

		innerError := dialer.DialAndSend(m)
		// emailsSentTotal.Inc()
		if innerError != nil {
			// As gomail does not return typed errors we have to parse the error
			// to catch invalid error when the address is invalid.
			// https://github.com/go-gomail/gomail/blob/81ebce5c23dfd25c6c67194b37d3dd3f338c98b1/send.go#L113
			if !strings.HasPrefix(innerError.Error(), "gomail: invalid address") {
				// emailsSentFailed.Inc()
			}

			err = fmt.Errorf("failed to send notification to email addresses: %s: %w", strings.Join(msg.To, ";"), innerError)
			// span.RecordError(err)
			// span.SetStatus(codes.Error, err.Error())

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
func (s *defaultEmailSender) buildEmail(ctx context.Context, msg *Message) *gomail.Message {
	m := gomail.NewMessage()
	// add all static headers to the email message
	for h, val := range s.cfg.StaticHeaders {
		m.SetHeader(h, val)
	}
	m.SetHeader("From", msg.From)
	m.SetHeader("To", msg.To...)
	m.SetHeader("Subject", msg.Subject)

	// if s.enableTracing {
	// 	otel.GetTextMapPropagator().Inject(ctx, gomailHeaderCarrier{m})
	// }

	setFiles(m, msg)
	for _, replyTo := range msg.ReplyTo {
		m.SetAddressHeader("Reply-To", replyTo, "")
	}
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

// hiddenSubjectTemplateFunc sets the subject template (value) on the map represented by `.Subject.` (obj) so that it can be compiled and executed later.
// It returns a blank string, so there will be no resulting value left in place of the template.
func hiddenSubjectTemplateFunc(obj map[string]any, value string) string {
	obj["value"] = value
	return ""
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
// nolint:gosec
func __dangerouslyInjectHTML(s string) template.HTML {
	return template.HTML(s)
}
