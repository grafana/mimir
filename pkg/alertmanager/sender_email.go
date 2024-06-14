package alertmanager

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"html/template"
	"io"
	"net"
	"net/mail"
	"os/user"
	"strconv"
	"strings"

	alertingReceivers "github.com/grafana/alerting/receivers"
	"github.com/grafana/mimir/pkg/util/version"
	"github.com/pkg/errors"
	gomail "gopkg.in/mail.v2"
)

var ErrInvalidEmailCode = errors.New("invalid or expired email code")

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
	Body          map[string]string
	Info          string
	ReplyTo       []string
	EmbeddedFiles []string
	AttachedFiles []*AttachedFile
}

// SendEmail implements alertingReceivers.EmailSender.
func (s *Sender) SendEmail(ctx context.Context, cmd *alertingReceivers.SendEmailSettings) error {
	fmt.Println("SendEmail() called!")
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

func (s *Sender) SendEmailCommandHandlerSync(ctx context.Context, cmd *SendEmailCommand) error {
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

func (s *Sender) buildEmailMessage(cmd *SendEmailCommand) (*Message, error) {
	data := cmd.Data
	if data == nil {
		data = make(map[string]any, 10)
	}

	setDefaultTemplateData(s.externalURL, data, nil)

	body := make(map[string]string)
	// TODO: what?
	// for _, contentType := range ns.Cfg.Smtp.ContentTypes {
	// 	fileExtension, err := getFileExtensionByContentType(contentType)
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// 	var buffer bytes.Buffer
	// 	err = mailTemplates.ExecuteTemplate(&buffer, cmd.Template+fileExtension, data)
	// 	if err != nil {
	// 		return nil, err
	// 	}

	// 	body[contentType] = buffer.String()
	// }

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

	addr := mail.Address{Name: s.fromName, Address: s.fromAddress}
	return &Message{
		To:            cmd.To,
		SingleEmail:   cmd.SingleEmail,
		From:          addr.String(),
		Subject:       subject,
		Body:          body,
		EmbeddedFiles: cmd.EmbeddedFiles,
		AttachedFiles: cmd.AttachedFiles,
		ReplyTo:       cmd.ReplyTo,
	}, nil
}

func setDefaultTemplateData(externalURL string, data map[string]any, u *user.User) {
	data["AppUrl"] = externalURL
	data["BuildVersion"] = version.Version
	// TODO: what?
	// data["EmailCodeValidHours"] = cfg.EmailCodeValidMinutes / 60
	data["Subject"] = map[string]any{}
	// TODO: what?
	// if u != nil {
	// 	data["Name"] = u.NameOrFallback()
	// }
	dataCopy := map[string]any{}
	for k, v := range data {
		dataCopy[k] = v
	}
	data["TemplateData"] = dataCopy
}

func (s *Sender) Send(ctx context.Context, messages ...*Message) (int, error) {
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

func (s *Sender) createDialer() (*gomail.Dialer, error) {
	host, port, err := net.SplitHostPort(s.host)
	if err != nil {
		return nil, err
	}
	iPort, err := strconv.Atoi(port)
	if err != nil {
		return nil, err
	}

	tlsconfig := &tls.Config{
		InsecureSkipVerify: s.skipVerify,
		ServerName:         host,
	}

	if s.certFile != "" {
		cert, err := tls.LoadX509KeyPair(s.certFile, s.keyFile)
		if err != nil {
			return nil, fmt.Errorf("could not load cert or key file: %w", err)
		}
		tlsconfig.Certificates = []tls.Certificate{cert}
	}

	d := gomail.NewDialer(host, iPort, s.user, s.password)
	d.TLSConfig = tlsconfig
	d.StartTLSPolicy = getStartTLSPolicy(s.startTLSPolicy)
	d.LocalName = s.ehloIdentity

	return d, nil
}

// buildEmail converts the Message DTO to a gomail message.
func (s *Sender) buildEmail(ctx context.Context, msg *Message) *gomail.Message {
	m := gomail.NewMessage()
	// add all static headers to the email message
	for h, val := range s.staticHeaders {
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
	// loop over content types from settings in reverse order as they are ordered in according to descending
	// preference while the alternatives should be ordered according to ascending preference
	for i := len(s.contentTypes) - 1; i >= 0; i-- {
		if i == len(s.contentTypes)-1 {
			m.SetBody(s.contentTypes[i], msg.Body[s.contentTypes[i]])
		} else {
			m.AddAlternative(s.contentTypes[i], msg.Body[s.contentTypes[i]])
		}
	}

	return m
}

func getStartTLSPolicy(policy string) gomail.StartTLSPolicy {
	switch policy {
	case "NoStartTLS":
		return -1
	case "MandatoryStartTLS":
		return 1
	default:
		return 0
	}
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
