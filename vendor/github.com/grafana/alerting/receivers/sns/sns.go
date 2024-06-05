package sns

import (
	"context"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/prometheus/alertmanager/notify"
	"github.com/prometheus/alertmanager/types"

	"github.com/grafana/alerting/logging"
	"github.com/grafana/alerting/receivers"
	"github.com/grafana/alerting/templates"
)

// Notifier is responsible for sending
// alert notifications to Amazon SNS.
type Notifier struct {
	*receivers.Base
	log          logging.Logger
	tmpl         *templates.Template
	settings     Config
	sessionCache *SessionCache
}

func New(cfg Config, meta receivers.Metadata, template *templates.Template, logger logging.Logger) *Notifier {
	return &Notifier{
		Base:         receivers.NewBase(meta),
		log:          logger,
		tmpl:         template,
		settings:     cfg,
		sessionCache: NewSessionCache(),
	}
}

// Notify sends the alert notification to sns.
func (s *Notifier) Notify(ctx context.Context, as ...*types.Alert) (bool, error) {
	s.log.Info("sending SNS")

	awsSessionConfig := &SessionConfig{
		Settings: s.settings.AWSAuthSettings,
	}

	session, err := s.sessionCache.GetSession(*awsSessionConfig)
	if err != nil {
		return false, err
	}

	publishInput, err := s.createPublishInput(ctx, as...)
	if err != nil {
		return false, err
	}

	snsClient := sns.New(session, aws.NewConfig().WithEndpoint(*aws.String(s.settings.APIUrl)))
	publishOutput, err := snsClient.Publish(publishInput)
	if err != nil {
		s.log.Error("Failed to publish to Amazon SNS. ", "error", err)
	}

	s.log.Debug("SNS message successfully published", "messageId", publishOutput.MessageId, "sequenceNumber", publishOutput.SequenceNumber)
	return true, nil
}

func (s *Notifier) SendResolved() bool {
	return !s.GetDisableResolveMessage()
}

func (s *Notifier) createPublishInput(ctx context.Context, as ...*types.Alert) (*sns.PublishInput, error) {
	var err error
	tmpl, _ := templates.TmplText(ctx, s.tmpl, as, s.log, &err)
	if err != nil {
		return nil, fmt.Errorf("failed to create template: %w", err)
	}

	publishInput := &sns.PublishInput{}
	messageAttributes := s.createMessageAttributes(tmpl)
	// Max message size for a message in an SNS publish request is 256KB, except for SMS messages where the limit is 1600 characters/runes.
	messageSizeLimit := 256 * 1024
	if s.settings.TopicARN != "" {
		topicARN := tmpl(s.settings.TopicARN)
		publishInput.SetTopicArn(topicARN)
		// If we are using a topic ARN, it could be a FIFO topic specified by the topic's suffix ".fifo".
		if strings.HasSuffix(topicARN, ".fifo") {
			// Deduplication key and Message Group ID are only added if it's a FIFO SNS Topic.
			key, err := notify.ExtractGroupKey(ctx)
			if err != nil {
				return nil, err
			}
			publishInput.SetMessageDeduplicationId(key.Hash())
			publishInput.SetMessageGroupId(key.Hash())
		}
	}

	if s.settings.PhoneNumber != "" {
		publishInput.SetPhoneNumber(tmpl(s.settings.PhoneNumber))
		// If we have an SMS message, we need to truncate to 1600 characters/runes.
		messageSizeLimit = 1600
	}
	if s.settings.TargetARN != "" {
		publishInput.SetTargetArn(tmpl(s.settings.TargetARN))
	}

	messageToSend, isTrunc, err := validateAndTruncateMessage(tmpl(s.settings.Message), messageSizeLimit)
	if err != nil {
		return nil, err
	}
	if isTrunc {
		// If we truncated the message we need to add a message attribute showing that it was truncated.
		messageAttributes["truncated"] = &sns.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String("true")}
	}

	publishInput.SetMessage(messageToSend)
	publishInput.SetMessageAttributes(messageAttributes)

	if s.settings.Subject != "" {
		publishInput.SetSubject(tmpl(s.settings.Subject))
	}

	return publishInput, nil
}

func (s *Notifier) createMessageAttributes(tmpl func(string) string) map[string]*sns.MessageAttributeValue {
	// Convert the given attributes map into the AWS Message Attributes Format.
	attributes := make(map[string]*sns.MessageAttributeValue, len(s.settings.Attributes))
	for k, v := range s.settings.Attributes {
		attributes[tmpl(k)] = &sns.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String(tmpl(v))}
	}
	return attributes
}

func validateAndTruncateMessage(message string, maxMessageSizeInBytes int) (string, bool, error) {
	if !utf8.ValidString(message) {
		return "", false, fmt.Errorf("non utf8 encoded message string")
	}
	if len(message) <= maxMessageSizeInBytes {
		return message, false, nil
	}
	// If the message is larger than our specified size we have to truncate.
	truncated := make([]byte, maxMessageSizeInBytes)
	copy(truncated, message)
	return string(truncated), true, nil
}
