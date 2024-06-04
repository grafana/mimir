package sns

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go/aws/arn"
	"github.com/grafana/grafana-aws-sdk/pkg/awsds"

	"github.com/grafana/alerting/receivers"
	"github.com/grafana/alerting/templates"
)

type SigV4Config struct {
	Region    string `json:"region,omitempty" yaml:"region,omitempty"`
	AccessKey string `json:"access_key,omitempty" yaml:"access_key,omitempty"`
	SecretKey string `json:"secret_key,omitempty" yaml:"secret_key,omitempty"`
	Profile   string `json:"profile,omitempty" yaml:"profile,omitempty"`
	RoleARN   string `json:"role_arn,omitempty" yaml:"role_arn,omitempty"`
}

type Config struct {
	APIUrl          string
	TopicARN        string
	PhoneNumber     string
	TargetARN       string
	Subject         string
	Message         string
	Attributes      map[string]string
	AWSAuthSettings awsds.AWSDatasourceSettings
}

func NewConfig(jsonData json.RawMessage, decryptFn receivers.DecryptFunc) (Config, error) {
	type snsSettingsRaw struct {
		APIUrl      string            `yaml:"api_url,omitempty" json:"api_url,omitempty"`
		Sigv4       SigV4Config       `yaml:"sigv4" json:"sigv4"`
		TopicARN    string            `yaml:"topic_arn,omitempty" json:"topic_arn,omitempty"`
		PhoneNumber string            `yaml:"phone_number,omitempty" json:"phone_number,omitempty"`
		TargetARN   string            `yaml:"target_arn,omitempty" json:"target_arn,omitempty"`
		Subject     string            `yaml:"subject,omitempty" json:"subject,omitempty"`
		Message     string            `yaml:"message,omitempty" json:"message,omitempty"`
		Attributes  map[string]string `yaml:"attributes,omitempty" json:"attributes,omitempty"`
	}
	var settings snsSettingsRaw
	err := json.Unmarshal(jsonData, &settings)
	if err != nil {
		return Config{}, fmt.Errorf("failed to unmarshal settings: %w", err)
	}

	if settings.TopicARN != "" {
		_, err = arn.Parse(settings.TopicARN)
		if err != nil {
			return Config{}, errors.New("invalid topic ARN provided")
		}
	}

	if settings.TargetARN != "" {
		_, err = arn.Parse(settings.TopicARN)
		if err != nil {
			return Config{}, errors.New("invalid target ARN provided")
		}
	}

	if settings.TopicARN == "" && settings.TargetARN == "" && settings.PhoneNumber == "" {
		return Config{}, errors.New("must specify topicArn, targetArn, or phone number")
	}
	if settings.Subject == "" {
		settings.Subject = templates.DefaultMessageTitleEmbed
	}
	if settings.Message == "" {
		settings.Message = templates.DefaultMessageEmbed
	}
	if settings.APIUrl == "" {
		settings.APIUrl = fmt.Sprintf("https://sns.%s.amazonaws.com", settings.Sigv4.Region)
	}

	at := awsds.AuthTypeDefault
	if settings.Sigv4.Profile != "" {
		at = awsds.AuthTypeSharedCreds
	} else if settings.Sigv4.AccessKey != "" || settings.Sigv4.SecretKey != "" {
		if settings.Sigv4.AccessKey == "" || settings.Sigv4.SecretKey == "" {
			return Config{}, errors.New("must specify both access key and secret key")
		}
		settings.Sigv4.AccessKey = decryptFn("accessKey", settings.Sigv4.AccessKey)
		settings.Sigv4.SecretKey = decryptFn("secretKey", settings.Sigv4.SecretKey)
		at = awsds.AuthTypeKeys
	}

	return Config{
		APIUrl:      settings.APIUrl,
		TopicARN:    settings.TopicARN,
		PhoneNumber: settings.PhoneNumber,
		TargetARN:   settings.TargetARN,
		Subject:     settings.Subject,
		Message:     settings.Message,
		Attributes:  settings.Attributes,
		AWSAuthSettings: awsds.AWSDatasourceSettings{
			Profile:       settings.Sigv4.Profile,
			Region:        settings.Sigv4.Region,
			AuthType:      at,
			AssumeRoleARN: settings.Sigv4.RoleARN,
			AccessKey:     settings.Sigv4.AccessKey,
			SecretKey:     settings.Sigv4.SecretKey,
		},
	}, nil
}
