package e2edb

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/grafana/e2e"
	"github.com/grafana/e2e/images"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

type KafkaService struct {
	*e2e.HTTPService
	cfg KafkaConfig
}

func NewKafka() *KafkaService {
	return KafkaConfig{}.New()
}

type KafkaConfig struct {
	AuthMode    KafkaAuthMode
	DexEndpoint string
}

func (c KafkaConfig) ports() (clientPort, readinessPort int) {
	clientPort = 9092
	readinessPort = clientPort
	if c.AuthMode != KafkaAuthNone {
		// Use a separate PLAINTEXT listener for readiness probes since the main
		// listener requires authentication.
		readinessPort = 9093
	}
	return clientPort, readinessPort
}

func (c KafkaConfig) New() *KafkaService {
	clientPort, readinessPort := c.ports()
	var otherPorts []int
	if readinessPort != clientPort {
		otherPorts = []int{readinessPort}
	}

	return &KafkaService{
		HTTPService: e2e.NewHTTPService(
			"kafka",
			images.Kafka,
			nil, // No custom command.
			c.NewReadinessProbe(),
			clientPort,
			otherPorts...,
		),
		cfg: c,
	}
}

type KafkaAuthMode int

const (
	KafkaAuthNone KafkaAuthMode = iota
	KafkaAuthSASLPlain
	KafkaAuthSASLScramSHA256
	KafkaAuthSASLScramSHA512
	KafkaAuthSASLOAuthToken
	KafkaAuthSASLOAuthTokenFile
)

const (
	KafkaSASLUsername = "kafkauser"
	KafkaSASLPassword = "kafkapassword"

	scramIterations = 4096
)

func (s *KafkaService) Start(networkName, sharedDir string) (err error) {
	vars := map[string]string{
		// Configure Kafka to run in KRaft mode (without Zookeeper).
		"CLUSTER_ID":                      "NqnEdODVKkiLTfJvqd1uqQ==", // A random ID (16 bytes of a base64-encoded UUID).
		"KAFKA_BROKER_ID":                 "1",
		"KAFKA_NODE_ID":                   "1",
		"KAFKA_PROCESS_ROLES":             "broker,controller",
		"KAFKA_CONTROLLER_QUORUM_VOTERS":  "1@kafka:29093",
		"KAFKA_CONTROLLER_LISTENER_NAMES": "CONTROLLER",

		// RF=1.
		"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR":         "1",
		"KAFKA_TRANSACTION_STATE_LOG_MIN_ISR":            "1",
		"KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
		"KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS":         "0",

		// Enough partitions for integration tests.
		"KAFKA_NUM_PARTITIONS": "3",

		"LOG4J_ROOT_LOGLEVEL": "WARN",
	}

	switch s.cfg.AuthMode {
	case KafkaAuthNone:
		vars["KAFKA_LISTENERS"] = "PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:29093,PLAINTEXT_HOST://localhost:29092"
		vars["KAFKA_ADVERTISED_LISTENERS"] = fmt.Sprintf("PLAINTEXT://%s-kafka:9092,PLAINTEXT_HOST://localhost:29092", networkName)
		vars["KAFKA_LISTENER_SECURITY_PROTOCOL_MAP"] = "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT"
		vars["KAFKA_INTER_BROKER_LISTENER_NAME"] = "PLAINTEXT"

	case KafkaAuthSASLPlain:
		vars["KAFKA_LISTENERS"] = "SASLPLAIN://0.0.0.0:9092,CONTROLLER://0.0.0.0:29093,PLAINTEXT://0.0.0.0:9093"
		vars["KAFKA_ADVERTISED_LISTENERS"] = fmt.Sprintf("SASLPLAIN://%s-kafka:9092,PLAINTEXT://%s-kafka:9093", networkName, networkName)
		vars["KAFKA_LISTENER_SECURITY_PROTOCOL_MAP"] = "CONTROLLER:PLAINTEXT,SASLPLAIN:SASL_PLAINTEXT,PLAINTEXT:PLAINTEXT"
		vars["KAFKA_INTER_BROKER_LISTENER_NAME"] = "PLAINTEXT"
		vars["KAFKA_SASL_ENABLED_MECHANISMS"] = "PLAIN"
		vars["KAFKA_LISTENER_NAME_SASLPLAIN_PLAIN_SASL_JAAS_CONFIG"] = fmt.Sprintf(
			`org.apache.kafka.common.security.plain.PlainLoginModule required user_%s="%s";`,
			KafkaSASLUsername, KafkaSASLPassword,
		)

	case KafkaAuthSASLScramSHA256:
		vars["KAFKA_LISTENERS"] = "SASLSCRAM://0.0.0.0:9092,CONTROLLER://0.0.0.0:29093,PLAINTEXT://0.0.0.0:9093"
		vars["KAFKA_ADVERTISED_LISTENERS"] = fmt.Sprintf("SASLSCRAM://%s-kafka:9092,PLAINTEXT://%s-kafka:9093", networkName, networkName)
		vars["KAFKA_LISTENER_SECURITY_PROTOCOL_MAP"] = "CONTROLLER:PLAINTEXT,SASLSCRAM:SASL_PLAINTEXT,PLAINTEXT:PLAINTEXT"
		vars["KAFKA_INTER_BROKER_LISTENER_NAME"] = "PLAINTEXT"
		vars["KAFKA_SASL_ENABLED_MECHANISMS"] = "SCRAM-SHA-256"

	case KafkaAuthSASLScramSHA512:
		vars["KAFKA_LISTENERS"] = "SASLSCRAM://0.0.0.0:9092,CONTROLLER://0.0.0.0:29093,PLAINTEXT://0.0.0.0:9093"
		vars["KAFKA_ADVERTISED_LISTENERS"] = fmt.Sprintf("SASLSCRAM://%s-kafka:9092,PLAINTEXT://%s-kafka:9093", networkName, networkName)
		vars["KAFKA_LISTENER_SECURITY_PROTOCOL_MAP"] = "CONTROLLER:PLAINTEXT,SASLSCRAM:SASL_PLAINTEXT,PLAINTEXT:PLAINTEXT"
		vars["KAFKA_INTER_BROKER_LISTENER_NAME"] = "PLAINTEXT"
		vars["KAFKA_SASL_ENABLED_MECHANISMS"] = "SCRAM-SHA-512"

	case KafkaAuthSASLOAuthToken, KafkaAuthSASLOAuthTokenFile:
		vars["KAFKA_LISTENERS"] = "SASLOAUTH://0.0.0.0:9092,CONTROLLER://0.0.0.0:29093,PLAINTEXT://0.0.0.0:9093"
		vars["KAFKA_ADVERTISED_LISTENERS"] = fmt.Sprintf("SASLOAUTH://%s-kafka:9092,PLAINTEXT://%s-kafka:9093", networkName, networkName)
		vars["KAFKA_LISTENER_SECURITY_PROTOCOL_MAP"] = "CONTROLLER:PLAINTEXT,SASLOAUTH:SASL_PLAINTEXT,PLAINTEXT:PLAINTEXT"
		vars["KAFKA_INTER_BROKER_LISTENER_NAME"] = "PLAINTEXT"
		vars["KAFKA_SASL_ENABLED_MECHANISMS"] = "OAUTHBEARER"
		vars["KAFKA_LISTENER_NAME_SASLOAUTH_OAUTHBEARER_SASL_JAAS_CONFIG"] = "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;"
		vars["KAFKA_LISTENER_NAME_SASLOAUTH_OAUTHBEARER_SASL_SERVER_CALLBACK_HANDLER_CLASS"] = "org.apache.kafka.common.security.oauthbearer.OAuthBearerValidatorCallbackHandler"
		vars["KAFKA_LISTENER_NAME_SASLOAUTH_SASL_OAUTHBEARER_JWKS_ENDPOINT_URL"] = fmt.Sprintf("http://%s/dex/keys", s.cfg.DexEndpoint)
		vars["KAFKA_LISTENER_NAME_SASLOAUTH_SASL_OAUTHBEARER_EXPECTED_AUDIENCE"] = DexClientID
		// Kafka 4.0+ requires JWKS URLs to be explicitly allowed via this JVM system property.
		vars["KAFKA_OPTS"] = fmt.Sprintf("-Dorg.apache.kafka.sasl.oauthbearer.allowed.urls=http://%s/dex/keys", s.cfg.DexEndpoint)
	}

	// SCRAM config cannot be passed as env vars because it contains hyphens,
	// which the apache/kafka image can't handle.
	if s.cfg.AuthMode == KafkaAuthSASLScramSHA256 || s.cfg.AuthMode == KafkaAuthSASLScramSHA512 {
		jaasContent := fmt.Sprintf(
			"saslscram.KafkaServer {\n    org.apache.kafka.common.security.scram.ScramLoginModule required username=%q password=%q;\n};\n",
			KafkaSASLUsername, KafkaSASLPassword,
		)
		jaasPath := filepath.Join(sharedDir, "jaas.conf")
		if err := os.WriteFile(jaasPath, []byte(jaasContent), 0644); err != nil {
			return fmt.Errorf("writing kafka JAAS config: %w", err)
		}

		vars["KAFKA_OPTS"] = "-Djava.security.auth.login.config=" + filepath.Join(e2e.ContainerSharedDir, "jaas.conf")
	}

	// Configures Kafka right before starting it so that we have the networkName to correctly compute
	// the advertised host.
	s.SetEnvVars(vars)

	return s.HTTPService.Start(networkName, sharedDir)
}

// KafkaReadinessProbe checks readiness by ensure a Kafka broker is up and running.
type KafkaReadinessProbe struct {
	cfg KafkaConfig
}

func (c KafkaConfig) NewReadinessProbe() *KafkaReadinessProbe {
	return &KafkaReadinessProbe{cfg: c}
}

func (p *KafkaReadinessProbe) Ready(service *e2e.ConcreteService) (err error) {
	const timeout = 5 * time.Second

	_, readinessPort := p.cfg.ports()

	endpoint := service.Endpoint(readinessPort)
	switch endpoint {
	case "":
		return fmt.Errorf("cannot get service endpoint for port %d", readinessPort)
	case "stopped":
		return errors.New("service has stopped")
	}

	// Connect to the readiness port, which is always PLAINTEXT (no auth
	// required). For non-auth modes, the readiness port is the same as the
	// client port; for auth modes, it's a separate PLAINTEXT listener.
	opts := []kgo.Opt{kgo.SeedBrokers(endpoint), kgo.DialTimeout(timeout)}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return err
	}

	// Ensure we close the client once done.
	defer client.Close()

	admin := kadm.NewClient(client)

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	_, err = admin.ApiVersions(ctx)
	if err != nil {
		return err
	}

	if err := p.createSCRAMCredentials(service); err != nil {
		return fmt.Errorf("creating SCRAM credentials: %w", err)
	}

	return nil
}

// createSCRAMCredentials creates SCRAM user credentials by running
// kafka-configs.sh inside the Kafka container. It is a no-op for non-SCRAM
// auth modes.
//
// We use Exec rather than the admin client API because the readiness probe
// runs on the host, and the admin client's AlterUserSCRAMs request requires
// talking to the controller, whose address is discovered via metadata. The
// metadata returns the Docker-internal advertised address which is not
// resolvable from the host.
func (p *KafkaReadinessProbe) createSCRAMCredentials(service *e2e.ConcreteService) error {
	if p.cfg.AuthMode != KafkaAuthSASLScramSHA256 && p.cfg.AuthMode != KafkaAuthSASLScramSHA512 {
		return nil
	}

	var mechanism string
	switch p.cfg.AuthMode {
	case KafkaAuthSASLScramSHA256:
		mechanism = "SCRAM-SHA-256"
	case KafkaAuthSASLScramSHA512:
		mechanism = "SCRAM-SHA-512"
	default:
		return fmt.Errorf("unexpected auth mode: %v", p.cfg.AuthMode)
	}

	_, readinessPort := p.cfg.ports()

	stdout, stderr, err := service.Exec(e2e.NewCommandWithoutEntrypoint(
		"/opt/kafka/bin/kafka-configs.sh",
		fmt.Sprintf("--bootstrap-server=localhost:%d", readinessPort),
		"--alter",
		fmt.Sprintf("--add-config=%s=[iterations=%d,password=%s]", mechanism, scramIterations, KafkaSASLPassword),
		"--entity-type=users",
		fmt.Sprintf("--entity-name=%s", KafkaSASLUsername),
	))
	if err != nil {
		return fmt.Errorf("kafka-configs.sh failed: %w; stdout: %s; stderr: %s", err, stdout, stderr)
	}
	return nil
}
