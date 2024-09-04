package testcontainer

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/network"
	"github.com/go-playground/validator/v10"
	"github.com/lithammer/shortuuid/v4"
	"github.com/testcontainers/testcontainers-go"
)

var (
	// KafkaBrokerImage - docker image name for apache kafka broker
	KafkaBrokerImage = "confluentinc/cp-server:7.5.0"

	// BrokerExternalPort - broker port for external communications
	BrokerExternalPort = "9092"
	// BrokerInternalPort - broker port for internal communications
	BrokerInternalPort = "29092"
)

var (
	ErrNoBrokerAvailable     = errors.New("no broker available")
	ErrConfigValidationError = errors.New("config validation error")
	ErrNoDockerClient        = errors.New("no docker client available")
)

// KafkaContainerConfig - config struct for container with kafka broker
type KafkaContainerConfig struct {
	// Timeout. After expiration context will be canceled
	Timeout time.Duration `validate:"required"`
	// Network. Prefix for network name
	Network string `validate:"required"`
	// Kafka. Prefix for kafka container name
	Kafka string

	// Waiting. Time period for kafka launch waiting for
	Waiting time.Duration
}

// Validate - validate config struct
func (c *KafkaContainerConfig) Validate() error {
	if c.Network == "" {
		c.Network = "Net4Test"
	}

	if c.Kafka == "" {
		c.Kafka = "KafkaBroker"
	}

	if c.Waiting == 0 {
		c.Waiting = time.Minute * 3
	}
	return validator.New().Struct(c)
}

// KafkaContainer - test container for kafka broker
type KafkaContainer struct {
	logger       Logger
	broker       testcontainers.Container
	cfg          KafkaContainerConfig
	networkID    string
	sessionID    string
	networkName  string
	dockerClient *testcontainers.DockerClient
	brokerPort   string
}

// NewKafkaContainer - returns new KafkaContainer
func NewKafkaContainer(ctx context.Context, cfg KafkaContainerConfig, logger Logger) (*KafkaContainer, error) {
	var target KafkaContainer

	if logger != nil {
		target.logger = logger
	} else {
		target.logger = newLogger()
	}

	testcontainers.Logger = &containerLogger{log: target.logger}

	if err := cfg.Validate(); err != nil {
		target.logger.LogError(ctx, "Config validation error", err)
		return nil, ErrConfigValidationError
	}
	target.cfg = cfg

	ctxWithTimeout, cancel := context.WithTimeout(ctx, cfg.Timeout)
	defer cancel()

	target.sessionID = target.generateID()

	err := target.initDockerClient()
	if err != nil {
		target.logger.LogError(ctx, "Can't init docker client", err)
		return nil, ErrNoDockerClient
	}

	err = target.initNetwork(ctxWithTimeout)
	if err != nil {
		target.logger.LogError(ctx, "Can't init network for containers", err)
		return nil, err
	}

	err = target.initKafkaBroker(ctx)
	if err != nil {
		target.logger.LogError(ctx, "Can't init kafka broker", err)
		return nil, err
	}

	return &target, nil
}

// GetBrokerList - return endpoint list for kafka connecting
func (target *KafkaContainer) GetBrokerList(ctx context.Context) ([]string, error) {
	if target.broker == nil {
		return nil, ErrNoBrokerAvailable
	}
	host, err := target.broker.Host(ctx)
	if err != nil {
		target.logger.LogError(ctx, "Can't get endpoint from kafka broker container", err)
		return nil, ErrNoBrokerAvailable
	}
	endpoint := fmt.Sprintf("%s:%s", host, target.brokerPort)
	return []string{endpoint}, nil
}

// Close - destruct all run containers
func (target *KafkaContainer) Close(ctx context.Context) {
	var err error
	if target.broker != nil {
		err = target.broker.Terminate(ctx)
		if err != nil {
			target.logger.LogError(ctx, "Error while broker termination", err)
		}
	}

	err = target.dockerClient.NetworkRemove(ctx, target.networkID)
	if err != nil {
		target.logger.LogError(ctx, "Error while network termination", err)
	}
}

func (target *KafkaContainer) initNetwork(ctx context.Context) error {
	target.logger.LogDebug(ctx, "Try to create network")
	if target.dockerClient == nil {
		target.logger.LogError(ctx, "Docker client not initialized", nil)
		return ErrNoDockerClient
	}

	networkName := target.cfg.Network + target.sessionID
	target.logger.LogDebug(ctx, "Network name is "+networkName)
	netFilterArgs := filters.NewArgs()
	netFilterArgs.Add("name", networkName)
	target.logger.LogDebug(ctx, "Try to find an existing network")
	foundNet, err := target.dockerClient.NetworkList(ctx, network.ListOptions{Filters: netFilterArgs})
	if err != nil {
		target.logger.LogError(ctx, "Can't execute query with docker client", err)
		return err
	}
	if len(foundNet) == 0 {
		target.logger.LogDebug(ctx, "Network not found. Try to create. Network name is "+networkName)
		n, err := target.dockerClient.NetworkCreate(ctx, networkName, network.CreateOptions{})
		if err != nil {
			target.logger.LogError(ctx, "Can't execute create network", err)
			return err
		}
		target.networkID = n.ID
		target.networkName = networkName
		target.logger.LogDebug(ctx, fmt.Sprintf("Network created. Network name is %s, id is %s", networkName, n.ID))
		return nil
	}

	target.networkID = foundNet[0].ID
	target.networkName = networkName
	target.logger.LogDebug(ctx, fmt.Sprintf("Network found. Network name is %s, id is %s", networkName, foundNet[0].ID))

	return nil
}

// getFreePort - return free port
func (target *KafkaContainer) getFreePort() (int, error) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return 0, err
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, err
	}
	defer func() {
		_ = l.Close()
	}()
	listenerAddr, _ := l.Addr().(*net.TCPAddr)
	return listenerAddr.Port, nil
}

func (target *KafkaContainer) initKafkaBroker(ctx context.Context) error {
	target.logger.LogDebug(ctx, "Try to init kafka broker")
	brokerName := target.cfg.Kafka + target.sessionID

	port, err := target.getFreePort()
	if err != nil {
		target.logger.LogError(ctx, "Can't get free port", err)
		return err
	}
	target.brokerPort = strconv.Itoa(port)

	expose := fmt.Sprintf("%d:%s", port, BrokerExternalPort)
	controllerPort := 29093 // TODO: какой тут должен быть порт?
	advertisedListeners := fmt.Sprintf("INTERNAL://localhost:%s,EXTERNAL://localhost:%d", BrokerInternalPort, port)
	kafkaListeners := fmt.Sprintf("INTERNAL://:%s,EXTERNAL://0.0.0.0:%s,CONTROLLER://0.0.0.0:%d, ", BrokerInternalPort, BrokerExternalPort, controllerPort)
	req := testcontainers.ContainerRequest{
		Name:     brokerName,
		Networks: []string{target.networkName},
		Image:    KafkaBrokerImage,
		ExposedPorts: []string{
			expose,
		},
		// AutoRemove: true,
		Env: map[string]string{
			"KAFKA_BROKER_ID":                                   "1",
			"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP":              "CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT",
			"KAFKA_ADVERTISED_LISTENERS":                        advertisedListeners,
			"KAFKA_LISTENERS":                                   kafkaListeners,
			"KAFKA_INTER_BROKER_LISTENER_NAME":                  "INTERNAL",
			"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR":            "1",
			"KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS":            "0",
			"KAFKA_CONFLUENT_LICENSE_TOPIC_REPLICATION_FACTOR":  "1",
			"KAFKA_CONFLUENT_BALANCER_TOPIC_REPLICATION_FACTOR": "1",
			"KAFKA_TRANSACTION_STATE_LOG_MIN_ISR":               "1",
			"KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR":    "1",
			"CONFLUENT_METRICS_ENABLE":                          "false",
			"KAFKA_PROCESS_ROLES":                               "broker,controller",
			"KAFKA_CONTROLLER_QUORUM_VOTERS":                    "1@localhost:29093",
			"KAFKA_CONTROLLER_LISTENER_NAMES":                   "CONTROLLER",
			"KAFKA_LOG_DIRS":                                    "/tmp/kraft-combined-logs",
			// CLUSTER_ID should be correct uuid
			"CLUSTER_ID": "MkU3OEVBNTcwNTJENDM2Qk",
		},
		WaitingFor: NewMetadataWaitStrategy(target.cfg.Waiting, target.logger),
	}
	target.logger.LogDebug(ctx, "Final broker configuration is:")
	target.logger.LogDebug(ctx, fmt.Sprint(req.Env))

	broker, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return err
	}

	target.broker = broker
	cInfo, err := broker.Inspect(ctx)
	if err != nil {
		target.logger.LogError(ctx, "can't inspect container", err)
		return err
	}

	target.logger.LogDebug(ctx, fmt.Sprintf("Kafka broker created. DokerID:%s, Name:%s", broker.GetContainerID(), cInfo.Name))

	return nil
}

func (target *KafkaContainer) initDockerClient() error {
	cli, err := testcontainers.NewDockerClientWithOpts(context.Background())
	target.dockerClient = cli
	if err != nil {
		return err
	}
	return nil
}

// cleanNetworks - removes docker networks if name match pattern
func (target *KafkaContainer) cleanNetworks(ctx context.Context) { //nolint:unused
	foundNets, err := target.dockerClient.NetworkList(ctx, network.ListOptions{})
	if err != nil {
		target.logger.LogError(ctx, "can't get network list", err)
		return
	}
	for _, n := range foundNets {
		if strings.Contains(n.Name, target.cfg.Network) {
			target.logger.LogDebug(ctx, fmt.Sprintf("try to remove network: Name is %s, ID is %s", n.Name, n.ID))
			err := target.dockerClient.NetworkRemove(ctx, n.ID)
			if err != nil {
				target.logger.LogError(ctx, fmt.Sprintf("can't remove network: Name is %s, ID is %s", n.Name, n.ID), err)
			}
		}
	}
}

func (target *KafkaContainer) generateID() string {
	return shortuuid.New()
}
