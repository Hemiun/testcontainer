package testcontainer

import (
	"context"
	"fmt"
	"time"

	"github.com/Shopify/sarama"

	"github.com/testcontainers/testcontainers-go/wait"
)

// MetadataWaitStrategy - strategy for waiting kafka readiness
type MetadataWaitStrategy struct {
	logger Logger
	// all WaitStrategies should have a startupTimeout to avoid waiting infinitely
	startupTimeout time.Duration
}

// NewMetadataWaitStrategy  returns new MetadataWaitStrategy
func NewMetadataWaitStrategy(startupTimeout time.Duration, logger Logger) *MetadataWaitStrategy {
	var target MetadataWaitStrategy
	// set default preference
	target.startupTimeout = startupTimeout
	target.logger = logger
	return &target
}

// WaitUntilReady - strategy for waiting kafka readiness
func (mw *MetadataWaitStrategy) WaitUntilReady(ctx context.Context, target wait.StrategyTarget) error {
	// limit context to startupTimeout
	ctx, cancelContext := context.WithTimeout(ctx, mw.startupTimeout)
	defer cancelContext()

	waitInterval := 100 * time.Millisecond
	i := 0
	for {
		i++
		select {
		case <-ctx.Done():
			mw.logger.LogError(ctx, "context done", ctx.Err())
			return fmt.Errorf("context done:%s", ctx.Err())
		case <-time.After(waitInterval):
			port, err := target.MappedPort(ctx, "9092")
			if err != nil {
				mw.logger.LogDebug(ctx, fmt.Sprintf("(%d) [%s] %s\n", i, port, err))
				continue
			}

			host, err := target.Host(ctx)
			if err != nil {
				mw.logger.LogDebug(ctx, fmt.Sprintf("(%d) [%s] %s\n", i, host, err))
				continue
			}
			broker := host + ":" + port.Port()
			mw.logger.LogDebug(ctx, "broker="+broker)
			if mw.getKafkaMetadata(ctx, []string{broker}) {
				return nil
			}
		}
	}
}

// getKafkaMetadata - try get metadata from kafka. If successful then return true
func (mw *MetadataWaitStrategy) getKafkaMetadata(ctx context.Context, brokers []string) bool {
	config := sarama.NewConfig()
	prodClient, err := sarama.NewClient(brokers, config)
	if err != nil {
		mw.logger.LogDebug(ctx, "[TESTCONTAINER] Kafka client creation error:"+err.Error())
		return false
	}
	defer prodClient.Close() //nolint:errcheck
	b := prodClient.Brokers()
	if len(b) < 1 {
		mw.logger.LogDebug(ctx, "[TESTCONTAINER] Got empty broker list")
		return false
	}
	return true
}
