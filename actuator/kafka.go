package actuator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/segmentio/kafka-go"
	"kafka-actuator/client"
	"kafka-actuator/config/model"
	"kafka-actuator/logger"
	"strings"
	"time"
)

type Actuator interface {
	IsHealthy() bool
}

type KafkaActuator struct {
	brokers     []string
	kafkaConfig model.ClientDetails
	topics      []string
}

func NewKafkaActuator(conf model.ClientDetails, topicConfig any) *KafkaActuator {
	return &KafkaActuator{
		brokers:     strings.Split(conf.Servers, ","),
		kafkaConfig: conf,
		topics:      extractTopics(topicConfig),
	}
}

func (k KafkaActuator) IsHealthy() bool {
	for _, address := range k.brokers {
		if isHealthy, err := k.checkTopics(address); err == nil {
			return isHealthy
		} else {
			logger.Logger().Error(fmt.Sprintf("kafka topic check failed. err: %s", err))
		}
	}

	return false
}

func (k KafkaActuator) checkTopics(address string) (bool, error) {
	var conn *kafka.Conn
	var err error

	ctx, cancelFn := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelFn()

	if k.kafkaConfig.IsSecureCluster {
		tlsConfig := client.CreateTLSConfig(k.kafkaConfig.RootCAPath, k.kafkaConfig.IntermediateCAPath)
		secureDialer := client.CreateSecureKafkaDialer(k.kafkaConfig.ScramUsername, k.kafkaConfig.ScramPassword, tlsConfig)

		conn, err = secureDialer.DialContext(ctx, "tcp", address)
	} else {
		conn, err = kafka.DialContext(ctx, "tcp", address)
	}

	if err != nil {
		logger.Logger().Error(fmt.Sprintf("cannot connect to kafka broker: %s, err: %v", address, err.Error()))
		return false, err
	}

	defer conn.Close()

	if _, err := conn.ReadPartitions(k.topics...); err != nil {
		if errors.Is(err, kafka.UnknownTopicOrPartition) {
			logger.Logger().Error(fmt.Sprintf("unknown topic exist: %v", k.topics))
			return false, nil
		}

		return false, err
	}

	return true, nil
}

func extractTopics(topicConfig any) []string {
	b, _ := json.Marshal(topicConfig)
	topicMap := make(map[string]string)
	_ = json.Unmarshal(b, &topicMap)

	topics := make([]string, 0, len(topicMap))
	for key, topic := range topicMap {
		if topic == "" {
			logger.Logger().Fatal(fmt.Sprintf("empty topic name: %s", key))
		}

		topics = append(topics, topic)
	}

	return topics
}
