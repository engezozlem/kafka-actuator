package main

import (
	"context"
	"kafka-actuator/actuator"
	"kafka-actuator/config"
	"kafka-actuator/logger"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	_, cancel := context.WithCancel(context.Background())
	gracefulShutdown := createGracefulShutdownChannel()
	configInstance := config.CreateConfigInstance()
	applicationConfig, err := configInstance.GetConfig()
	if err != nil {
		panic("application config read failed: " + err.Error())
	}

	kafkaActuator := actuator.NewKafkaActuator(applicationConfig.Kafka.ClientDetails, applicationConfig.Kafka.Topics)

	if kafkaActuator.IsHealthy() {
		panic("kafka is not health!")
	}

	<-gracefulShutdown
	defer func() {
		logger.Logger().Info("gracefully shutting down...")
		_ = logger.Logger().Sync()
		cancel()
	}()
}

func createGracefulShutdownChannel() chan os.Signal {
	gracefulShutdown := make(chan os.Signal, 1)
	signal.Notify(gracefulShutdown, syscall.SIGTERM)
	signal.Notify(gracefulShutdown, syscall.SIGINT)
	return gracefulShutdown
}
