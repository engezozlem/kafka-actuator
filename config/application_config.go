package config

import (
	"github.com/spf13/viper"
	"kafka-actuator/config/model"
	"os"
)

type ApplicationConfig struct {
	Kafka model.KafkaConfig
}

type config struct{}

type Config interface {
	GetConfig() (*ApplicationConfig, error)
}

func (c *config) GetConfig() (*ApplicationConfig, error) {
	configuration := ApplicationConfig{}
	env := getGoEnv()

	viperInstance := getViperInstance()
	err := viperInstance.ReadInConfig()

	if err != nil {
		return nil, err
	}

	sub := viperInstance.Sub(env)
	err = sub.Unmarshal(&configuration)

	if err != nil {
		return nil, err
	}

	return &configuration, nil
}

func CreateConfigInstance() *config {
	return &config{}
}

func getViperInstance() *viper.Viper {
	viperInstance := viper.New()
	viperInstance.SetConfigFile("resources/config.yml")
	return viperInstance
}

func getGoEnv() string {
	env := os.Getenv("GO_ENV")
	if env != "" {
		return env
	}
	return "dev"
}
