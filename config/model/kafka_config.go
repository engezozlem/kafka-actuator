package model

type KafkaConfig struct {
	ClientDetails ClientDetails
	Topics        []string
}

type ClientDetails struct {
	Servers            string
	ConsumerGroup      string
	RootCAPath         string
	IntermediateCAPath string
	Rack               string
	ScramUsername      string
	ScramPassword      string
	IsSecureCluster    bool
}
