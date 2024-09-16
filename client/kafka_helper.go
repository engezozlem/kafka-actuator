package client

import (
	"crypto/tls"
	"crypto/x509"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
	"io/ioutil"
)

func CreateTLSConfig(rootCAPath, interCAPath string) *tls.Config {
	rootCA, err := ioutil.ReadFile(rootCAPath)
	if err != nil {
		panic("Error while reading Root CA file: " + rootCAPath + " error: " + err.Error())
	}

	interCA, err := ioutil.ReadFile(interCAPath)
	if err != nil {
		panic("Error while reading Intermediate CA file: " + interCAPath + " error: " + err.Error())
	}

	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(rootCA)
	caCertPool.AppendCertsFromPEM(interCA)

	return &tls.Config{
		RootCAs: caCertPool,
	}
}

func CreateSecureKafkaDialer(scramUsername, scramPassword string, tlsConfig *tls.Config) *kafka.Dialer {
	mechanism, err := scram.Mechanism(scram.SHA512, scramUsername, scramPassword)
	if err != nil {
		panic("Error while creating SCRAM configuration, error: " + err.Error())
	}

	return &kafka.Dialer{
		TLS:           tlsConfig,
		SASLMechanism: mechanism,
	}
}
