package infra

import "github.com/Shopify/sarama"

type ConnectionModel struct {
	ClientConfig    *sarama.Config
	Brokers         []string
	Topic           string
	ConsumerGroupID string
}
