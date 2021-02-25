package infra

import (
	"github.com/Shopify/sarama"
	"gitlab.trendyol.com/delivery/cosmos/kafka-message-cycle-manager/configuration"
	"gitlab.trendyol.com/delivery/cosmos/kafka-message-cycle-manager/models"
)

type Broker interface {
	CreateTopic(models.TopicModel)
}

type kafkaBroker struct {
	kafkaConfig configuration.KafkaConfig
}

func NewBroker(kafkaConfig configuration.KafkaConfig) Broker {
	return &kafkaBroker{
		kafkaConfig: kafkaConfig,
	}
}

func (broker *kafkaBroker) CreateTopic(topicModel models.TopicModel) {
	brokerAddress := broker.kafkaConfig.Brokers
	config := sarama.NewConfig()
	parsedKafkaVersion, err := sarama.ParseKafkaVersion(broker.kafkaConfig.ClientConfig.KafkaVersion)
	if err != nil {
		panic(err)
	}
	config.Version = parsedKafkaVersion
	admin, err := sarama.NewClusterAdmin(brokerAddress, config)
	if err != nil {
		panic(err)
	}

	defer func() { _ = admin.Close() }()
	listTopics, _ := admin.ListTopics()
	if _, ok := listTopics[topicModel.Name]; !ok {
		err = admin.CreateTopic(topicModel.Name, &sarama.TopicDetail{
			NumPartitions:     topicModel.PartitionCount,
			ReplicationFactor: topicModel.ReplicationFactor,
		}, false)
		if err != nil {
			panic(err)
		}
	}
}
