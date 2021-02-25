package services

import (
	"fmt"

	"gitlab.trendyol.com/delivery/cosmos/kafka-message-cycle-manager/behaviours"
	"gitlab.trendyol.com/delivery/cosmos/kafka-message-cycle-manager/configuration"
	"gitlab.trendyol.com/delivery/cosmos/kafka-message-cycle-manager/executor"
	"gitlab.trendyol.com/delivery/cosmos/kafka-message-cycle-manager/handlers"
	"gitlab.trendyol.com/delivery/cosmos/kafka-message-cycle-manager/infra"
	"gitlab.trendyol.com/delivery/cosmos/kafka-message-cycle-manager/models"
)

func CreateTopics(kafkaConfig configuration.KafkaConfig,
	topicConfig configuration.TopicConfig) {
	broker := infra.NewBroker(kafkaConfig)
	broker.CreateTopic(getTopicModel(topicConfig, models.MainBehaviour))

	/*
		If retry limit is bigger than 0, this means, data will be moved to retry topic and when its failed on retry, it will be moved to error topic.
		So, if there is retry logic (if it has complex consumer structure), retry and error topics should be created.
	*/
	if topicConfig.RetryLimit > 0 {
		broker.CreateTopic(getTopicModel(topicConfig, models.RetryBehaviour))
		broker.CreateTopic(getTopicModel(topicConfig, models.ErrorBehaviour))
	}
}

func RunConsumer(kafkaConfig configuration.KafkaConfig,
	topicConfig configuration.TopicConfig,
	behaviourModel models.BehaviourModel,
	behaviourType models.ConsumerBehaviourType,
	executor executor.LogicExecutor) chan bool {

	listenerChannel := make(chan bool)

	topicModel := getTopicModel(topicConfig, behaviourType)
	connectionModel := getConnectionModel(kafkaConfig, topicModel)

	consumer := infra.NewConsumer(connectionModel)
	deadLetterProducer := infra.NewSyncProducer(kafkaConfig)

	behaviour := behaviours.NewBehaviour(deadLetterProducer, behaviourModel, executor, behaviourType)
	handler := handlers.NewSyncEventHandler(behaviour)

	errChannel := consumer.Subscribe(handler)

	// close deadLetterProducer if consumer is closed
	go func() {
		for e := range errChannel {
			fmt.Println(e)
			_ = deadLetterProducer.Close()
		}
	}()
	go func() {
		<-listenerChannel
		consumer.Unsubscribe()
	}()
	fmt.Printf("%v listener is starting \n", connectionModel.Topic)

	return listenerChannel
}

func getConnectionModel(kafkaConfig configuration.KafkaConfig, topicModel models.TopicModel) infra.ConnectionModel {
	return infra.ConnectionModel{
		ConsumerGroupID: topicModel.GroupName,
		ClientConfig:    infra.GetKafkaClientConfig(kafkaConfig.ClientConfig, topicModel.AutoOffsetReset),
		Brokers:         kafkaConfig.Brokers,
		Topic:           topicModel.Name,
	}
}

func getTopicModel(topicConfig configuration.TopicConfig, behaviourType models.ConsumerBehaviourType) models.TopicModel {
	switch behaviourType {
	case models.RetryBehaviour:
		return models.TopicModel{
			Name:              topicConfig.RetryTopicName,
			GroupName:         topicConfig.RetryConsumerGroup,
			PartitionCount:    int32(setDefaultValue(topicConfig.RetryPartitionCount, 1)),
			ReplicationFactor: int16(setDefaultValue(int(topicConfig.RetryReplicationFactor), 1)),
			AutoOffsetReset:   topicConfig.AutoOffsetReset,
		}
	case models.ErrorBehaviour:
		return models.TopicModel{
			Name:              topicConfig.ErrorTopicName,
			GroupName:         topicConfig.ErrorConsumerGroup,
			PartitionCount:    int32(setDefaultValue(topicConfig.ErrorPartitionCount, 1)),
			ReplicationFactor: int16(setDefaultValue(int(topicConfig.ErrorReplicationFactor), 1)),
			AutoOffsetReset:   topicConfig.AutoOffsetReset,
		}
	case models.MainBehaviour:
		fallthrough
	default:
		return models.TopicModel{
			Name:              topicConfig.TopicName,
			GroupName:         topicConfig.ConsumerGroup,
			PartitionCount:    int32(topicConfig.PartitionCount),
			ReplicationFactor: int16(topicConfig.ReplicationFactor),
			AutoOffsetReset:   topicConfig.AutoOffsetReset,
		}
	}
}
func setDefaultValue(value int, defaultValue int) int {
	if value == 0 {
		return defaultValue
	} else {
		return value
	}
}
