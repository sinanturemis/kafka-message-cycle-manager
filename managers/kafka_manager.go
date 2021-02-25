package managers

import (
	"gitlab.trendyol.com/delivery/cosmos/kafka-message-cycle-manager/configuration"
	"gitlab.trendyol.com/delivery/cosmos/kafka-message-cycle-manager/executor"
	"gitlab.trendyol.com/delivery/cosmos/kafka-message-cycle-manager/models"
	"gitlab.trendyol.com/delivery/cosmos/kafka-message-cycle-manager/scheduler"
	"gitlab.trendyol.com/delivery/cosmos/kafka-message-cycle-manager/services"
)

type kafkaConsumer struct {
	kafkaConfig configuration.KafkaConfig
}

func NewKafkaManager(kafkaConfig configuration.KafkaConfig) *kafkaConsumer {
	configuration.AddCustomConfigValidationRules()
	configuration.ValidateKafkaConfig(&kafkaConfig)

	return &kafkaConsumer{
		kafkaConfig: kafkaConfig,
	}
}

func (kafkaConsumer *kafkaConsumer) Start(topicConfig configuration.TopicConfig, executor executor.LogicExecutor) {
	configuration.ValidateTopicConfig(&topicConfig)
	behaviourModel := models.BehaviourModel{
		Retry:             topicConfig.RetryTopicName,
		Error:             topicConfig.ErrorTopicName,
		RetryLimit:        topicConfig.RetryLimit,
		ShovelLimit:       topicConfig.ShovelLimit,
		RetryIntervalInMs: topicConfig.RetryIntervalInMs,
	}

	kafkaConsumer.CreateTopics(topicConfig)
	kafkaConsumer.startConsumer(topicConfig, behaviourModel, executor)
	kafkaConsumer.startRetryConsumer(topicConfig, behaviourModel, executor)
	kafkaConsumer.startShovelProcessor(topicConfig, behaviourModel)
}

func (kafkaConsumer *kafkaConsumer) CreateTopics(topicConfig configuration.TopicConfig) {
	services.CreateTopics(kafkaConsumer.kafkaConfig, topicConfig)
}

func (kafkaConsumer *kafkaConsumer) startConsumer(topicConfig configuration.TopicConfig, behaviourModel models.BehaviourModel, executor executor.LogicExecutor) {
	services.RunConsumer(kafkaConsumer.kafkaConfig,
		topicConfig,
		behaviourModel,
		models.MainBehaviour,
		executor)
}

func (kafkaConsumer *kafkaConsumer) startRetryConsumer(topicConfig configuration.TopicConfig, behaviourModel models.BehaviourModel, executor executor.LogicExecutor) {
	if topicConfig.RetryLimit > models.DefaultRetryCount {
		services.RunConsumer(kafkaConsumer.kafkaConfig,
			topicConfig,
			behaviourModel,
			models.RetryBehaviour,
			executor)
	}
}

func (kafkaConsumer *kafkaConsumer) startShovelProcessor(topicConfig configuration.TopicConfig, behaviourModel models.BehaviourModel) {
	if topicConfig.ShovelLimit > 0 {
		scheduler.SetCron(services.RunShovel,
			kafkaConsumer.kafkaConfig,
			topicConfig,
			behaviourModel,
			models.ErrorBehaviour)
	}
}

func (kafkaConsumer *kafkaConsumer) buildBehaviourModel(topicConfig configuration.TopicConfig) models.BehaviourModel {
	return models.BehaviourModel{
		Retry:             topicConfig.RetryTopicName,
		Error:             topicConfig.ErrorTopicName,
		RetryLimit:        topicConfig.RetryLimit,
		ShovelLimit:       topicConfig.ShovelLimit,
		RetryIntervalInMs: topicConfig.RetryIntervalInMs,
	}
}
