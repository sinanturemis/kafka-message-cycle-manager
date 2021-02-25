package services

import (
	"gitlab.trendyol.com/delivery/cosmos/kafka-message-cycle-manager/configuration"
	"gitlab.trendyol.com/delivery/cosmos/kafka-message-cycle-manager/models"
	"time"
)

func RunShovel(kafkaConfig configuration.KafkaConfig, topicConfig configuration.TopicConfig, behaviourModel models.BehaviourModel, behaviourType models.ConsumerBehaviourType) {
	channel := RunConsumer(kafkaConfig, topicConfig, behaviourModel, behaviourType, nil)
	go func() {
		<-time.Tick(time.Duration(topicConfig.ShovelRunningDurationInMs) * time.Millisecond)
		close(channel)
	}()
}
