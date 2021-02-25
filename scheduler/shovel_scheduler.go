package scheduler

import (
	"fmt"
	"github.com/robfig/cron"
	"gitlab.trendyol.com/delivery/cosmos/kafka-message-cycle-manager/configuration"
	"gitlab.trendyol.com/delivery/cosmos/kafka-message-cycle-manager/models"
)

func SetCron(scheduledJob func(configuration.KafkaConfig, configuration.TopicConfig, models.BehaviourModel, models.ConsumerBehaviourType),
	kafkaConfig configuration.KafkaConfig,
	topicConfig configuration.TopicConfig,
	behaviourModel models.BehaviourModel,
	behaviourType models.ConsumerBehaviourType) {
	spec := fmt.Sprintf("@every %dms", topicConfig.ShovelIntervalInMs)

	cronJob := cron.New()
	err := cronJob.AddFunc(spec, func() {
		scheduledJob(kafkaConfig, topicConfig, behaviourModel, behaviourType)
	})
	if err != nil {
		panic(err)
	}

	cronJob.Start()
}
