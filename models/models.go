package models

import "gitlab.trendyol.com/delivery/cosmos/kafka-message-cycle-manager/configuration"

type BehaviourModel struct {
	Retry             string
	Error             string
	RetryLimit        int
	ShovelLimit       int
	RetryIntervalInMs int
}

type TopicModel struct {
	Name              string
	GroupName         string
	PartitionCount    int32
	ReplicationFactor int16
	AutoOffsetReset   configuration.AutoOffsetResetType
}

type ConsumerContext struct {
	CorrelationId string
}

type ConsumerBehaviourType string

const (
	MainBehaviour  ConsumerBehaviourType = "MAIN_BEHAVIOUR"
	RetryBehaviour ConsumerBehaviourType = "RETRY_BEHAVIOUR"
	ErrorBehaviour ConsumerBehaviourType = "ERROR_BEHAVIOUR"
)

const DefaultShovelCount = 0
const DefaultRetryCount = 0
const ShovelCountKey = "ShovelCount"
const RetryCountKey = "RetryCount"
const CorrelationId = "X-CorrelationId"
