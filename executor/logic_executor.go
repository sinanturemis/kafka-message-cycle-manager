package executor

import "gitlab.trendyol.com/delivery/cosmos/kafka-message-cycle-manager/models"

type LogicExecutor interface {
	Consume(value []byte, context models.ConsumerContext) error
	ConsumeRetry(value []byte, context models.ConsumerContext) error
}
