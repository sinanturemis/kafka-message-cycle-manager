package behaviours

import (
	"errors"
	"runtime/debug"
	"time"

	"github.com/Shopify/sarama"
	"gitlab.trendyol.com/delivery/cosmos/kafka-message-cycle-manager/executor"
	"gitlab.trendyol.com/delivery/cosmos/kafka-message-cycle-manager/infra"
	"gitlab.trendyol.com/delivery/cosmos/kafka-message-cycle-manager/models"
)

type retryBehaviour struct {
	behaviourModel  models.BehaviourModel
	recoverProducer infra.SyncProducer
	executor        executor.LogicExecutor
}

func newRetryBehaviour(recoverProducer infra.SyncProducer, behaviourModel models.BehaviourModel, executor executor.LogicExecutor) Behaviour {
	return &retryBehaviour{
		behaviourModel:  behaviourModel,
		recoverProducer: recoverProducer,
		executor:        executor,
	}
}

func (retryBehaviour *retryBehaviour) OperateEvent(message *sarama.ConsumerMessage) (bool, error) {
	retryCount := models.DefaultRetryCount
	executorErr := errors.New("")
	for retryBehaviour.canBeRetried(retryCount) {
		time.Sleep(time.Duration(retryBehaviour.behaviourModel.RetryIntervalInMs) * time.Millisecond)
		executorErr = retryBehaviour.executor.ConsumeRetry(message.Value, getConsumerContext(message))
		if executorErr == nil {
			return true, nil
		}
		retryCount++
	}

	setTraceHeader(message, executorErr, string(debug.Stack()))

	err := retryBehaviour.recoverProducer.Publish(
		retryBehaviour.behaviourModel.Error,
		string(message.Key[:]),
		message.Value,
		getHeaders(message),
	)
	return err == nil, err
}

func (retryBehaviour *retryBehaviour) canBeRetried(currentRetryCount int) bool {
	return currentRetryCount < retryBehaviour.behaviourModel.RetryLimit
}
