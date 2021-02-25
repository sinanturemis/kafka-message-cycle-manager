package behaviours

import (
	"runtime/debug"

	"github.com/Shopify/sarama"
	"gitlab.trendyol.com/delivery/cosmos/kafka-message-cycle-manager/executor"
	"gitlab.trendyol.com/delivery/cosmos/kafka-message-cycle-manager/infra"
	"gitlab.trendyol.com/delivery/cosmos/kafka-message-cycle-manager/models"
)

type mainBehaviour struct {
	behaviourModel  models.BehaviourModel
	recoverProducer infra.SyncProducer
	executor        executor.LogicExecutor
}

func newMainBehaviour(recoverProducer infra.SyncProducer, behaviourModel models.BehaviourModel, executor executor.LogicExecutor) Behaviour {
	return &mainBehaviour{
		behaviourModel:  behaviourModel,
		recoverProducer: recoverProducer,
		executor:        executor,
	}
}

//returns (isOk, error) pair
func (mainBehaviour *mainBehaviour) OperateEvent(message *sarama.ConsumerMessage) (bool, error) {
	if executorErr := mainBehaviour.executor.Consume(message.Value, getConsumerContext(message)); executorErr != nil {
		if mainBehaviour.behaviourModel.RetryLimit == models.DefaultRetryCount {
			return true, nil
		}

		setTraceHeader(message, executorErr, string(debug.Stack()))
		err := mainBehaviour.recoverProducer.Publish(
			mainBehaviour.behaviourModel.Retry,
			string(message.Key[:]),
			message.Value,
			getHeaders(message),
		)
		return err == nil, err
	}

	return true, nil
}
