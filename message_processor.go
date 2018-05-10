package queueprocessor

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
)

const (
	brokerURLEnvVariable             string = "BROKER_URL"
	transportNameEnvVariable         string = "TRANSPORT_NAME"
	errorTransportNameEnvVariable    string = "ERROR_TRANSPORT_NAME"
	userNameEnvVariable              string = "QUEUE_PROCESSING_USER"
	passwordEnvVariable              string = "QUEUE_PROCESSING_PASSWORD"
	requestRetryThresholdEnvVariable string = "REQUEST_RETRY_THRESHOLD_ENV_VARIABLE"
	messageThresholdEnvVariable      string = "MESSAGE_THRESHOLD_ENV_VARIABLE"
	recheckPeriodEnvVariable         string = "RECHECK_PERIOD_ENV_VARIABLE"
	maxCores                         int    = 8
)

//MessageProcessor is handles messages specific to a business case
type MessageProcessor interface {
	HandleMessage(msg []byte) error
}

//MessageHandler is a harness for reactive jms processor
type MessageHandler interface {
	Run()
	SetProcessor(processor *MessageProcessor)
}

var brokerURL string
var transportName string
var errorTransportName string
var username string
var password string
var requestRetryThreshold int

var messageThreshold int
var recheckPeriod int
var cores int

type messageHandler struct {
	messageProcessor *MessageProcessor
}

func init() {
	fmt.Println("In init")

	brokerURL = os.Getenv(brokerURLEnvVariable)
	transportName = os.Getenv(transportNameEnvVariable)
	errorTransportName = os.Getenv(errorTransportNameEnvVariable)
	username = os.Getenv(userNameEnvVariable)
	password = os.Getenv(passwordEnvVariable)
	var err error
	requestRetryThreshold, err = strconv.Atoi(os.Getenv(requestRetryThresholdEnvVariable))

	if err != nil {
		requestRetryThreshold = 3
	}

	messageThreshold, err = strconv.Atoi(os.Getenv(messageThresholdEnvVariable))

	if err != nil {
		messageThreshold = 10
	}

	recheckPeriod, err = strconv.Atoi(os.Getenv(recheckPeriodEnvVariable))

	if err != nil {
		recheckPeriod = 3000
	}

	cores = runtime.NumCPU()
	if cores > maxCores {
		cores = maxCores
	}
}

func (handler *messageHandler) SetProcessor(processor *MessageProcessor) {
	handler.messageProcessor = processor
}

func (handler *messageHandler) Run() {
	err := handler.initializeEnvironment()

	if err != nil {
		//blow up
	}
}

func (handler *messageHandler) initializeEnvironment() error {

	return nil
}
