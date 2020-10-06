package stats

import (
	"context"
	"testing"
	"time"

	"github.com/rs/xstats"
	"github.com/stretchr/testify/assert"

	"github.com/aws/aws-sdk-go/service/kinesis"

	"github.com/golang/mock/gomock"
)

func TestStatMessageProcessor_Component(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockMessageProcessor := NewMockMessageProcessor(ctrl)
	mockStater := NewMockStat(ctrl)

	statMessageProcessor := StatMessageProcessor{
		ConsumedCounter:        consumedCounter,
		ConsumerSuccessCounter: consumerSuccessCounter,
		ConsumedSize:           consumedSize,
		ConsumerLag:            consumerLag,
		ConsumerTimingSuccess:  consumerTimingSuccess,
		wrapped:                mockMessageProcessor,
	}

	incomingDataRecord := []byte(`{
		"type":"CHANNEL",
		"rawpayload":"{\"channel\":\"channelbob\",\"text\":\"the message\"}"
	}`)
	currentTime := time.Now()
	sequenceNumber := "12345"
	kinesisRecord := kinesis.Record{
		Data:                        incomingDataRecord,
		ApproximateArrivalTimestamp: &currentTime,
		SequenceNumber:              &sequenceNumber,
	}
	gomock.InOrder(
		mockStater.EXPECT().Timing(consumerLag, gomock.Any()),
		mockStater.EXPECT().Count(consumedCounter, gomock.Any()),
		mockStater.EXPECT().Count(consumedSize, gomock.Any()),
		mockStater.EXPECT().Count(consumerSuccessCounter, gomock.Any()),
		mockStater.EXPECT().Timing(consumerTimingSuccess, gomock.Any()),
	)

	mockMessageProcessor.EXPECT().ProcessMessage(gomock.Any(), gomock.Any()).Return(nil)
	e := statMessageProcessor.ProcessMessage(xstats.NewContext(context.Background(), mockStater), &kinesisRecord)
	assert.Nil(t, e)

}

func TestStatMessageProcessor_ProcessMessageSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockMessageProcessor := NewMockMessageProcessor(ctrl)
	mockStater := NewMockStat(ctrl)

	statMessageProcessor := StatMessageProcessor{
		ConsumedCounter:        consumedCounter,
		ConsumerSuccessCounter: consumerSuccessCounter,
		ConsumedSize:           consumedSize,
		ConsumerLag:            consumerLag,
		ConsumerTimingSuccess:  consumerTimingSuccess,
		wrapped:                mockMessageProcessor,
	}

	incomingDataRecord := []byte(`{
		"type":"CHANNEL",
		"rawpayload":"{\"channel\":\"channelbob\",\"text\":\"the message\"}"
	}`)
	currentTime := time.Now()
	sequenceNumber := "12345"
	kinesisRecord := kinesis.Record{
		Data:                        incomingDataRecord,
		ApproximateArrivalTimestamp: &currentTime,
		SequenceNumber:              &sequenceNumber,
	}
	gomock.InOrder(
		mockStater.EXPECT().Timing(consumerLag, gomock.Any()),
		mockStater.EXPECT().Count(consumedCounter, gomock.Any()),
		mockStater.EXPECT().Count(consumedSize, gomock.Any()),
		mockStater.EXPECT().Count(consumerSuccessCounter, gomock.Any()),
		mockStater.EXPECT().Timing(consumerTimingSuccess, gomock.Any()),
	)

	mockMessageProcessor.EXPECT().ProcessMessage(gomock.Any(), gomock.Any()).Return(nil)
	e := statMessageProcessor.ProcessMessage(xstats.NewContext(context.Background(), mockStater), &kinesisRecord)
	assert.Nil(t, e)

}

func TestStatMessageProcessor_ProcessMessageFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockMessageProcessor := NewMockMessageProcessor(ctrl)
	mockStater := NewMockStat(ctrl)
	mockMessageProcessorError := NewMockMessageProcessorError(ctrl)

	statMessageProcessor := StatMessageProcessor{
		ConsumedCounter:       consumedCounter,
		ConsumerErrorCounter:  consumerErrorCounter,
		ConsumedSize:          consumedSize,
		ConsumerLag:           consumerLag,
		ConsumerTimingFailure: consumerTimingFailure,
		wrapped:               mockMessageProcessor,
	}

	incomingDataRecord := []byte(`{
		"type":"CHANNEL",
		"rawpayload":"{\"channel\":\"channelbob\",\"text\":\"the message\"}"
	}`)
	currentTime := time.Now()
	sequenceNumber := "12345"
	kinesisRecord := kinesis.Record{
		Data:                        incomingDataRecord,
		ApproximateArrivalTimestamp: &currentTime,
		SequenceNumber:              &sequenceNumber,
	}

	gomock.InOrder(
		mockStater.EXPECT().Timing(consumerLag, gomock.Any()),
		mockStater.EXPECT().Count(consumedCounter, gomock.Any()),
		mockStater.EXPECT().Count(consumedSize, gomock.Any()),
		mockStater.EXPECT().Count(consumerErrorCounter, gomock.Any()),
		mockStater.EXPECT().Timing(consumerTimingFailure, gomock.Any()),
	)

	mockMessageProcessor.EXPECT().ProcessMessage(gomock.Any(), gomock.Any()).Return(mockMessageProcessorError)
	e := statMessageProcessor.ProcessMessage(xstats.NewContext(context.Background(), mockStater), &kinesisRecord)
	assert.NotNil(t, e)

}
