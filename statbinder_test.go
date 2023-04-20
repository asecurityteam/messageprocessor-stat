package stats

import (
	"context"
	"testing"

	"github.com/asecurityteam/messageprocessor"

	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/golang/mock/gomock"
	"github.com/rs/xstats"
	"github.com/stretchr/testify/assert"
)

type dummyMessageProcessor struct {
	testFunc func(ctx context.Context)
}

func (t *dummyMessageProcessor) ProcessMessage(ctx context.Context, record *kinesis.Record) messageprocessor.MessageProcessorError {
	return nil
}

func TestStatBinder_ProcessMessage(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStater := NewMockStat(ctrl)

	processMessageFunc := func(ctx context.Context) {
		stater := xstats.FromContext(ctx)
		assert.Equal(t, mockStater, stater)
	}

	statBinder := NewStatBinder(mockStater)
	messageProcessor := statBinder(&dummyMessageProcessor{
		testFunc: processMessageFunc,
	})

	e := messageProcessor.ProcessMessage(context.Background(), &kinesis.Record{})
	assert.Nil(t, e)
}
