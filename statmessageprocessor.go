package stats

import (
	"context"
	"time"

	"github.com/asecurityteam/messageprocessor"

	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/rs/xstats"
)

const (
	consumedCounter        = "kinesis.consumed"
	consumerSuccessCounter = "kinesis.consumer_success"
	consumerErrorCounter   = "kinesis.consumer_error"
	consumedSize           = "kinesis.consumed_size"
	consumerLag            = "kinesis.consumer_lag.timing"
	consumerTimingSuccess  = "kinesis.consumer.timing.success"
	consumerTimingFailure  = "kinesis.consumer.timing.failure"
)

// StatMessageProcessorConfig is the config for creating a StatMessageProcessor
type StatMessageProcessorConfig struct {
	ConsumedCounter        string `description:"Name of overall kinesis record consumption metric."`
	ConsumerSuccessCounter string `description:"Name of overall successful kinesis record consumption metric."`
	ConsumerErrorCounter   string `description:"Name of overall failed kinesis record consumption metric."`
	ConsumedSize           string `description:"Name of consumed kinesis record size metric."`
	ConsumerLag            string `description:"Name of lag time between kinesis production and consumption metric."`
	ConsumerTimingSuccess  string `description:"Name of time to process successful kinesis record metric."`
	ConsumerTimingFailure  string `description:"Name of time to process failed kinesis record metric."`
}

// Name of the config root.
func (*StatMessageProcessorConfig) Name() string {
	return "consumerMetrics"
}

// StatMessageProcessorComponent implements the settings.Component interface.
type StatMessageProcessorComponent struct{}

// NewComponent populates default values.
func NewComponent() *StatMessageProcessorComponent {
	return &StatMessageProcessorComponent{}
}

// Settings generates a config populated with defaults.
func (*StatMessageProcessorComponent) Settings() *StatMessageProcessorConfig {
	return &StatMessageProcessorConfig{
		ConsumedCounter:        consumedCounter,
		ConsumerSuccessCounter: consumerSuccessCounter,
		ConsumerErrorCounter:   consumerErrorCounter,
		ConsumedSize:           consumedSize,
		ConsumerLag:            consumerLag,
		ConsumerTimingSuccess:  consumerTimingSuccess,
		ConsumerTimingFailure:  consumerTimingFailure,
	}
}

func (c *StatMessageProcessorComponent) New(_ context.Context, conf *StatMessageProcessorConfig) (func(messageprocessor.MessageProcessor) messageprocessor.MessageProcessor, error) { // nolint

	return func(processor messageprocessor.MessageProcessor) messageprocessor.MessageProcessor {
		return &StatMessageProcessor{
			ConsumedCounter:        conf.ConsumedCounter,
			ConsumerSuccessCounter: conf.ConsumerSuccessCounter,
			ConsumerErrorCounter:   conf.ConsumerErrorCounter,
			ConsumedSize:           conf.ConsumedSize,
			ConsumerLag:            conf.ConsumerLag,
			ConsumerTimingSuccess:  conf.ConsumerTimingSuccess,
			ConsumerTimingFailure:  conf.ConsumerTimingFailure,
			wrapped:                processor,
		}
	}, nil
}

// StatMessageProcessor is wrapper around MessageProcessor to capture and emit Kinesis related stats
type StatMessageProcessor struct {
	ConsumedCounter        string
	ConsumerSuccessCounter string
	ConsumerErrorCounter   string
	ConsumedSize           string
	ConsumerLag            string
	ConsumerTimingSuccess  string
	ConsumerTimingFailure  string
	wrapped                messageprocessor.MessageProcessor
}

// ProcessMessage injects an `xstats.XStater` into the request and invokes the
// wrapped `MessageProcessor.ProcessMessage`.
func (t StatMessageProcessor) ProcessMessage(ctx context.Context, record *kinesis.Record) messageprocessor.MessageProcessError {
	stat := xstats.FromContext(ctx)
	// consumerLag - Time.Duration between the time immediately before the message is processed and its Record.ApproximateArrivalTimestamp,
	//which is the timestamp of when the record was inserted into the Kinesis stream.
	messageArrivalTimeStamp := record.ApproximateArrivalTimestamp
	lagDuration := time.Since(messageArrivalTimeStamp.Local())
	stat.Timing(t.ConsumerLag, lagDuration)

	// consumedCounter - Incremented for every message received, regardless of success or failure
	stat.Count(t.ConsumedCounter, 1)

	// length of message body
	stat.Count(t.ConsumedSize, float64(len(record.Data)))

	var start = time.Now()
	err := t.wrapped.ProcessMessage(ctx, record)
	var end = time.Now().Sub(start)
	if err == nil {
		// consumerSuccessCounter - Incremented for every message processed successfully
		stat.Count(t.ConsumerSuccessCounter, 1)
		// consumerTimingSuccess - Time.Duration for processing of a message which is successfully processed by underlying Kinesis consumer
		stat.Timing(t.ConsumerTimingSuccess, end)
	} else {
		// consumerFailure - Incremented for every message which is failed to be processed
		stat.Count(t.ConsumerErrorCounter, 1)
		// consumerTimingFailure - Time.Duration for processing of a message which underlying Kinesis consumer fails to process
		stat.Timing(t.ConsumerTimingFailure, end)
	}
	return err
}

// NewStatMessageProcessor returns a function that wraps a `messageprocessor.MessageProcessor` in a
// `StatMessageProcessor` `messageprocessor.MessageProcessor`.
func NewStatMessageProcessor() func(messageprocessor.MessageProcessor) messageprocessor.MessageProcessor {
	return func(processor messageprocessor.MessageProcessor) messageprocessor.MessageProcessor {
		return &StatMessageProcessor{wrapped: processor}
	}
}
