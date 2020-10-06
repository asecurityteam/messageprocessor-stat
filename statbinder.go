package stats

import (
	"context"

	"github.com/asecurityteam/messageprocessor"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/rs/xstats"
)

// StatBinder is a `MessageProcessor` decorator that injects an
// `xstats.XStater` into the `context.Context` of the given `context`.
//
// The `StatBinder` can then, in turn, be decorated with `Transport` to
// make use of the injected `xstats.XStater` to emit key HTTP metrics on
// each ProcessMessage.
type StatBinder struct {
	stats   xstats.XStater
	wrapped messageprocessor.MessageProcessor
}

// ProcessMessage injects an `xstats.XStater` into the context and invokes the
// wrapped `MessageProcessor`.
func (t *StatBinder) ProcessMessage(ctx context.Context, record *kinesis.Record) messageprocessor.MessageProcessorError {
	ctx = xstats.NewContext(ctx, xstats.Copy(t.stats))
	return t.wrapped.ProcessMessage(ctx, record)
}

// NewStatBinder returns a function that wraps a `transport.Decorator` in a
// `StatTransport` `transport.Decorator`.
func NewStatBinder(stats xstats.XStater) func(messageprocessor.MessageProcessor) messageprocessor.MessageProcessor {
	return func(next messageprocessor.MessageProcessor) messageprocessor.MessageProcessor {
		return &StatBinder{stats: stats, wrapped: next}
	}
}
