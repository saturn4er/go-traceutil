package traceutil

import (
	"context"

	"github.com/Shopify/sarama"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"github.com/opentracing/opentracing-go/log"
	"github.com/pkg/errors"
)

func ChildSpanFromContext(ctx context.Context, operationName string, options ...opentracing.StartSpanOption) (context.Context, opentracing.Span) {
	ctxSpan := opentracing.SpanFromContext(ctx)
	if ctxSpan != nil {
		parentSpan := ctxSpan.(opentracing.Span)
		tracer := parentSpan.Tracer()
		options = append([]opentracing.StartSpanOption{opentracing.ChildOf(parentSpan.Context())}, options...)
		span := tracer.StartSpan(operationName, options...)
		ctx = opentracing.ContextWithSpan(ctx, span)

		return ctx, span
	}

	return ctx, opentracing.NoopTracer{}.StartSpan("")
}

func FinishSpanWithErr(span opentracing.Span, err error) {
	if err != nil {
		ext.Error.Set(span, true)
		span.LogFields(log.Error(err))
	}
	span.Finish()
}

func SpanFromSaramaMessage(tracer opentracing.Tracer, message *sarama.ConsumerMessage, operationName string, options ...opentracing.StartSpanOption) (opentracing.Span, error) {
	spanCtx, err := ExtractSpanContextFromMessage(tracer, message)
	if err != nil && err != opentracing.ErrSpanContextNotFound {
		return nil, errors.Wrap(err, "failed to extract span context from message")
	}
	options = append([]opentracing.StartSpanOption{
		ext.RPCServerOption(spanCtx),
		ext.SpanKindConsumer,
		opentracing.Tag{Key: "msg.key", Value: string(message.Key)},
	}, options...)
	span := tracer.StartSpan(operationName,
		options...,
	)
	span.LogFields(
		log.Int("offset", int(message.Offset)),
		log.String("timestamp", message.Timestamp.String()),
	)

	return span, nil
}
