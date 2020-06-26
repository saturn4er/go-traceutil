package traceutil

import (
	"github.com/Shopify/sarama"
	"github.com/opentracing/opentracing-go"
)

type ProducerMessageCarrier sarama.ProducerMessage

func (c *ProducerMessageCarrier) Set(key, val string) {
	c.Headers = append(c.Headers, sarama.RecordHeader{
		Key:   []byte(key),
		Value: []byte(val),
	})
}

func InjectSpanContextToMessage(span opentracing.Span, message *sarama.ProducerMessage) error {
	return span.Tracer().Inject(span.Context(), opentracing.TextMap, (*ProducerMessageCarrier)(message))
}

type ConsumerMessageCarrier sarama.ConsumerMessage

func (c *ConsumerMessageCarrier) ForeachKey(handler func(key, val string) error) error {
	for _, header := range c.Headers {
		if err := handler(string(header.Key), string(header.Value)); err != nil {
			return err
		}
	}

	return nil
}

func ExtractSpanContextFromMessage(tracer opentracing.Tracer, message *sarama.ConsumerMessage) (opentracing.SpanContext, error) {
	return tracer.Extract(opentracing.TextMap, (*ConsumerMessageCarrier)(message))
}
