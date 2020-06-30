package druid

import (
	"github.com/Shopify/sarama"
	"github.com/jaegertracing/jaeger/model"
)

type SpanWriter struct {
	producer   sarama.AsyncProducer
	topic      string
	marshaller DruidMarshall
}
// NewSpanWriter initiates and returns a new kafka spanwriter
func NewSpanWriter(producer sarama.AsyncProducer, topic string) *SpanWriter {
	go func() {
		for range producer.Successes() {
		}
	}()
	go func() {
		for e := range producer.Errors() {
			println(e)
		}
	}()
	return &SpanWriter{
		producer: producer,
		topic:    topic,
	}
}

// WriteSpan writes the span to kafka.
func (w *SpanWriter) WriteSpan(span *model.Span) error {
	// Need to normalize the span,
	spanBytes, err := w.marshaller.Marshal(span)

	if err != nil {
		return err
	}


	// The AsyncProducer accepts messages on a channel and produces them asynchronously
	// in the background as efficiently as possible
	w.producer.Input() <- &sarama.ProducerMessage{
		Topic: w.topic,
		Key:   sarama.StringEncoder(span.TraceID.String()),
		Value: sarama.ByteEncoder(spanBytes),
	}
	return nil
}

// Close closes SpanWriter by closing producer
func (w *SpanWriter) Close() error {
	return w.producer.Close()
}
