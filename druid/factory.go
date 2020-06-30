package druid

import (
	"flag"
	"github.com/Shopify/sarama"
	"github.com/jaegertracing/jaeger/pkg/kafka/producer"
	"github.com/jaegertracing/jaeger/storage/dependencystore"
	"github.com/jaegertracing/jaeger/storage/spanstore"
	"github.com/spf13/viper"
	"github.com/uber/jaeger-lib/metrics"
	"go.uber.org/zap"
)

type Factory struct {
	options Options
	producer.Builder
	producer   sarama.AsyncProducer

}

func NewFactory() *Factory {
	return &Factory{}
}

func (f *Factory) AddFlags(flagSet *flag.FlagSet) {
	f.options.AddFlags(flagSet)
}
// InitFromViper implements plugin.Configurable
func (f *Factory) InitFromViper(v *viper.Viper) {
	f.options.InitFromViper(v)
	f.Builder = &f.options.Config
}


func (f *Factory) Initialize(metricsFactory metrics.Factory, zapLogger *zap.Logger) error {
	p, err := f.NewProducer()
	if err != nil {
		return err
	}
	f.producer = p
	return nil
}

func (f *Factory) CreateSpanReader() (spanstore.Reader, error) {
	reader, err := NewReader("http://localhost:8888")
	return reader, err
}

func (f *Factory) CreateSpanWriter() (spanstore.Writer, error) {
	return NewSpanWriter(f.producer, f.options.Topic), nil
}
func (f *Factory) CreateDependencyReader() (dependencystore.Reader, error) {
	return nil, nil
}
