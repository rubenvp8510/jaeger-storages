package questbd

import (
	"flag"
	"github.com/jaegertracing/jaeger/storage/dependencystore"
	"github.com/jaegertracing/jaeger/storage/spanstore"
	"github.com/spf13/viper"
	"github.com/uber/jaeger-lib/metrics"
	"go.uber.org/zap"
)


type Factory struct {
	options Options
	questDB *QuestDBRest
	writer *Writer
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
}

func (f *Factory) Initialize(metricsFactory metrics.Factory, zapLogger *zap.Logger) error {
	client, err := NewQuestDBRest(f.options.Host)
	f.questDB = client

	if err != nil {
		return err
	}

	f.writer = NewWriter(f.questDB)
	f.writer.start()

	return nil
}

func (f *Factory) CreateSpanReader() (spanstore.Reader, error) {
	return &Reader{
		questDB:f.questDB,
	}, nil
}

func (f *Factory) CreateSpanWriter() (spanstore.Writer, error) {
	return f.writer, nil
}

func (f *Factory) CreateDependencyReader() (dependencystore.Reader, error) {
	return nil, nil
}
