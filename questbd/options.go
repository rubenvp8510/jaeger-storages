package questbd

import (
	"flag"
	"github.com/spf13/viper"
)

const (
	configPrefix = "questdb"
	suffixHost   = ".host"

	defaultHost = "http://127.0.0.1:9000"
)

type Options struct {
	Host string
}

// AddFlags adds flags for Options
func (opt *Options) AddFlags(flagSet *flag.FlagSet) {
	flagSet.String(
		configPrefix+suffixHost,
		defaultHost,
		"Quest database host:port , REST endpoint")
}

func (opt *Options) InitFromViper(v *viper.Viper) {
	opt.Host = v.GetString(configPrefix + suffixHost)

}
