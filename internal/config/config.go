package config

import (
	"os"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/caarlos0/env/v11"
	"go.uber.org/zap/zapcore"
)

type Config struct {
	Daemon struct {
		Enabled          bool          `env:"ENABLED" envDefault:"false"`
		Frequency        time.Duration `env:"FREQUENCY" envDefault:"1h"`
		ExecutionTimeout time.Duration `env:"EXECUTION_TIMEOUT" envDefault:"30m"`
	} `envPrefix:"DAEMON_"`

	SentryDsn string        `env:"SENTRY_DSN"`
	JsonLogs  bool          `env:"JSON_LOGS" envDefault:"false"`
	LogLevel  zapcore.Level `env:"LOG_LEVEL" envDefault:"info"`

	DatabaseUri string `env:"DATABASE_URI"`

	S3 struct {
		Import struct {
			Bucket string `env:"BUCKET,required"`
		} `envPrefix:"IMPORT_"`

		Archive struct {
			Bucket string `env:"BUCKET,required"`
		} `envPrefix:"ARCHIVE_"`

		Endpoint  string `env:"ENDPOINT,required"`
		AccessKey string `env:"ACCESS_KEY,required"`
		SecretKey string `env:"SECRET_KEY,required"`
	} `envPrefix:"S3_"`

	V1PublicKey string `env:"V1_PUBLIC_KEY"`

	LogArchiver struct {
		Url string `env:"URL,,required"`
		Key string `env:"KEY,required"`
	} `envPrefix:"LOG_ARCHIVER_"`
}

var Conf Config

func LoadConfig() (Config, error) {
	if _, err := os.Stat("config.toml"); err == nil {
		return fromToml()
	} else {
		return fromEnvvar()
	}
}

func fromToml() (Config, error) {
	var config Config
	if _, err := toml.DecodeFile("config.toml", &Conf); err != nil {
		return Config{}, err
	}

	return config, nil
}

func fromEnvvar() (Config, error) {
	return env.ParseAs[Config]()
}
