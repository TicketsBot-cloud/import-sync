package utils

import (
	"crypto/ed25519"
	"crypto/x509"
	"encoding/pem"
	"errors"

	"github.com/TicketsBot-cloud/archiverclient"
	"github.com/TicketsBot-cloud/common/observability"
	"github.com/TicketsBot-cloud/transcript-import-sync/internal/config"
	"github.com/minio/minio-go"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var ArchiverClient *archiverclient.ArchiverClient
var S3Client *minio.Client

func SetupLogger() (*zap.Logger, error) {
	var logger *zap.Logger
	var err error
	if config.Conf.JsonLogs {
		loggerConfig := zap.NewProductionConfig()
		loggerConfig.Level.SetLevel(config.Conf.LogLevel)

		logger, err = loggerConfig.Build(
			zap.AddCaller(),
			zap.AddStacktrace(zap.ErrorLevel),
			zap.WrapCore(observability.ZapSentryAdapter(observability.EnvironmentProduction)),
		)
	} else {
		loggerConfig := zap.NewDevelopmentConfig()
		loggerConfig.Level.SetLevel(config.Conf.LogLevel)
		loggerConfig.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder

		logger, err = loggerConfig.Build(zap.AddCaller(), zap.AddStacktrace(zap.ErrorLevel))
	}

	return logger, err
}

func GetV1PublicKey() (*ed25519.PublicKey, error) {
	publicKeyBlock, _ := pem.Decode([]byte(config.Conf.V1PublicKey))
	if publicKeyBlock == nil {
		return nil, errors.New("failed to decode public key")
	}

	parsedKey, err := x509.ParsePKIXPublicKey(publicKeyBlock.Bytes)
	if err != nil {
		return nil, err
	}

	decryptedPublicKey, ok := parsedKey.(ed25519.PublicKey)
	if !ok {
		return nil, errors.New("failed to convert public key")
	}

	return &decryptedPublicKey, nil
}
