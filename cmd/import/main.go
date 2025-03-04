package main

import (
	"context"
	"fmt"

	"github.com/TicketsBot-cloud/archiverclient"
	configp "github.com/TicketsBot-cloud/import-sync/internal/config"
	"github.com/TicketsBot-cloud/import-sync/internal/daemon"
	"github.com/TicketsBot-cloud/import-sync/internal/database"
	"github.com/TicketsBot-cloud/import-sync/internal/log"
	"github.com/TicketsBot-cloud/import-sync/internal/utils"
	"github.com/TicketsBot-cloud/import-sync/redis"
	"github.com/getsentry/sentry-go"
	"github.com/minio/minio-go"
	"go.uber.org/zap"

	_ "github.com/joho/godotenv/autoload"
)

func main() {
	config, err := configp.LoadConfig()
	if err != nil {
		panic(err)
	}

	configp.Conf = config

	// Build logger
	if len(config.SentryDsn) > 0 {
		if err := sentry.Init(sentry.ClientOptions{
			Dsn: config.SentryDsn,
		}); err != nil {
			panic(fmt.Errorf("sentry.Init: %w", err))
		}
	}

	// Configure logger
	logger, err := utils.SetupLogger()
	if err != nil {
		panic(err)
	}

	log.Logger = logger

	logger.Info("Connecting to database...")
	database.ConnectToDatabase()

	logger.Info("Database connected.")

	s3Client, err := minio.New(config.S3.Endpoint, config.S3.AccessKey, config.S3.SecretKey, config.S3.Secure)
	if err != nil {
		logger.Fatal("Failed to connect to S3", zap.Error(err))
		return
	}

	logger.Info("S3 connected.")
	utils.S3Client = s3Client

	redis.Client = redis.NewRedisClient()

	utils.ArchiverClient = archiverclient.NewArchiverClient(archiverclient.NewProxyRetriever(config.LogArchiver.Url), []byte(config.LogArchiver.Key))

	d := daemon.NewDaemon(config, database.Client, logger, redis.Client)

	if config.Daemon.Enabled {
		if err := d.Start(); err != nil {
			logger.Error("Failed to start daemon", zap.Error(err))
		}
	} else {
		ctx, _ := context.WithTimeout(context.Background(), config.Daemon.ExecutionTimeout)
		// defer cancel()

		switch config.Daemon.Type {
		case "TRANSCRIPT":
			if err := d.RunTranscriptsOnce(ctx); err != nil {
				logger.Error("Failed to run once", zap.Error(err))
			}
		case "DATA":
			if err := d.RunDataOnce(ctx); err != nil {
				logger.Error("Failed to run once", zap.Error(err))
			}
		}
	}

}
