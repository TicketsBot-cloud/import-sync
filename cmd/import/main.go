package main

import (
	"context"
	"fmt"

	"github.com/TicketsBot-cloud/archiverclient"
	configp "github.com/TicketsBot-cloud/transcript-import-sync/internal/config"
	"github.com/TicketsBot-cloud/transcript-import-sync/internal/daemon"
	"github.com/TicketsBot-cloud/transcript-import-sync/internal/database"
	"github.com/TicketsBot-cloud/transcript-import-sync/internal/log"
	"github.com/TicketsBot-cloud/transcript-import-sync/internal/utils"
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

	s3Client, err := minio.New(config.S3Import.Endpoint, config.S3Import.AccessKey, config.S3Import.SecretKey, true)
	if err != nil {
		logger.Fatal("Failed to connect to S3", zap.Error(err))
		return
	}

	logger.Info("S3 connected.")
	utils.S3Client = s3Client

	utils.ArchiverClient = archiverclient.NewArchiverClient(archiverclient.NewProxyRetriever(config.LogArchiver.Url), []byte(config.LogArchiver.Key))

	d := daemon.NewDaemon(config, database.Client, logger)

	if config.Daemon.Enabled {
		if err := d.Start(); err != nil {
			logger.Error("Failed to start daemon", zap.Error(err))
		}
	} else {
		ctx, cancel := context.WithTimeout(context.Background(), config.Daemon.ExecutionTimeout)
		defer cancel()

		if err := d.RunOnce(ctx); err != nil {
			logger.Error("Failed to run once", zap.Error(err))
		}
	}

}
