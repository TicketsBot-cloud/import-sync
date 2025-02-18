package daemon

import (
	"bytes"
	"context"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/TicketsBot-cloud/database"
	"github.com/TicketsBot-cloud/transcript-import-sync/internal/config"
	"github.com/TicketsBot-cloud/transcript-import-sync/internal/utils"
	"github.com/TicketsBot/export/pkg/validator"
	"github.com/minio/minio-go"
	"go.uber.org/zap"
)

type Daemon struct {
	config config.Config
	db     *database.Database
	logger *zap.Logger
}

func NewDaemon(config config.Config, db *database.Database, logger *zap.Logger) *Daemon {
	return &Daemon{
		config: config,
		db:     db,
		logger: logger,
	}
}

func (d *Daemon) Start() error {
	d.logger.Info("Starting daemon")
	ctx := context.Background()

	timer := time.NewTimer(d.config.Daemon.Frequency)
	defer timer.Stop()

	for {
		select {
		case <-timer.C:
			start := time.Now()
			if err := d.doRun(ctx, d.config.Daemon.ExecutionTimeout); err != nil {
				d.logger.Error("Failed to run", zap.Error(err))
			}

			d.logger.Info("Run completed", zap.Duration("duration", time.Since(start)))

			timer.Reset(d.config.Daemon.Frequency)
		case <-ctx.Done():
			d.logger.Info("Shutting down daemon")
			return nil

		}
	}
}

func (d *Daemon) doRun(ctx context.Context, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	return d.RunOnce(ctx)
}

func (d *Daemon) RunOnce(ctx context.Context) error {
	doneCh := make(chan struct{})
	defer close(doneCh)

	objectCh := utils.S3Client.ListObjects(d.config.S3Import.Bucket, "", true, doneCh)

	v1PublicKey, err := utils.GetV1PublicKey()
	if err != nil {
		d.logger.Error("Failed to get public key", zap.Error(err))
		return err
	}

	v := validator.NewValidator(
		*v1PublicKey,
		validator.WithMaxUncompressedSize(1024*1024*1024),
		validator.WithMaxIndividualFileSize(100*1024*1024),
	)

	var wg sync.WaitGroup

	for object := range objectCh {
		wg.Add(1)

		go func() {
			defer wg.Done()

			if object.Err != nil {
				d.logger.Error("Failed to list object", zap.Error(object.Err))
				return
			}

			d.logger.Info("Found object", zap.String("object", object.Key))

			guildId, err := strconv.ParseUint(strings.Replace(strings.Replace(object.Key, "transcripts/", "", 1), ".zip", "", 1), 10, 64)
			if err != nil {
				d.logger.Error("Failed to parse guild ID", zap.Error(err))
				return
			}

			// Download the file
			file, err := utils.S3Client.GetObject(d.config.S3Import.Bucket, object.Key, minio.GetObjectOptions{})
			if err != nil {
				d.logger.Error("Failed to download object", zap.Error(err))
				return
			}

			dataBuffer := bytes.NewBuffer(nil)
			if _, err := io.Copy(dataBuffer, file); err != nil {
				d.logger.Error("Failed to read object", zap.Error(err))
				return
			}
			dataReader := bytes.NewReader(dataBuffer.Bytes())

			transcripts, err := v.ValidateGuildTranscripts(dataReader, object.Size)
			if err != nil {
				d.logger.Error("Failed to validate transcript", zap.Error(err))
				return
			}

			if transcripts.GuildId != guildId {
				d.logger.Error("Guild ID mismatch", zap.Uint64("expected", guildId), zap.Uint64("actual", transcripts.GuildId))
				return
			}

			d.logger.Info("Validated transcripts file", zap.Int("count", len(transcripts.Transcripts)))

			// Get mapping for guild
			mapping, err := d.db.ImportMappingTable.GetMapping(ctx, guildId)
			if err != nil {
				d.logger.Error("Failed to get mapping", zap.Error(err))
				return
			}

			ticketMapping := mapping["ticket"]

			for ticketId, transcript := range transcripts.Transcripts {
				newTicketId := ticketMapping[ticketId]

				if newTicketId == 0 {
					d.logger.Warn("No mapping found for ticket", zap.Int("ticket", ticketId))
					return
				}

				d.logger.Info("Inserting transcript", zap.Uint64("guild", guildId), zap.Int("ticket", newTicketId))
				if err := utils.ArchiverClient.ImportTranscript(ctx, guildId, newTicketId, transcript); err != nil {
					d.logger.Error("Failed to import transcript", zap.Error(err))
					return
				}
			}

			d.logger.Info("Finished processing guild", zap.Uint64("guild", guildId))

			// Delete transcripts object
			if err := utils.S3Client.RemoveObject(d.config.S3Import.Bucket, object.Key); err != nil {
				d.logger.Error("Failed to delete object", zap.Error(err))
				return
			}
		}()
	}

	wg.Wait()

	return nil
}
