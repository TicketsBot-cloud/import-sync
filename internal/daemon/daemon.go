package daemon

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/TicketsBot-cloud/database"
	"github.com/TicketsBot-cloud/import-sync/internal/config"
	"github.com/TicketsBot-cloud/import-sync/internal/utils"
	"github.com/TicketsBot-cloud/import-sync/redis"
	"github.com/TicketsBot-cloud/import-sync/validator"
	"github.com/jackc/pgx/v4"
	"github.com/minio/minio-go"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type Daemon struct {
	config config.Config
	db     *database.Database
	logger *zap.Logger
	r      *redis.RedisClient
}

func NewDaemon(config config.Config, db *database.Database, logger *zap.Logger, r *redis.RedisClient) *Daemon {
	return &Daemon{
		config: config,
		db:     db,
		logger: logger,
		r:      r,
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
			switch d.config.Daemon.Type {
			case "TRANSCRIPT":
				if err := d.doTranscriptsRun(ctx, d.config.Daemon.ExecutionTimeout); err != nil {
					d.logger.Error("Failed to run", zap.Error(err))
				}
			case "DATA":
				if err := d.doDataRun(ctx, d.config.Daemon.ExecutionTimeout); err != nil {
					d.logger.Error("Failed to run", zap.Error(err))
				}
			default:
				d.logger.Fatal("Invalid daemon type")
			}

			d.logger.Info("Run completed", zap.Duration("duration", time.Since(start)))

			timer.Reset(d.config.Daemon.Frequency)
		case <-ctx.Done():
			d.logger.Info("Shutting down daemon")
			return nil

		}
	}
}

func (d *Daemon) doTranscriptsRun(ctx context.Context, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	return d.RunTranscriptsOnce(ctx)
}

func (d *Daemon) doDataRun(ctx context.Context, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	return d.RunDataOnce(ctx)
}

func (d *Daemon) RunTranscriptsOnce(ctx context.Context) error {
	doneCh := make(chan struct{})
	defer close(doneCh)

	objectCh := utils.S3Client.ListObjects(d.config.S3.Import.Bucket, "", true, doneCh)

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

	for object := range objectCh {
		if object.Err != nil {
			d.logger.Error("Failed to list object", zap.Error(object.Err))
			continue
		}

		d.logger.Info("Found object", zap.String("object", object.Key))

		guildId, err := strconv.ParseUint(strings.Replace(strings.Replace(object.Key, "transcripts/", "", 1), ".zip", "", 1), 10, 64)
		if err != nil {
			d.logger.Error("Failed to parse guild ID", zap.Error(err))
			// Delete file
			if err := utils.S3Client.RemoveObject(d.config.S3.Import.Bucket, object.Key); err != nil {
				d.logger.Error("Failed to delete object", zap.Error(err))
			}
			continue
		}

		// Check if guild is processing
		processing, err := d.r.IsGuildProcessing(ctx, guildId)
		if err != nil {
			d.logger.Error("Failed to check if guild is processing", zap.Error(err))
			continue
		}

		if processing {
			d.logger.Warn("Guild is already processing", zap.Uint64("guild", guildId))
			continue
		}

		// Download the file
		file, err := utils.S3Client.GetObject(d.config.S3.Import.Bucket, object.Key, minio.GetObjectOptions{})
		if err != nil {
			d.logger.Error("Failed to download object", zap.Error(err))
			continue
		}

		dataBuffer := bytes.NewBuffer(nil)
		if _, err := io.Copy(dataBuffer, file); err != nil {
			d.logger.Error("Failed to read object", zap.Error(err))
			continue
		}
		dataReader := bytes.NewReader(dataBuffer.Bytes())

		transcripts, err := v.ValidateGuildTranscripts(dataReader, object.Size)
		if err != nil {
			d.logger.Error("Failed to validate transcript", zap.Error(err))
			if err := utils.S3Client.RemoveObject(d.config.S3.Import.Bucket, object.Key); err != nil {
				d.logger.Error("Failed to delete object", zap.Error(err))
			}
			continue
		}

		if transcripts.GuildId != guildId {
			d.logger.Error("Guild ID mismatch", zap.Uint64("expected", guildId), zap.Uint64("actual", transcripts.GuildId))
			if err := utils.S3Client.RemoveObject(d.config.S3.Import.Bucket, object.Key); err != nil {
				d.logger.Error("Failed to delete object", zap.Error(err))
			}
			continue
		}

		d.logger.Info("Validated transcripts file", zap.Int("count", len(transcripts.Transcripts)))
		file.Close()

		// Get list of archived tickets for guild
		archiveObjs := utils.S3Client.ListObjects(d.config.S3.Archive.Bucket, strconv.FormatUint(guildId, 10), true, doneCh)
		existingTranscripts := []int{}

		// Mark guild as processing
		if _, err := d.r.MarkGuildProcessing(ctx, guildId); err != nil {
			d.logger.Error("Failed to mark guild as processing", zap.Error(err))
			continue
		}

		const runType = "TRANSCRIPT"

		runId, err := d.db.ImportLogs.CreateRun(ctx, guildId, runType)
		if err != nil {
			d.logger.Error("Failed to create run", zap.Error(err))
			continue
		}

		for archiveObj := range archiveObjs {
			if archiveObj.Err != nil {
				d.logger.Error("Failed to list archive object", zap.Error(archiveObj.Err))
			}

			ticketId, err := strconv.Atoi(strings.Replace(archiveObj.Key, strconv.FormatUint(guildId, 10)+"/", "", 1))
			if err != nil {
				d.logger.Error("Failed to parse ticket ID", zap.Error(err))
				continue
			}

			existingTranscripts = append(existingTranscripts, ticketId)
		}

		// Get mapping for guild
		mapping, err := d.db.ImportMappingTable.GetMapping(ctx, guildId)
		if err != nil {
			d.logger.Error("Failed to get mapping", zap.Error(err))
			continue
		}

		ticketMapping := mapping["ticket"]

		for ticketId, transcript := range transcripts.Transcripts {
			newTicketId := ticketMapping[ticketId]

			if newTicketId == 0 {
				d.logger.Warn("No mapping found for ticket", zap.Int("ticket", ticketId))
				continue
			}

			const maxRetries = 10
			var attempt int

			var exists bool

			// Check if the ticket already exists
			for _, existingTicketId := range existingTranscripts {
				if existingTicketId == newTicketId {
					exists = true
					break
				}
			}

			if exists {
				d.logger.Warn("Ticket already exists", zap.Uint64("guild", guildId), zap.Int("ticket", newTicketId))
				d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "SKIP", "transcript", fmt.Sprintf("Ticket %d already exists", newTicketId))
				continue
			}
			d.logger.Info("Inserting transcript", zap.Uint64("guild", guildId), zap.Int("ticket", newTicketId))

			for attempt = 1; attempt <= maxRetries; attempt++ {
				if err := utils.ArchiverClient.ImportTranscript(ctx, guildId, newTicketId, transcript); err != nil {
					d.logger.Error("Failed to import transcript", zap.Error(err), zap.Int("attempt", attempt))
					if attempt == maxRetries {
						d.logger.Error("Max retries reached. Giving up on importing transcript", zap.Error(err))
						d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "FAIL", "transcript", fmt.Sprintf("Failed to import transcript for ticket #%d after %d attempts", newTicketId, maxRetries))
						continue
					}
				} else {
					d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "SUCCESS", "transcript", fmt.Sprintf("Imported transcript for ticket #%d", newTicketId))
					break
				}
			}
		}

		d.logger.Info("Finished processing guild", zap.Uint64("guild", guildId))

		d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "RUN_COMPLETE", "transcripts", "Imported transcripts")

		// Delete transcripts object
		if err := utils.S3Client.RemoveObject(d.config.S3.Import.Bucket, object.Key); err != nil {
			d.logger.Error("Failed to delete object", zap.Error(err))
			continue
		}

		// Mark guild as processed
		if err := d.r.MarkGuildProcessed(ctx, guildId); err != nil {
			d.logger.Error("Failed to mark guild as processed", zap.Error(err))
			continue
		}
	}

	return nil
}

func (d *Daemon) RunDataOnce(ctx context.Context) error {
	doneCh := make(chan struct{})
	defer close(doneCh)

	objectCh := utils.S3Client.ListObjects(d.config.S3.Import.Bucket, "", true, doneCh)

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

	for object := range objectCh {

		if object.Err != nil {
			d.logger.Error("Failed to list object", zap.Error(object.Err))
			continue
		}

		d.logger.Info("Found object", zap.String("object", object.Key))

		guildId, err := strconv.ParseUint(strings.Replace(strings.Replace(object.Key, "data/", "", 1), ".zip", "", 1), 10, 64)
		if err != nil {
			d.logger.Error("Failed to parse guild ID", zap.Error(err))
			continue
		}

		// Check if guild is processing
		processing, err := d.r.IsGuildProcessing(ctx, guildId)
		if err != nil {
			d.logger.Error("Failed to check if guild is processing", zap.Error(err))
			continue
		}

		if processing {
			d.logger.Warn("Guild is already processing", zap.Uint64("guild", guildId))
			continue
		}

		// Download the file
		file, err := utils.S3Client.GetObject(d.config.S3.Import.Bucket, object.Key, minio.GetObjectOptions{})
		if err != nil {
			d.logger.Error("Failed to download object", zap.Error(err))
			continue
		}

		dataBuffer := bytes.NewBuffer(nil)
		if _, err := io.Copy(dataBuffer, file); err != nil {
			d.logger.Error("Failed to read object", zap.Error(err))
			continue
		}
		dataReader := bytes.NewReader(dataBuffer.Bytes())

		data, err := v.ValidateGuildData(dataReader, object.Size)
		if err != nil {
			d.logger.Error("Failed to validate data", zap.Error(err))
			continue
		}

		if data.GuildId != guildId {
			d.logger.Error("Guild ID mismatch", zap.Uint64("expected", guildId), zap.Uint64("actual", data.GuildId))
			continue
		}

		d.logger.Info("Validated data file", zap.Uint64("guild", guildId))

		// Get ticket maps
		mapping, err := d.db.ImportMappingTable.GetMapping(ctx, guildId)
		if err != nil {
			d.logger.Error("Failed to get mapping", zap.Error(err))
			continue
		}

		var (
			ticketIdMap    = mapping["ticket"]
			formIdMap      = mapping["form"]
			formInputIdMap = mapping["form_input"]
			panelIdMap     = mapping["panel"]
		)

		if ticketIdMap == nil {
			ticketIdMap = make(map[int]int)
		}

		if formIdMap == nil {
			formIdMap = make(map[int]int)
		}

		if formInputIdMap == nil {
			formInputIdMap = make(map[int]int)
		}

		if panelIdMap == nil {
			panelIdMap = make(map[int]int)
		}

		// Mark guild as processing
		if _, err := d.r.MarkGuildProcessing(ctx, guildId); err != nil {
			d.logger.Error("Failed to mark guild as processing", zap.Error(err))
			continue
		}

		const runType = "DATA"

		runId, err := d.db.ImportLogs.CreateRun(ctx, guildId, runType)
		if err != nil {
			d.logger.Error("Failed to create run", zap.Error(err))
			continue
		}

		if data.GuildIsGloballyBlacklisted {
			reason := "Blacklisted on v1"
			_ = d.db.ServerBlacklist.Add(ctx, guildId, &reason)
			d.logger.Info("Imported globally blacklisted", zap.Uint64("guild", guildId))
			continue
		}

		group, _ := errgroup.WithContext(ctx)

		// Import active language
		group.Go(func() (err error) {
			lang := "en"
			if data.ActiveLanguage != nil {
				lang = *data.ActiveLanguage
			}
			if err := d.db.ActiveLanguage.Set(ctx, guildId, lang); err != nil {
				d.logger.Error("Failed to import active language", zap.Uint64("guild", guildId), zap.Error(err))
				d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "FAIL", "language", "Failed to import active language")
			} else {
				d.logger.Info("Imported active language", zap.Uint64("guild", guildId), zap.String("language", lang))
				d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "SUCCESS", "language", "Imported active language")
			}

			return
		})

		// Import archive channel
		group.Go(func() (err error) {
			if data.ArchiveChannel != nil {
				if err := d.db.ArchiveChannel.Set(ctx, guildId, data.ArchiveChannel); err != nil {
					d.logger.Error("Failed to import archive channel", zap.Uint64("guild", guildId), zap.Error(err))
					d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "FAIL", "archive_channel", "Failed to import archive channel")
				} else {
					d.logger.Info("Imported archive channel", zap.Uint64("guild", guildId), zap.Uint64("channel", *data.ArchiveChannel))
					d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "SUCCESS", "archive_channel", "Imported archive channel")
				}
			}

			return
		})

		// import AutocloseSettings
		group.Go(func() (err error) {
			if data.AutocloseSettings != nil {
				if err := d.db.AutoClose.Set(ctx, guildId, *data.AutocloseSettings); err != nil {
					d.logger.Error("Failed to import autoclose settings", zap.Uint64("guild", guildId), zap.Error(err))
					d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "FAIL", "autoclose_settings", "Failed to import autoclose settings")
				} else {
					d.logger.Info("Imported autoclose settings", zap.Uint64("guild", guildId), zap.Bool("enabled", data.AutocloseSettings.Enabled))
					d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "SUCCESS", "autoclose_settings", "Imported autoclose settings")
				}
			}

			return
		})

		// Import blacklisted users
		group.Go(func() (err error) {
			failedUsers := 0
			for _, user := range data.GuildBlacklistedUsers {
				err = d.db.Blacklist.Add(ctx, guildId, user)
				d.logger.Info("Imported blacklisted user", zap.Uint64("guild", guildId), zap.Uint64("user", user))
				if err != nil {
					d.logger.Error("Failed to import blacklisted user", zap.Uint64("guild", guildId), zap.Uint64("user", user), zap.Error(err))
					failedUsers++
				}
			}

			if failedUsers == 0 {
				d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "SUCCESS", "blacklisted_users", "Imported blacklisted users")
			} else {
				d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "FAIL", "blacklisted_users", fmt.Sprintf("Failed to import %d blacklisted users", failedUsers))
			}

			return
		})

		// Import channel category
		group.Go(func() (err error) {
			if data.ChannelCategory != nil {
				if err := d.db.ChannelCategory.Set(ctx, guildId, *data.ChannelCategory); err != nil {
					d.logger.Error("Failed to import channel category", zap.Uint64("guild", guildId), zap.Error(err))
					d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "FAIL", "channel_category", "Failed to import channel category")
				} else {
					d.logger.Info("Imported channel category", zap.Uint64("guild", guildId), zap.Uint64("category", *data.ChannelCategory))
					d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "SUCCESS", "channel_category", "Imported channel category")
				}
			}

			return
		})

		// Import claim settings
		group.Go(func() (err error) {
			if data.ClaimSettings != nil {
				if err := d.db.ClaimSettings.Set(ctx, guildId, *data.ClaimSettings); err != nil {
					d.logger.Error("Failed to import claim settings", zap.Uint64("guild", guildId), zap.Error(err))
					d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "FAIL", "claim_settings", "Failed to import claim settings")
				} else {
					d.logger.Info("Imported claim settings", zap.Uint64("guild", guildId), zap.Any("settings", data.ClaimSettings))
					d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "SUCCESS", "claim_settings", "Imported claim settings")
				}
			}

			return
		})

		// Import close confirmation enabled
		group.Go(func() (err error) {
			if err := d.db.CloseConfirmation.Set(ctx, guildId, data.CloseConfirmationEnabled); err != nil {
				d.logger.Error("Failed to import close confirmation enabled", zap.Uint64("guild", guildId), zap.Error(err))
				d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "FAIL", "close_confirmation", "Failed to import close confirmation enabled")
			} else {
				d.logger.Info("Imported close confirmation enabled", zap.Uint64("guild", guildId), zap.Bool("enabled", data.CloseConfirmationEnabled))
				d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "SUCCESS", "close_confirmation", "Imported close confirmation enabled")
			}
			return
		})

		// Import custom colours
		group.Go(func() (err error) {
			failedColours := 0

			for k, v := range data.CustomColors {
				err = d.db.CustomColours.Set(ctx, guildId, k, v)
				d.logger.Info("Imported custom colour", zap.Uint64("guild", guildId), zap.Int16("key", k), zap.Int("value", v))
				if err != nil {
					d.logger.Error("Failed to import custom colour", zap.Uint64("guild", guildId), zap.Int16("key", k), zap.Int("value", v), zap.Error(err))
					failedColours++
				}
			}

			if failedColours == 0 {
				d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "SUCCESS", "custom_colours", "Imported custom colours")
			} else {
				d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "FAIL", "custom_colours", fmt.Sprintf("Failed to import %d custom colours", failedColours))
			}

			return
		})

		// Import feedback enabled
		group.Go(func() (err error) {
			if err := d.db.FeedbackEnabled.Set(ctx, guildId, data.FeedbackEnabled); err != nil {
				d.logger.Error("Failed to import feedback enabled", zap.Uint64("guild", guildId), zap.Error(err))
				d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "FAIL", "feedback_enabled", "Failed to import feedback enabled")
			} else {
				d.logger.Info("Imported feedback enabled", zap.Uint64("guild", guildId), zap.Bool("enabled", data.FeedbackEnabled))
				d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "SUCCESS", "feedback_enabled", "Imported feedback enabled")
			}
			return
		})

		// Import Guild Metadata
		group.Go(func() (err error) {
			if err := d.db.GuildMetadata.Set(ctx, guildId, data.GuildMetadata); err != nil {
				d.logger.Error("Failed to import guild metadata", zap.Uint64("guild", guildId), zap.Error(err))
				d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "FAIL", "guild_metadata", "Failed to import guild metadata")
			} else {
				d.logger.Info("Imported guild metadata", zap.Uint64("guild", guildId), zap.Any("metadata", data.GuildMetadata))
				d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "SUCCESS", "guild_metadata", "Imported guild metadata")
			}
			return
		})

		// Import Naming Scheme
		group.Go(func() (err error) {
			if data.NamingScheme != nil {
				if err := d.db.NamingScheme.Set(ctx, guildId, *data.NamingScheme); err != nil {
					d.logger.Error("Failed to import naming scheme", zap.Uint64("guild", guildId), zap.Error(err))
					d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "FAIL", "naming_scheme", "Failed to import naming scheme")
				} else {
					d.logger.Info("Imported naming scheme", zap.Uint64("guild", guildId), zap.Any("scheme", data.NamingScheme))
					d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "SUCCESS", "naming_scheme", "Imported naming scheme")
				}
			}

			return
		})

		// Import On Call Users
		group.Go(func() (err error) {

			failedUsers := 0

			for _, user := range data.OnCallUsers {
				if isOnCall, oncallerr := d.db.OnCall.IsOnCall(ctx, guildId, user); oncallerr != nil {
					return oncallerr
				} else if !isOnCall {
					if _, err := d.db.OnCall.Toggle(ctx, guildId, user); err != nil {
						d.logger.Error("Failed to import on call user", zap.Uint64("guild", guildId), zap.Uint64("user", user), zap.Error(err))
						failedUsers++
					}
				}
			}

			if failedUsers == 0 {
				d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "SUCCESS", "on_call_users", "Imported on call users")
			} else {
				d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "FAIL", "on_call_users", fmt.Sprintf("Failed to import %d on call users", failedUsers))
			}

			return
		})

		// Import User Permissions
		group.Go(func() (err error) {

			failedUsers := 0

			for _, perm := range data.UserPermissions {
				if perm.IsSupport {
					if err := d.db.Permissions.AddSupport(ctx, guildId, perm.Snowflake); err != nil {
						d.logger.Error("Failed to import user permission", zap.Uint64("guild", guildId), zap.Uint64("user", perm.Snowflake), zap.Error(err))
						failedUsers++
					}
				}

				if perm.IsAdmin {
					if err := d.db.Permissions.AddAdmin(ctx, guildId, perm.Snowflake); err != nil {
						d.logger.Error("Failed to import user permission", zap.Uint64("guild", guildId), zap.Uint64("user", perm.Snowflake), zap.Error(err))
						failedUsers++
					}
				}
			}

			if failedUsers == 0 {
				d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "SUCCESS", "user_permissions", "Imported user permissions")
			} else {
				d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "FAIL", "user_permissions", fmt.Sprintf("Failed to import %d user permissions", failedUsers))
			}

			return
		})

		// Import Guild Blacklisted Roles
		group.Go(func() (err error) {

			failedRoles := 0

			for _, role := range data.GuildBlacklistedRoles {
				if err := d.db.RoleBlacklist.Add(ctx, guildId, role); err != nil {
					d.logger.Error("Failed to import guild blacklisted role", zap.Uint64("guild", guildId), zap.Uint64("role", role), zap.Error(err))
					failedRoles++
				}
				d.logger.Info("Imported guild blacklisted role", zap.Uint64("guild", guildId), zap.Uint64("role", role))
			}

			if failedRoles == 0 {
				d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "SUCCESS", "guild_blacklisted_roles", "Imported guild blacklisted roles")
			} else {
				d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "FAIL", "guild_blacklisted_roles", fmt.Sprintf("Failed to import %d guild blacklisted roles", failedRoles))
			}

			return
		})

		// Import Role Permissions
		group.Go(func() (err error) {

			failedRoles := 0

			for _, perm := range data.RolePermissions {
				if perm.IsSupport {
					if err := d.db.RolePermissions.AddSupport(ctx, guildId, perm.Snowflake); err != nil {
						d.logger.Error("Failed to import role permission", zap.Uint64("guild", guildId), zap.Uint64("role", perm.Snowflake), zap.Error(err))
						failedRoles++
					}
				}

				if perm.IsAdmin {
					if err := d.db.RolePermissions.AddAdmin(ctx, guildId, perm.Snowflake); err != nil {
						d.logger.Error("Failed to import role permission", zap.Uint64("guild", guildId), zap.Uint64("role", perm.Snowflake), zap.Error(err))
						failedRoles++
					}
				}
			}

			if failedRoles == 0 {
				d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "SUCCESS", "role_permissions", "Imported role permissions")
			} else {
				d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "FAIL", "role_permissions", fmt.Sprintf("Failed to import %d role permissions", failedRoles))
			}

			return
		})

		// Import Tags
		group.Go(func() (err error) {

			failedTags := 0

			for _, tag := range data.Tags {
				if err := d.db.Tag.Set(ctx, tag); err != nil {
					d.logger.Error("Failed to import tag", zap.Uint64("guild", guildId), zap.String("name", tag.Id), zap.Error(err))
					failedTags++
				}
			}

			if failedTags == 0 {
				d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "SUCCESS", "tags", "Imported tags")
			} else {
				d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "FAIL", "tags", fmt.Sprintf("Failed to import %d tags", failedTags))
			}

			return
		})

		// Import Ticket Limit
		group.Go(func() (err error) {
			if data.TicketLimit != nil {
				if err := d.db.TicketLimit.Set(ctx, guildId, uint8(*data.TicketLimit)); err != nil {
					d.logger.Error("Failed to import ticket limit", zap.Uint64("guild", guildId), zap.Uint8("limit", uint8(*data.TicketLimit)), zap.Error(err))
					d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "FAIL", "ticket_limit", "Failed to import ticket limit")
				} else {
					d.logger.Info("Imported ticket limit", zap.Uint64("guild", guildId), zap.Uint8("limit", uint8(*data.TicketLimit)))
					d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "SUCCESS", "ticket_limit", "Imported ticket limit")
				}
			}

			return
		})

		// Import Ticket Permissions
		group.Go(func() (err error) {
			if err := d.db.TicketPermissions.Set(ctx, guildId, data.TicketPermissions); err != nil {
				d.logger.Error("Failed to import ticket permissions", zap.Uint64("guild", guildId), zap.Any("permissions", data.TicketPermissions), zap.Error(err))
				d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "FAIL", "ticket_permissions", "Failed to import ticket permissions")
			} else {
				d.logger.Info("Imported ticket permissions", zap.Uint64("guild", guildId), zap.Any("permissions", data.TicketPermissions))
				d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "SUCCESS", "ticket_permissions", "Imported ticket permissions")
			}

			return
		})

		// Import Users Can Close
		group.Go(func() (err error) {
			if err := d.db.UsersCanClose.Set(ctx, guildId, data.UsersCanClose); err != nil {
				d.logger.Error("Failed to import users can close", zap.Uint64("guild", guildId), zap.Bool("can_close", data.UsersCanClose), zap.Error(err))
				d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "FAIL", "users_can_close", "Failed to import users can close")
			} else {
				d.logger.Info("Imported users can close", zap.Uint64("guild", guildId), zap.Bool("can_close", data.UsersCanClose))
				d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "SUCCESS", "users_can_close", "Imported users can close")
			}

			return
		})

		// Import Welcome Message
		group.Go(func() (err error) {
			if data.WelcomeMessage != nil {
				if err := d.db.WelcomeMessages.Set(ctx, guildId, *data.WelcomeMessage); err != nil {
					d.logger.Error("Failed to import welcome message", zap.Uint64("guild", guildId), zap.String("message", *data.WelcomeMessage), zap.Error(err))
					d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "FAIL", "welcome_message", "Failed to import welcome message")
				} else {
					d.logger.Info("Imported welcome message", zap.Uint64("guild", guildId), zap.String("message", *data.WelcomeMessage))
					d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "SUCCESS", "welcome_message", "Imported welcome message")
				}
			}

			return
		})

		_ = group.Wait()

		supportTeamIdMap := make(map[int]int)

		failedSupportTeams := 0

		// Import Support Teams
		for _, team := range data.SupportTeams {
			teamId, err := d.db.SupportTeam.Create(ctx, guildId, fmt.Sprintf("%s (Imported)", team.Name))
			if err != nil {
				d.logger.Error("Failed to import support team", zap.Uint64("guild", guildId), zap.String("name", team.Name), zap.Error(err))
				failedSupportTeams++
			}

			supportTeamIdMap[team.Id] = teamId
		}

		if failedSupportTeams == 0 {
			d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "SUCCESS", "support_teams", "Imported support teams")
		} else {
			d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "FAIL", "support_teams", fmt.Sprintf("Failed to import %d support teams", failedSupportTeams))
		}

		failedSupportTeamUsers := 0

		// Import Support Team Users
		d.logger.Info("Importing support team users", zap.Uint64("guild", guildId))
		for teamId, users := range data.SupportTeamUsers {
			for _, user := range users {
				if err := d.db.SupportTeamMembers.Add(ctx, supportTeamIdMap[teamId], user); err != nil {
					d.logger.Error("Failed to import support team user", zap.Uint64("guild", guildId), zap.Uint64("user", user), zap.Error(err))
					failedSupportTeamUsers++
				}
			}
		}

		if failedSupportTeamUsers == 0 {
			d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "SUCCESS", "support_team_users", "Imported support team users")
		} else {
			d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "FAIL", "support_team_users", fmt.Sprintf("Failed to import %d support team users", failedSupportTeamUsers))
		}

		failedSupportTeamRoles := 0

		// Import Support Team Roles
		d.logger.Info("Importing support team roles", zap.Uint64("guild", guildId))
		for teamId, roles := range data.SupportTeamRoles {
			for _, role := range roles {
				if err := d.db.SupportTeamRoles.Add(ctx, supportTeamIdMap[teamId], role); err != nil {
					d.logger.Error("Failed to import support team role", zap.Uint64("guild", guildId), zap.Uint64("role", role), zap.Error(err))
					failedSupportTeamRoles++
				}
			}
		}

		if failedSupportTeamRoles == 0 {
			d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "SUCCESS", "support_team_roles", "Imported support team roles")
		} else {
			d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "FAIL", "support_team_roles", fmt.Sprintf("Failed to import %d support team roles", failedSupportTeamRoles))
		}

		// Import forms
		d.logger.Info("Importing forms", zap.Uint64("guild", guildId))
		failedForms := make([]int, 0)
		for _, form := range data.Forms {
			if _, ok := formIdMap[form.Id]; !ok {
				newCustomId, _ := utils.RandString(30)
				formId, err := d.db.Forms.Create(ctx, guildId, fmt.Sprintf("%s (Imported)", form.Title), newCustomId)
				if err != nil {
					d.logger.Error("Failed to import form", zap.Uint64("guild", guildId), zap.String("title", form.Title), zap.Error(err))
					failedForms = append(failedForms, form.Id)
				} else {
					formIdMap[form.Id] = formId
				}
			} else {
			}
		}

		if len(failedForms) == 0 {
			d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "SUCCESS", "forms", "Imported forms")
			d.logger.Info("Importing mapping for forms", zap.Uint64("guild", guildId))
			if err := d.db.ImportMappingTable.SetBulk(ctx, guildId, "form", formIdMap); err != nil {
				d.logger.Error("Failed to set mapping", zap.Error(err))
				continue
			}
		} else {
			d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "FAIL", "forms", fmt.Sprintf("Failed to import %d forms", len(failedForms)))
		}

		// Import form inputs
		d.logger.Info("Importing form inputs", zap.Uint64("guild", guildId))
		failedFormInputs := make([]int, 0)
		for _, input := range data.FormInputs {
			if failedForms != nil && utils.Contains(failedForms, input.FormId) {
				d.logger.Warn("Skipping form input due to missing form", zap.Uint64("guild", guildId), zap.Int("form", input.FormId))
				d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "SKIP", "form_input", fmt.Sprintf("Skipping form input due to missing form (Form: %d, ID: %d)", input.FormId, input.Id))
				continue
			}
			if _, ok := formInputIdMap[input.Id]; !ok {
				newCustomId, _ := utils.RandString(30)
				newInputId, err := d.db.FormInput.Create(ctx, formIdMap[input.FormId], newCustomId, input.Style, input.Label, input.Placeholder, input.Required, input.MinLength, input.MaxLength)
				if err != nil {
					d.logger.Error("Failed to import form input", zap.Uint64("guild", guildId), zap.Int("form", input.FormId), zap.Int("input", input.Id), zap.Error(err))
					failedFormInputs = append(failedFormInputs, input.Id)
				} else {
					formInputIdMap[input.Id] = newInputId
				}
			} else {
				d.logger.Warn("Skipping form input due to existing input", zap.Uint64("guild", guildId), zap.Int("form", input.FormId), zap.Int("input", input.Id))
				d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "SKIP", "form_input", fmt.Sprintf("Skipping form input due to existing input (Form: %d, ID: %d)", input.FormId, input.Id))
			}
		}

		if len(failedFormInputs) == 0 {
			d.logger.Info("Importing mapping for forms inputs", zap.Uint64("guild", guildId))
			if err := d.db.ImportMappingTable.SetBulk(ctx, guildId, "form_input", formInputIdMap); err != nil {
				d.logger.Error("Failed to set mapping", zap.Error(err))
				continue
			}
		}

		embedMap := make(map[int]int)

		// Import embeds
		d.logger.Info("Importing embeds", zap.Uint64("guild", guildId))
		failedEmbeds := make([]int, 0)
		for _, embed := range data.Embeds {
			var embedFields []database.EmbedField

			for _, field := range data.EmbedFields {
				if field.EmbedId == embed.Id {
					embedFields = append(embedFields, field)
				}
			}

			embed.GuildId = guildId

			embedId, err := d.db.Embeds.CreateWithFields(ctx, &embed, embedFields)
			if err != nil {
				d.logger.Error("Failed to import embed", zap.Uint64("guild", guildId), zap.Int("embed", embed.Id), zap.Error(err))
				failedEmbeds = append(failedEmbeds, embed.Id)
			} else {
				d.logger.Info("Imported embed", zap.Uint64("guild", guildId), zap.Int("embed", embed.Id), zap.Int("new_embed", embedId))
				embedMap[embed.Id] = embedId
			}
		}

		if len(failedEmbeds) == 0 {
			d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "SUCCESS", "embeds", "Imported embeds")
		} else {
			d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "FAIL", "embeds", fmt.Sprintf("Failed to import %d embeds", len(failedEmbeds)))
		}

		// Panel id map
		existingPanels, err := d.db.Panel.GetByGuild(ctx, guildId)
		if err != nil {
			d.logger.Error("Failed to get existing panels", zap.Error(err))
			continue
		}
		panelCount := len(existingPanels)

		panelTx, _ := d.db.Panel.BeginTx(ctx, pgx.TxOptions{})

		// Import Panels
		d.logger.Info("Importing panels", zap.Uint64("guild", guildId))
		failedPanels := make([]int, 0)
		for _, panel := range data.Panels {
			if _, ok := panelIdMap[panel.PanelId]; !ok {
				if panel.FormId != nil {
					if failedForms != nil && utils.Contains(failedForms, *panel.FormId) {
						d.logger.Warn("Skipping panel due to missing form", zap.Uint64("guild", guildId), zap.Int("panel", panel.PanelId))
						d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "SKIP", "panel", fmt.Sprintf("Skipping panel due to missing form (Panel: %d)", panel.PanelId))
						continue
					}
					newFormId := formIdMap[*panel.FormId]
					panel.FormId = &newFormId
				}

				if panel.ExitSurveyFormId != nil {
					newFormId := formIdMap[*panel.ExitSurveyFormId]
					panel.ExitSurveyFormId = &newFormId
				}

				if panel.WelcomeMessageEmbed != nil {
					if failedEmbeds == nil || !utils.Contains(failedEmbeds, *panel.WelcomeMessageEmbed) {
						newEmbedId := embedMap[*panel.WelcomeMessageEmbed]
						panel.WelcomeMessageEmbed = &newEmbedId
					}
				}

				panel.Title = fmt.Sprintf("%s (Imported)", panel.Title)

				// TODO: Fix this permanently
				panel.MessageId = panel.MessageId - 1
				newCustomId, _ := utils.RandString(30)
				panel.CustomId = newCustomId

				panelId, err := d.db.Panel.CreateWithTx(ctx, panelTx, panel)
				if err != nil {
					d.logger.Error("Failed to import panel", zap.Uint64("guild", guildId), zap.Int("panel", panel.PanelId), zap.Error(err))
					failedPanels = append(failedPanels, panel.PanelId)
				} else {
					d.logger.Info("Imported panel", zap.Uint64("guild", guildId), zap.Int("panel", panel.PanelId), zap.Int("new_panel", panelId))
					panelIdMap[panel.PanelId] = panelId
					panelCount++
				}
			}
		}

		if len(failedPanels) == 0 {
			d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "SUCCESS", "panels", "Imported panels")
		} else {
			d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "FAIL", "panels", fmt.Sprintf("Failed to import %d panels", len(failedPanels)))
		}

		if err := panelTx.Commit(ctx); err != nil {
			d.logger.Error("Failed to commit panel transaction", zap.Error(err))
			continue
		} else {
			d.logger.Info("Importing mapping for panels", zap.Uint64("guild", guildId))
			if err := d.db.ImportMappingTable.SetBulk(ctx, guildId, "panel", panelIdMap); err != nil {
				d.logger.Error("Failed to set mapping", zap.Error(err))
				continue
			}
		}

		// Import Panel Access Control Rules
		d.logger.Info("Importing panel access control rules", zap.Uint64("guild", guildId))
		failedPanelAccessControlRules := 0
		for panelId, rules := range data.PanelAccessControlRules {
			if len(failedPanels) > 0 && utils.Contains(failedPanels, panelId) {
				d.logger.Warn("Skipping panel access control rules due to missing panel", zap.Uint64("guild", guildId), zap.Int("panel", panelId))
				d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "SKIP", "panel_access_control_rules", fmt.Sprintf("Skipping panel access control rules due to missing panel (Panel: %d)", panelId))
				continue
			}
			if _, ok := panelIdMap[panelId]; ok {
				if err := d.db.PanelAccessControlRules.Replace(ctx, panelIdMap[panelId], rules); err != nil {
					d.logger.Error("Failed to import panel access control rules", zap.Uint64("guild", guildId), zap.Int("panel", panelId), zap.Error(err))
					failedPanelAccessControlRules++
				}
			} else {
				d.logger.Warn("Skipping panel access control rules due to missing panel", zap.Uint64("guild", guildId), zap.Int("panel", panelId))
				d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "SKIP", "panel_access_control_rules", fmt.Sprintf("Skipping panel access control rules due to missing panel (Panel: %d)", panelId))
			}
		}

		if failedPanelAccessControlRules == 0 {
			d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "SUCCESS", "panel_access_control_rules", "Imported panel access control rules")
		} else {
			d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "FAIL", "panel_access_control_rules", fmt.Sprintf("Failed to import %d panel access control rules", failedPanelAccessControlRules))
		}

		// Import Panel Mention User
		d.logger.Info("Importing panel mention user", zap.Uint64("guild", guildId))
		failedPanelMentionUser := 0
		for panelId, shouldMention := range data.PanelMentionUser {
			if len(failedPanels) > 0 && utils.Contains(failedPanels, panelId) {
				d.logger.Warn("Skipping panel mention user due to missing panel", zap.Uint64("guild", guildId), zap.Int("panel", panelId))
				d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "SKIP", "panel_mention_user", fmt.Sprintf("Skipping panel mention user due to missing panel (Panel: %d)", panelId))
				continue
			}
			if err := d.db.PanelUserMention.Set(ctx, panelIdMap[panelId], shouldMention); err != nil {
				d.logger.Error("Failed to import panel mention user", zap.Uint64("guild", guildId), zap.Int("panel", panelId), zap.Error(err))
				failedPanelMentionUser++
			}
		}

		if failedPanelMentionUser == 0 {
			d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "SUCCESS", "panel_mention_user", "Imported panel mention user")
		} else {
			d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "FAIL", "panel_mention_user", fmt.Sprintf("Failed to import %d panel mention user", failedPanelMentionUser))
		}

		// Import Panel Role Mentions
		failedPanelRoleMentions := 0
		d.logger.Info("Importing panel role mentions", zap.Uint64("guild", guildId))
		for panelId, roles := range data.PanelRoleMentions {
			if len(failedPanels) > 0 && utils.Contains(failedPanels, panelId) {
				d.logger.Warn("Skipping panel role mentions due to missing panel", zap.Uint64("guild", guildId), zap.Int("panel", panelId))
				d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "SKIP", "panel_role_mentions", fmt.Sprintf("Skipping panel role mentions due to missing panel (Panel: %d)", panelId))
				continue
			}
			if err := d.db.PanelRoleMentions.Replace(ctx, panelIdMap[panelId], roles); err != nil {
				d.logger.Error("Failed to import panel role mentions", zap.Uint64("guild", guildId), zap.Int("panel", panelId), zap.Error(err))
				failedPanelRoleMentions++
			}
		}

		if failedPanelRoleMentions == 0 {
			d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "SUCCESS", "panel_role_mentions", "Imported panel role mentions")
		} else {
			d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "FAIL", "panel_role_mentions", fmt.Sprintf("Failed to import %d panel role mentions", failedPanelRoleMentions))
		}

		// Import Panel Teams
		d.logger.Info("Importing panel teams", zap.Uint64("guild", guildId))
		failedPanelTeams := 0
		for panelId, teams := range data.PanelTeams {
			if len(failedPanels) > 0 && utils.Contains(failedPanels, panelId) {
				d.logger.Warn("Skipping panel teams due to missing panel", zap.Uint64("guild", guildId), zap.Int("panel", panelId))
				d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "SKIP", "panel_teams", fmt.Sprintf("Skipping panel teams due to missing panel (Panel: %d)", panelId))
				continue
			}
			teamsToAdd := make([]int, len(teams))
			for _, team := range teams {
				teamsToAdd = append(teamsToAdd, supportTeamIdMap[team])
			}

			if err := d.db.PanelTeams.Replace(ctx, panelIdMap[panelId], teamsToAdd); err != nil {
				d.logger.Error("Failed to import panel teams", zap.Uint64("guild", guildId), zap.Int("panel", panelId), zap.Error(err))
				failedPanelTeams++
			}
		}

		if failedPanelTeams == 0 {
			d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "SUCCESS", "panel_teams", "Imported panel teams")
		} else {
			d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "FAIL", "panel_teams", fmt.Sprintf("Failed to import %d panel teams", failedPanelTeams))
		}

		// Import Multi panels
		d.logger.Info("Importing multi panels", zap.Uint64("guild", guildId))
		multiPanelIdMap := make(map[int]int)
		failedMultiPanels := make([]int, 0)
		for _, multiPanel := range data.MultiPanels {
			multiPanelId, err := d.db.MultiPanels.Create(ctx, multiPanel)
			if err != nil {
				d.logger.Error("Failed to import multi panel", zap.Uint64("guild", guildId), zap.Int("multi_panel", multiPanel.Id), zap.Error(err))
				failedMultiPanels = append(failedMultiPanels, multiPanel.Id)
			} else {
				d.logger.Info("Imported multi panel", zap.Uint64("guild", guildId), zap.Int("multi_panel", multiPanel.Id), zap.Int("new_multi_panel", multiPanelId))
				multiPanelIdMap[multiPanel.Id] = multiPanelId
			}
		}

		if len(failedMultiPanels) == 0 {
			d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "SUCCESS", "multi_panels", "Imported multi panels")
		} else {
			d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "FAIL", "multi_panels", fmt.Sprintf("Failed to import %d multi panels", len(failedMultiPanels)))
		}

		// Import Multi Panel Targets
		d.logger.Info("Importing multi panel targets", zap.Uint64("guild", guildId))
		failedMultiPanelTargets := 0
		for multiPanelId, panelIds := range data.MultiPanelTargets {
			if len(failedMultiPanels) > 0 && utils.Contains(failedMultiPanels, multiPanelId) {
				d.logger.Warn("Skipping multi panel targets due to missing multi panel", zap.Uint64("guild", guildId), zap.Int("multi_panel", multiPanelId))
				d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "SKIP", "multi_panel_targets", fmt.Sprintf("Skipping multi panel targets due to missing multi panel (Multi Panel: %d)", multiPanelId))
				continue
			}
			for _, panelId := range panelIds {
				if len(failedPanels) > 0 && utils.Contains(failedPanels, panelId) {
					d.logger.Warn("Skipping multi panel target due to missing panel", zap.Uint64("guild", guildId), zap.Int("multi_panel", multiPanelId), zap.Int("panel", panelId))
					d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "SKIP", "multi_panel_targets", fmt.Sprintf("Skipping multi panel target due to missing panel (Multi Panel: %d, Panel: %d)", multiPanelId, panelId))
					continue
				}
				if err := d.db.MultiPanelTargets.Insert(ctx, multiPanelIdMap[multiPanelId], panelIdMap[panelId]); err != nil {
					failedMultiPanelTargets++
				}
			}
		}

		if failedMultiPanelTargets == 0 {
			d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "SUCCESS", "multi_panel_targets", "Imported multi panel targets")
		} else {
			d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "FAIL", "multi_panel_targets", fmt.Sprintf("Failed to import %d multi panel targets", failedMultiPanelTargets))
		}

		if data.Settings.ContextMenuPanel != nil {
			newContextMenuPanel := panelIdMap[*data.Settings.ContextMenuPanel]
			data.Settings.ContextMenuPanel = &newContextMenuPanel
		}

		// Import settings
		d.logger.Info("Importing settings", zap.Uint64("guild", guildId))
		if err := d.db.Settings.Set(ctx, guildId, data.Settings); err != nil {
			d.logger.Error("Failed to import settings", zap.Uint64("guild", guildId), zap.Error(err))
			d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "FAIL", "settings", "Failed to import settings")
		} else {
			d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "SUCCESS", "settings", "Imported settings")
		}

		ticketCount, err := d.db.Tickets.GetTotalTicketCount(ctx, guildId)
		if err != nil {
			d.logger.Error("Failed to get total ticket count", zap.Error(err))
			continue
		}
		ticketsToCreate := make([]database.Ticket, len(data.Tickets))
		ticketIdMapTwo := make(map[int]int)

		// Import tickets
		for i, ticket := range data.Tickets {
			if _, ok := ticketIdMap[ticket.Id]; !ok {
				d.logger.Info("Importing ticket", zap.Uint64("guild", guildId), zap.Int("ticket", ticket.Id), zap.Int("new_ticket", ticket.Id+ticketCount))
				var panelId *int
				if ticket.PanelId != nil {
					a := panelIdMap[*ticket.PanelId]
					panelId = &a
				}
				ticketsToCreate[i] = database.Ticket{
					Id:               ticket.Id + ticketCount,
					GuildId:          guildId,
					ChannelId:        ticket.ChannelId,
					UserId:           ticket.UserId,
					Open:             ticket.Open,
					OpenTime:         ticket.OpenTime,
					WelcomeMessageId: ticket.WelcomeMessageId,
					PanelId:          panelId,
					HasTranscript:    ticket.HasTranscript,
					CloseTime:        ticket.CloseTime,
					IsThread:         ticket.IsThread,
					JoinMessageId:    ticket.JoinMessageId,
					NotesThreadId:    ticket.NotesThreadId,
				}

				ticketIdMapTwo[ticket.Id] = ticket.Id + ticketCount
			} else {
				d.logger.Warn("Skipping ticket due to existing ticket", zap.Uint64("guild", guildId), zap.Int("ticket", ticket.Id))
				d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "SKIP", "ticket", fmt.Sprintf("Skipping ticket due to existing ticket (Ticket: %d)", ticket.Id))
			}
		}

		d.logger.Info("Importing tickets", zap.Uint64("guild", guildId))
		if err := d.db.Tickets.BulkImport(ctx, guildId, ticketsToCreate); err != nil {
			d.logger.Error("Failed to import tickets", zap.Uint64("guild", guildId), zap.Error(err))
			d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "FAIL", "tickets", "Failed to import tickets")
		} else {
			d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "SUCCESS", "tickets", "Imported tickets")
			d.logger.Info("Importing mapping for tickets", zap.Uint64("guild", guildId))
			if err := d.db.ImportMappingTable.SetBulk(ctx, guildId, "ticket", ticketIdMap); err != nil {
				d.logger.Error("Failed to set mapping", zap.Error(err))
				continue
			}
		}

		ticketsExtrasGroup, _ := errgroup.WithContext(ctx)

		ticketsExtrasGroup.Go(func() (err error) {
			d.logger.Info("Importing ticket members", zap.Uint64("guild", guildId))
			newMembersMap := make(map[int][]uint64)
			for ticketId, members := range data.TicketAdditionalMembers {
				if _, ok := ticketIdMap[ticketId]; !ok {
					continue
				}

				newMembersMap[ticketIdMap[ticketId]] = members
			}

			if err := d.db.TicketMembers.ImportBulk(ctx, guildId, newMembersMap); err != nil {
				d.logger.Error("Failed to import ticket members", zap.Uint64("guild", guildId), zap.Error(err))
				d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "FAIL", "ticket_members", "Failed to import ticket members")
			} else {
				d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "SUCCESS", "ticket_members", "Imported ticket members")
			}

			return
		})

		// Import ticket last messages
		ticketsExtrasGroup.Go(func() (err error) {
			d.logger.Info("Importing ticket last messages", zap.Uint64("guild", guildId))
			msgs := map[int]database.TicketLastMessage{}
			for _, msg := range data.TicketLastMessages {
				if _, ok := ticketIdMap[msg.TicketId]; !ok {
					continue
				}

				msgs[ticketIdMap[msg.TicketId]] = database.TicketLastMessage{
					LastMessageId:   msg.Data.LastMessageId,
					LastMessageTime: msg.Data.LastMessageTime,
					UserId:          msg.Data.UserId,
					UserIsStaff:     msg.Data.UserIsStaff,
				}
			}

			if err := d.db.TicketLastMessage.ImportBulk(ctx, guildId, msgs); err != nil {
				d.logger.Error("Failed to import ticket last messages", zap.Uint64("guild", guildId), zap.Error(err))
				d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "FAIL", "ticket_last_messages", "Failed to import ticket last messages")
			} else {
				d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "SUCCESS", "ticket_last_messages", "Imported ticket last messages")
			}
			return
		})

		ticketsExtrasGroup.Go(func() (err error) {
			d.logger.Info("Importing ticket claims", zap.Uint64("guild", guildId))
			newClaimsMap := make(map[int]uint64)
			for ticketId, user := range data.TicketClaims {
				if _, ok := ticketIdMap[ticketId]; !ok {
					continue
				}
				newClaimsMap[ticketIdMap[ticketId]] = user.Data
			}

			if err := d.db.TicketClaims.ImportBulk(ctx, guildId, newClaimsMap); err != nil {
				d.logger.Error("Failed to import ticket claims", zap.Uint64("guild", guildId), zap.Error(err))
				d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "FAIL", "ticket_claims", "Failed to import ticket claims")
			} else {
				d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "SUCCESS", "ticket_claims", "Imported ticket claims")
			}
			return
		})

		// Import ticket ratings
		ticketsExtrasGroup.Go(func() (err error) {
			d.logger.Info("Importing ticket ratings", zap.Uint64("guild", guildId))
			newRatingsMap := make(map[int]uint8)
			for ticketId, rating := range data.ServiceRatings {
				if _, ok := ticketIdMapTwo[ticketId]; !ok {
					continue
				}
				newRatingsMap[ticketIdMapTwo[ticketId]] = uint8(rating.Data)
			}

			if err := d.db.ServiceRatings.ImportBulk(ctx, guildId, newRatingsMap); err != nil {
				d.logger.Error("Failed to import ticket ratings", zap.Uint64("guild", guildId), zap.Error(err))
				d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "FAIL", "ticket_ratings", "Failed to import ticket ratings")
			} else {
				d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "SUCCESS", "ticket_ratings", "Imported ticket ratings")
			}
			return
		})

		// Import participants
		ticketsExtrasGroup.Go(func() (err error) {
			d.logger.Info("Importing ticket participants", zap.Uint64("guild", guildId))
			newParticipantsMap := make(map[int][]uint64)
			for ticketId, participants := range data.Participants {
				if _, ok := ticketIdMapTwo[ticketId]; !ok {
					continue
				}
				newParticipantsMap[ticketIdMapTwo[ticketId]] = participants
			}

			if err := d.db.Participants.ImportBulk(ctx, guildId, newParticipantsMap); err != nil {
				d.logger.Error("Failed to import ticket participants", zap.Uint64("guild", guildId), zap.Error(err))
				d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "FAIL", "ticket_participants", "Failed to import ticket participants")
			} else {
				d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "SUCCESS", "ticket_participants", "Imported ticket participants")
			}
			return
		})

		// Import First Response Times
		ticketsExtrasGroup.Go(func() (err error) {
			failedFirstResponseTimes := 0
			d.logger.Info("Importing first response times", zap.Uint64("guild", guildId))
			for _, frt := range data.FirstResponseTimes {
				if _, ok := ticketIdMapTwo[frt.TicketId]; !ok {
					continue
				}
				if err := d.db.FirstResponseTime.Set(ctx, guildId, frt.UserId, ticketIdMapTwo[frt.TicketId], frt.ResponseTime); err != nil {
					d.logger.Error("Failed to import first response time", zap.Uint64("guild", guildId), zap.Uint64("user", frt.UserId), zap.Int("ticket", frt.TicketId), zap.Error(err))
					failedFirstResponseTimes++
				}
			}

			if failedFirstResponseTimes == 0 {
				d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "SUCCESS", "first_response_times", "Imported first response times")
			} else {
				d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "FAIL", "first_response_times", fmt.Sprintf("Failed to import %d first response times", failedFirstResponseTimes))
			}
			return
		})

		ticketsExtrasGroup.Go(func() (err error) {
			d.logger.Info("Importing ticket survey responses", zap.Uint64("guild", guildId))
			failedSurveyResponses := 0
			for _, response := range data.ExitSurveyResponses {
				if _, ok := ticketIdMapTwo[response.TicketId]; !ok {
					continue
				}
				resps := map[int]string{
					*response.Data.QuestionId: *response.Data.Response,
				}

				if err := d.db.ExitSurveyResponses.AddResponses(ctx, guildId, ticketIdMapTwo[response.TicketId], formIdMap[*response.Data.FormId], resps); err != nil {
					d.logger.Error("Failed to import survey response", zap.Uint64("guild", guildId), zap.Int("ticket", response.TicketId), zap.Error(err))
					failedSurveyResponses++
				}
			}

			if failedSurveyResponses == 0 {
				d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "SUCCESS", "exit_survey_responses", "Imported exit survey responses")
			} else {
				d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "FAIL", "exit_survey_responses", fmt.Sprintf("Failed to import %d exit survey responses", failedSurveyResponses))
			}
			return
		})

		// Import Close Reasons
		ticketsExtrasGroup.Go(func() (err error) {
			d.logger.Info("Importing close reasons", zap.Uint64("guild", guildId))
			failedCloseReasons := 0
			for _, reason := range data.CloseReasons {
				if _, ok := ticketIdMapTwo[reason.TicketId]; !ok {
					continue
				}
				if err := d.db.CloseReason.Set(ctx, guildId, ticketIdMapTwo[reason.TicketId], reason.Data); err != nil {
					d.logger.Error("Failed to import close reason", zap.Uint64("guild", guildId), zap.Int("ticket", reason.TicketId), zap.Error(err))
					failedCloseReasons++
				}
			}

			if failedCloseReasons == 0 {
				d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "SUCCESS", "close_reasons", "Imported close reasons")
			} else {
				d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "FAIL", "close_reasons", fmt.Sprintf("Failed to import %d close reasons", failedCloseReasons))
			}
			return
		})

		// Import Autoclose Excluded Tickets
		ticketsExtrasGroup.Go(func() (err error) {
			failedAutocloseExcluded := 0
			d.logger.Info("Importing autoclose excluded tickets", zap.Uint64("guild", guildId))
			for _, ticketId := range data.AutocloseExcluded {
				if _, ok := ticketIdMapTwo[ticketId]; !ok {
					continue
				}
				if err := d.db.AutoCloseExclude.Exclude(ctx, guildId, ticketIdMapTwo[ticketId]); err != nil {
					d.logger.Error("Failed to import autoclose excluded ticket", zap.Uint64("guild", guildId), zap.Int("ticket", ticketId), zap.Error(err))
					failedAutocloseExcluded++
				}
			}

			if failedAutocloseExcluded == 0 {
				d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "SUCCESS", "autoclose_excluded_tickets", "Imported autoclose excluded tickets")
			} else {
				d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "FAIL", "autoclose_excluded_tickets", fmt.Sprintf("Failed to import %d autoclose excluded tickets", failedAutocloseExcluded))
			}
			return
		})

		// Import Archive Messages
		ticketsExtrasGroup.Go(func() (err error) {
			d.logger.Info("Importing archive messages", zap.Uint64("guild", guildId))
			failedArchiveMessages := 0
			for _, message := range data.ArchiveMessages {
				if _, ok := ticketIdMapTwo[message.TicketId]; !ok {
					continue
				}
				if err := d.db.ArchiveMessages.Set(ctx, guildId, ticketIdMapTwo[message.TicketId], message.Data.ChannelId, message.Data.MessageId); err != nil {
					d.logger.Error("Failed to import archive message", zap.Uint64("guild", guildId), zap.Int("ticket", message.TicketId), zap.Error(err))
					failedArchiveMessages++
				}
			}

			if failedArchiveMessages == 0 {
				d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "SUCCESS", "archive_messages", "Imported archive messages")
			} else {
				d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "FAIL", "archive_messages", fmt.Sprintf("Failed to import %d archive messages", failedArchiveMessages))
			}
			return
		})

		_ = ticketsExtrasGroup.Wait()

		d.logger.Info("Finished processing guild", zap.Uint64("guild", guildId))
		d.db.ImportLogs.AddLog(ctx, guildId, runId, runType, "RUN_COMPLETE", "guild", "Guild import run complete")

		// Delete object
		if err := utils.S3Client.RemoveObject(d.config.S3.Import.Bucket, object.Key); err != nil {
			d.logger.Error("Failed to delete object", zap.Error(err))
			continue
		}

		// Mark the run as complete
		if err := d.r.MarkGuildProcessed(ctx, guildId); err != nil {
			d.logger.Error("Failed to mark guild as processed", zap.Uint64("guild", guildId), zap.Error(err))
			continue
		}

	}

	return nil
}
