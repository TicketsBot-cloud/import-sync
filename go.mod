module github.com/TicketsBot-cloud/transcript-import-sync

go 1.22.6

// replace github.com/TicketsBot-cloud/database => ../database

// replace github.com/TicketsBot-cloud/common => ../common

require (
	github.com/BurntSushi/toml v1.4.0
	github.com/TicketsBot-cloud/archiverclient v0.0.0-20250206203822-d4f91573ad70
	github.com/TicketsBot-cloud/common v0.0.0-20250208140430-b5da1dd487b3
	github.com/TicketsBot-cloud/database v0.0.0-20250215204312-d433be0833c9
	github.com/TicketsBot/export v0.0.0-20250210204456-b8c76fd55d96
	github.com/caarlos0/env/v11 v11.3.1
	github.com/getsentry/sentry-go v0.31.1
	github.com/jackc/pgconn v1.14.3
	github.com/jackc/pgx/v4 v4.18.3
	github.com/joho/godotenv v1.5.1
	github.com/minio/minio-go v6.0.14+incompatible
	github.com/sirupsen/logrus v1.9.3
	go.uber.org/zap v1.27.0
)

require (
	github.com/TicketsBot/common v0.0.0-20241117150316-ff54c97b45c1 // indirect
	github.com/TicketsBot/database v0.0.0-20250205194156-c8239ae6eb4e // indirect
	github.com/TicketsBot/logarchiver v0.0.0-20241116233207-0cfab8ec82cf // indirect
	github.com/caarlos0/env v3.5.0+incompatible // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/go-ini/ini v1.67.0 // indirect
	github.com/goccy/go-json v0.10.5 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/jackc/chunkreader/v2 v2.0.1 // indirect
	github.com/jackc/pgio v1.0.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgproto3/v2 v2.3.3 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/pgtype v1.14.4 // indirect
	github.com/jackc/pgx v3.6.2+incompatible // indirect
	github.com/jackc/pgx/v5 v5.7.2 // indirect
	github.com/jackc/puddle v1.3.0 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/klauspost/compress v1.17.11 // indirect
	github.com/klauspost/cpuid/v2 v2.2.9 // indirect
	github.com/minio/crc64nvme v1.0.1 // indirect
	github.com/minio/md5-simd v1.1.2 // indirect
	github.com/minio/minio-go/v7 v7.0.86 // indirect
	github.com/mitchellh/go-homedir v1.1.0 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/rs/xid v1.6.0 // indirect
	github.com/rxdn/gdl v0.0.0-20241201120412-8fd61c53dd96 // indirect
	go.uber.org/multierr v1.10.0 // indirect
	golang.org/x/crypto v0.33.0 // indirect
	golang.org/x/net v0.35.0 // indirect
	golang.org/x/sync v0.11.0 // indirect
	golang.org/x/sys v0.30.0 // indirect
	golang.org/x/text v0.22.0 // indirect
)
