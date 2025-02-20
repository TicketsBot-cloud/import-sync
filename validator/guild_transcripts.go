package validator

import (
	"archive/zip"
	"io"
	"regexp"
	"strconv"
)

type GuildTranscriptsOutput struct {
	GuildId uint64
	// Ticket ID -> Transcript
	Transcripts map[int][]byte
}

var transcriptFileRegex = regexp.MustCompile(`^transcripts/(\d+)\.json$`)

func (v *Validator) ValidateGuildTranscripts(input io.ReaderAt, size int64) (*GuildTranscriptsOutput, error) {
	reader, err := zip.NewReader(input, size)
	if err != nil {
		return nil, err
	}

	guildId, n, err := v.readGuildId(reader)
	if err != nil {
		return nil, err
	}

	transcripts := make(map[int][]byte)
	for _, f := range reader.File {
		matches := transcriptFileRegex.FindStringSubmatch(f.Name)
		if len(matches) != 2 {
			continue
		}

		ticketId, err := strconv.Atoi(matches[1])
		if err != nil {
			continue
		}

		file, err := f.Open()
		if err != nil {
			return nil, err
		}

		b, err := io.ReadAll(v.newLimitReader(file))
		if err != nil {
			return nil, err
		}

		n += int64(len(b))
		if n > v.maxUncompressedSize {
			return nil, ErrMaximumSizeExceeded
		}

		sigSize, err := v.validateTranscriptSignature(reader, f.Name, guildId, ticketId, b)
		if err != nil {
			return nil, err
		}

		n += sigSize
		if n > v.maxUncompressedSize {
			return nil, ErrMaximumSizeExceeded
		}

		transcripts[ticketId] = b
	}

	return &GuildTranscriptsOutput{
		GuildId:     guildId,
		Transcripts: transcripts,
	}, nil
}

func (v *Validator) readGuildId(reader *zip.Reader) (uint64, int64, error) {
	f, err := reader.Open("guild_id.txt")
	if err != nil {
		return 0, 0, err
	}

	b, err := io.ReadAll(v.newLimitReader(f))
	if err != nil {
		return 0, 0, err
	}

	guildId, err := strconv.ParseUint(string(b), 10, 64)
	if err != nil {
		return 0, 0, err
	}

	sigSize, err := v.validateSignature(reader, "guild_id.txt", b)
	if err != nil {
		return 0, 0, err
	}

	return guildId, int64(len(b)) + sigSize, nil
}
