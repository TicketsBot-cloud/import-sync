package validator

import (
	"crypto/ed25519"
	"io"
)

type Validator struct {
	publicKey ed25519.PublicKey

	maxUncompressedSize   int64
	maxIndividualFileSize int64
}

type Option func(*Validator)

func NewValidator(publicKey ed25519.PublicKey, options ...Option) *Validator {
	v := &Validator{
		publicKey:             publicKey,
		maxUncompressedSize:   250 * 1024 * 1024,
		maxIndividualFileSize: 1 * 1024 * 1024,
	}

	for _, option := range options {
		option(v)
	}

	return v
}

func WithMaxUncompressedSize(size int64) Option {
	return func(v *Validator) {
		v.maxUncompressedSize = size
	}
}

func WithMaxIndividualFileSize(size int64) Option {
	return func(v *Validator) {
		v.maxIndividualFileSize = size
	}
}

func (v *Validator) newLimitReader(r io.Reader) io.Reader {
	return io.LimitReader(r, v.maxIndividualFileSize)
}
