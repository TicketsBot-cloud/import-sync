package validator

import "errors"

var (
	ErrValidationFailed    = errors.New("validation failed")
	ErrMaximumSizeExceeded = errors.New("maximum size exceeded")
)
