package errors

import "errors"

var (
	ErrNotFoundLeader    = errors.New("does not found leader")
	ErrNodeAlreadyExists = errors.New("node already exists")
	ErrNodeNotReady      = errors.New("node not ready")
	ErrNotFound          = errors.New("not found")
	ErrTimeout           = errors.New("timeout")
)
