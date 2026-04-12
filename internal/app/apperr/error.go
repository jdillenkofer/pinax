package apperr

import "fmt"

type Kind string

const (
	KindValidation Kind = "validation"
	KindConflict   Kind = "conflict"
	KindNotFound   Kind = "not_found"
)

type Error struct {
	Kind    Kind
	Message string
}

func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("%s: %s", e.Kind, e.Message)
}

func New(kind Kind, message string) error {
	return &Error{Kind: kind, Message: message}
}

func Validation(message string) error {
	return New(KindValidation, message)
}

func Conflict(message string) error {
	return New(KindConflict, message)
}

func NotFound(message string) error {
	return New(KindNotFound, message)
}
