package awserr

import (
	"encoding/json"
	"errors"
	"net/http"
)

const prefix = "com.amazonaws.dynamodb.v20120810#"

type APIError struct {
	Code    string
	Message string
	Status  int
}

func (e *APIError) Error() string { return e.Code + ": " + e.Message }

func Validation(msg string) *APIError {
	return &APIError{Code: "ValidationException", Message: msg, Status: http.StatusBadRequest}
}

func ResourceNotFound(msg string) *APIError {
	return &APIError{Code: "ResourceNotFoundException", Message: msg, Status: http.StatusBadRequest}
}

func ResourceInUse(msg string) *APIError {
	return &APIError{Code: "ResourceInUseException", Message: msg, Status: http.StatusBadRequest}
}

func ConditionalCheckFailed(msg string) *APIError {
	return &APIError{Code: "ConditionalCheckFailedException", Message: msg, Status: http.StatusBadRequest}
}

func Internal(msg string) *APIError {
	return &APIError{Code: "InternalServerError", Message: msg, Status: http.StatusInternalServerError}
}

func Write(w http.ResponseWriter, err error) {
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		apiErr = Internal(err.Error())
	}

	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	w.WriteHeader(apiErr.Status)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"__type":  prefix + apiErr.Code,
		"message": apiErr.Message,
	})
}
