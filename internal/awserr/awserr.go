package awserr

import (
	"encoding/json"
	"errors"
	"hash/crc32"
	"net/http"
	"strconv"
)

const prefix = "com.amazonaws.dynamodb.v20120810#"

type APIError struct {
	Code    string
	Message string
	Status  int
	Details map[string]any
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

func ConditionalCheckFailedWithItem(msg string, item map[string]any) *APIError {
	err := ConditionalCheckFailed(msg)
	if item != nil {
		err.Details = map[string]any{"Item": item}
	}
	return err
}

func ProvisionedThroughputExceeded(msg string) *APIError {
	return &APIError{Code: "ProvisionedThroughputExceededException", Message: msg, Status: http.StatusBadRequest}
}

func IdempotentParameterMismatch(msg string) *APIError {
	return &APIError{Code: "IdempotentParameterMismatchException", Message: msg, Status: http.StatusBadRequest}
}

type CancellationReason struct {
	Code    string         `json:"Code"`
	Message string         `json:"Message,omitempty"`
	Item    map[string]any `json:"Item,omitempty"`
}

func TransactionCanceled(msg string, reasons []CancellationReason) *APIError {
	return &APIError{
		Code:    "TransactionCanceledException",
		Message: msg,
		Status:  http.StatusBadRequest,
		Details: map[string]any{"CancellationReasons": reasons},
	}
}

func Internal(msg string) *APIError {
	return &APIError{Code: "InternalServerError", Message: msg, Status: http.StatusInternalServerError}
}

func Write(w http.ResponseWriter, err error) {
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		apiErr = Internal(err.Error())
	}

	payload := map[string]any{
		"__type":  prefix + apiErr.Code,
		"message": apiErr.Message,
	}
	for k, v := range apiErr.Details {
		payload[k] = v
	}
	body, err := json.Marshal(payload)
	if err != nil {
		body = []byte(`{"__type":"` + prefix + `InternalServerError","message":"failed to encode error payload"}`)
	}
	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	w.Header().Set("X-Amz-Crc32", strconv.FormatUint(uint64(crc32.ChecksumIEEE(body)), 10))
	w.WriteHeader(apiErr.Status)
	_, _ = w.Write(body)
}
