package httpapi

import (
	"errors"
	"net/http"
	"strings"

	tableapp "github.com/jdillenkofer/pinax/internal/app/table"
	"github.com/jdillenkofer/pinax/internal/awserr"
)

type errorMapping struct {
	target error
	api    error
}

func mapErr(target error, api error) errorMapping {
	return errorMapping{target: target, api: api}
}

func badRequestAPIError(code string, message string) error {
	return &awserr.APIError{Code: code, Message: message, Status: http.StatusBadRequest}
}

func mapAPIError(err error, mappings ...errorMapping) error {
	if err == nil {
		return nil
	}
	for _, m := range mappings {
		if errors.Is(err, m.target) {
			return m.api
		}
	}
	return err
}

func mapUpdateTableError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, tableapp.ErrTableNotFound) {
		return awserr.ResourceNotFound("Cannot do operations on a non-existent table")
	}
	var inUseErr *tableapp.TableInUseError
	if errors.As(err, &inUseErr) {
		return awserr.ResourceInUse("Table is currently " + inUseErr.Status)
	}
	if containsAny(err.Error(), "GlobalSecondaryIndexUpdates", "index", "ProvisionedThroughput", "SSE", "TableClass", "Stream", "DeletionProtection") {
		return awserr.Validation(err.Error())
	}
	return err
}

func containsAny(s string, parts ...string) bool {
	for _, p := range parts {
		if strings.Contains(s, p) {
			return true
		}
	}
	return false
}

func mapPartiQLError(err error) (code string, message string, item map[string]any) {
	code = "ValidationError"
	message = err.Error()
	var apiErr *awserr.APIError
	if !errors.As(err, &apiErr) {
		return code, message, nil
	}

	message = apiErr.Message
	switch apiErr.Code {
	case "ValidationException":
		code = "ValidationError"
	case "ResourceNotFoundException":
		code = "ResourceNotFound"
	case "ConditionalCheckFailedException":
		code = "ConditionalCheckFailed"
	case "ProvisionedThroughputExceededException":
		code = "ProvisionedThroughputExceeded"
	case "AccessDeniedException":
		code = "AccessDenied"
	default:
		code = "InternalServerError"
	}

	if mappedItem, ok := apiErr.Details["Item"].(map[string]any); ok && mappedItem != nil {
		item = mappedItem
	}
	return code, message, item
}

func mapPartiQLTransactionReason(err error) awserr.CancellationReason {
	code, message, item := mapPartiQLError(err)
	reason := awserr.CancellationReason{Code: code, Message: message}
	if item != nil {
		reason.Item = item
	}
	return reason
}
