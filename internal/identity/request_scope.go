package identity

import (
	"errors"
	"fmt"
	"strings"
)

var (
	ErrAccessDenied      = errors.New("access denied")
	ErrTableNameRequired = errors.New("table name required")
	ErrResourceARN       = errors.New("resource arn")
	ErrResourceNotARN    = errors.New("resource arn must be arn")
)

func ScopedTableKeyFromIdentifier(tableIdentifier string, requestAccountID string) (string, error) {
	tableName, arnAccountID, isARN, err := ParseTableARN(tableIdentifier)
	if err != nil {
		return "", fmt.Errorf("%w: %w", ErrResourceARN, err)
	}
	if isARN && arnAccountID != "" && arnAccountID != strings.TrimSpace(requestAccountID) {
		return "", ErrAccessDenied
	}
	if strings.TrimSpace(tableName) == "" {
		return "", ErrTableNameRequired
	}
	return ScopedTableKey(requestAccountID, tableName), nil
}

func ValidateTableARNForAccount(resourceARN string, requestAccountID string) (string, error) {
	tableName, arnAccountID, isARN, err := ParseTableARN(resourceARN)
	if err != nil {
		return "", fmt.Errorf("%w: %w", ErrResourceARN, err)
	}
	if !isARN {
		return "", ErrResourceNotARN
	}
	if arnAccountID != "" && arnAccountID != strings.TrimSpace(requestAccountID) {
		return "", ErrAccessDenied
	}
	return tableName, nil
}
