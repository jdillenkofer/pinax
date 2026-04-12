package resourcepolicy

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"strings"
	"time"

	tableapp "github.com/jdillenkofer/pinax/internal/app/table"
	"github.com/jdillenkofer/pinax/internal/app/uow"
)

var ErrPolicyNotFound = errors.New("policy not found")
var ErrConfirmRemoveSelfResourceAccessRequired = errors.New("confirm remove self resource access required")

type Service struct {
	uow       uow.UnitOfWork
	lifecycle *tableapp.LifecycleService
}

func NewService(unitOfWork uow.UnitOfWork, lifecycle *tableapp.LifecycleService) *Service {
	return &Service{uow: unitOfWork, lifecycle: lifecycle}
}

func (s *Service) EnsureTarget(ctx context.Context, tableKey, resourceARN string, isStream bool, now int64) error {
	if s.lifecycle == nil {
		return errors.New("lifecycle service is required")
	}
	return s.uow.Do(ctx, func(txCtx context.Context, repos uow.Repos) error {
		t, err := s.lifecycle.GetWithLifecycle(txCtx, repos.Tables(), tableKey, now)
		if err != nil {
			return err
		}
		if isStream {
			if !t.Stream.Enabled || strings.TrimSpace(t.Stream.ARN) == "" || t.Stream.ARN != resourceARN {
				return tableapp.ErrTableNotFound
			}
		}
		return nil
	})
}

func (s *Service) Put(ctx context.Context, resourceARN, policy, expectedRevisionID string, confirmRemoveSelfResourceAccess bool, needsConfirm func(string, string) bool) (string, error) {
	var revisionID string
	err := s.uow.Do(ctx, func(txCtx context.Context, repos uow.Repos) error {
		existingPolicy, existingRevision, err := repos.ResourcePolicies().GetResourcePolicy(txCtx, resourceARN)
		hasExisting := err == nil
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		expectedRevision := strings.TrimSpace(expectedRevisionID)
		if expectedRevision != "" {
			switch {
			case expectedRevision == "NO_POLICY":
				if hasExisting {
					return ErrPolicyNotFound
				}
			case !hasExisting || existingRevision != expectedRevision:
				return ErrPolicyNotFound
			}
		}
		if !confirmRemoveSelfResourceAccess && needsConfirm != nil && needsConfirm(resourceARN, policy) {
			return ErrConfirmRemoveSelfResourceAccessRequired
		}
		revisionID = existingRevision
		if !hasExisting || existingPolicy != policy {
			revisionID = RevisionID(resourceARN, policy)
			if err := repos.ResourcePolicies().PutResourcePolicy(txCtx, resourceARN, policy, revisionID, time.Now().UnixMilli()); err != nil {
				return err
			}
		}
		return nil
	})
	return revisionID, err
}

func (s *Service) Get(ctx context.Context, resourceARN string) (string, string, error) {
	var policy, revisionID string
	err := s.uow.Do(ctx, func(txCtx context.Context, repos uow.Repos) error {
		var err error
		policy, revisionID, err = repos.ResourcePolicies().GetResourcePolicy(txCtx, resourceARN)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrPolicyNotFound
			}
			return err
		}
		return nil
	})
	return policy, revisionID, err
}

func (s *Service) Delete(ctx context.Context, resourceARN, expectedRevisionID string) (string, error) {
	revisionID := ""
	err := s.uow.Do(ctx, func(txCtx context.Context, repos uow.Repos) error {
		_, existingRevision, err := repos.ResourcePolicies().GetResourcePolicy(txCtx, resourceARN)
		hasExisting := err == nil
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		expectedRevision := strings.TrimSpace(expectedRevisionID)
		if expectedRevision != "" {
			if !hasExisting || existingRevision != expectedRevision {
				return ErrPolicyNotFound
			}
		}
		if hasExisting {
			deletedRevision, found, err := repos.ResourcePolicies().DeleteResourcePolicy(txCtx, resourceARN)
			if err != nil {
				return err
			}
			if found {
				revisionID = deletedRevision
			}
		}
		return nil
	})
	return revisionID, err
}

func RevisionID(resourceARN, policy string) string {
	h := sha256.Sum256([]byte(resourceARN + "\x00" + policy))
	return hex.EncodeToString(h[:16])
}
