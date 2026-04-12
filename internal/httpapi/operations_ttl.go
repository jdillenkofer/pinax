package httpapi

import (
	"context"
	"net/http"
	"strings"

	"github.com/jdillenkofer/pinax/internal/app/uow"
	"github.com/jdillenkofer/pinax/internal/awserr"
	"github.com/jdillenkofer/pinax/internal/model"
)

type updateTimeToLiveRequest struct {
	TableName               string `json:"TableName"`
	TimeToLiveSpecification struct {
		Enabled       bool   `json:"Enabled"`
		AttributeName string `json:"AttributeName"`
	} `json:"TimeToLiveSpecification"`
}

func (s *Server) updateTimeToLive(r *http.Request, body []byte) (map[string]any, error) {
	var req updateTimeToLiveRequest
	if err := decode(body, &req); err != nil {
		return nil, awserr.Validation(err.Error())
	}
	if req.TableName == "" {
		return nil, awserr.Validation("TableName is required")
	}
	if req.TimeToLiveSpecification.AttributeName == "" {
		return nil, awserr.Validation("AttributeName is required")
	}

	ttl := model.TimeToLive{
		Enabled:  req.TimeToLiveSpecification.Enabled,
		AttrName: req.TimeToLiveSpecification.AttributeName,
		StatusAt: lifecycleNow() + lifecycleDelayMillis(),
	}
	if ttl.Enabled {
		ttl.Status = model.TTLStatusEnabling
	} else {
		ttl.Status = model.TTLStatusDisabling
	}
	if err := s.unitOfWork.Do(r.Context(), func(txCtx context.Context, repos uow.Repos) error {
		t, err := s.getTableWithLifecycleFromRepo(txCtx, repos.Tables(), req.TableName)
		if err != nil {
			return err
		}
		return repos.Tables().UpdateTimeToLive(txCtx, t.Name, ttl)
	}); err != nil {
		return nil, err
	}
	return map[string]any{
		"TimeToLiveDescription": map[string]any{
			"TimeToLiveStatus": ttl.Status,
			"AttributeName":    ttl.AttrName,
		},
	}, nil
}

type describeTimeToLiveRequest struct {
	TableName string `json:"TableName"`
}

func (s *Server) describeTimeToLive(r *http.Request, body []byte) (map[string]any, error) {
	var req describeTimeToLiveRequest
	if err := decode(body, &req); err != nil {
		return nil, awserr.Validation(err.Error())
	}
	if req.TableName == "" {
		return nil, awserr.Validation("TableName is required")
	}

	var t model.Table
	if err := s.unitOfWork.Do(r.Context(), func(txCtx context.Context, repos uow.Repos) error {
		var err error
		t, err = s.getTableWithLifecycleFromRepo(txCtx, repos.Tables(), req.TableName)
		if err != nil {
			return err
		}
		now := lifecycleNow()
		if t.TimeToLive.Status == model.TTLStatusEnabling && t.TimeToLive.StatusAt > 0 && now >= t.TimeToLive.StatusAt {
			t.TimeToLive.Status = model.TTLStatusEnabled
			t.TimeToLive.StatusAt = 0
			t.TimeToLive.Enabled = true
			if err := repos.Tables().UpdateTimeToLive(txCtx, t.Name, t.TimeToLive); err != nil {
				return err
			}
		}
		if t.TimeToLive.Status == model.TTLStatusDisabling && t.TimeToLive.StatusAt > 0 && now >= t.TimeToLive.StatusAt {
			t.TimeToLive.Status = model.TTLStatusDisabled
			t.TimeToLive.StatusAt = 0
			t.TimeToLive.Enabled = false
			if err := repos.Tables().UpdateTimeToLive(txCtx, t.Name, t.TimeToLive); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}

	status := t.TimeToLive.Status
	if strings.TrimSpace(status) == "" {
		if t.TimeToLive.Enabled {
			status = model.TTLStatusEnabled
		} else {
			status = model.TTLStatusDisabled
		}
	}
	return map[string]any{
		"TimeToLiveDescription": map[string]any{
			"TimeToLiveStatus": status,
			"AttributeName":    t.TimeToLive.AttrName,
		},
	}, nil
}
