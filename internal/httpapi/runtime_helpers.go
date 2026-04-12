package httpapi

import (
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jdillenkofer/pinax/internal/awserr"
	"github.com/jdillenkofer/pinax/internal/model"
)

func lifecycleNow() int64 {
	return time.Now().UnixMilli()
}

func lifecycleDelayMillis() int64 {
	raw := strings.TrimSpace(os.Getenv("PINAX_LIFECYCLE_DELAY_MS"))
	if raw == "" {
		return 1000
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || v < 0 {
		return 1000
	}
	return v
}

func enforceProvisionedLimits() bool {
	return strings.EqualFold(strings.TrimSpace(os.Getenv("PINAX_ENFORCE_PROVISIONED_LIMITS")), "true")
}

func pitrLatestRestorableLagMillisFromEnv() int64 {
	raw := strings.TrimSpace(os.Getenv("PINAX_PITR_LATEST_RESTORABLE_LAG_MS"))
	if raw == "" {
		return 0
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || v < 0 {
		return 0
	}
	return v
}

func (s *Server) ensureReadCapacity(t model.Table, units float64) error {
	if !s.reserveReadCapacity(t, units) {
		return awserr.ProvisionedThroughputExceeded("read capacity exceeded for table " + logicalTableNameFromKey(t.Name))
	}
	return nil
}

func (s *Server) ensureWriteCapacity(t model.Table, units float64) error {
	if !s.reserveWriteCapacity(t, units) {
		return awserr.ProvisionedThroughputExceeded("write capacity exceeded for table " + logicalTableNameFromKey(t.Name))
	}
	return nil
}

func (s *Server) reserveReadCapacity(t model.Table, units float64) bool {
	if !enforceProvisionedLimits() || t.BillingMode != "PROVISIONED" {
		return true
	}
	if units <= 0 || t.ReadCapacityUnits <= 0 {
		return true
	}
	return s.reserveCapacityUnits(t.Name, float64(t.ReadCapacityUnits), units, true)
}

func (s *Server) reserveWriteCapacity(t model.Table, units float64) bool {
	if !enforceProvisionedLimits() || t.BillingMode != "PROVISIONED" {
		return true
	}
	if units <= 0 || t.WriteCapacityUnits <= 0 {
		return true
	}
	return s.reserveCapacityUnits(t.Name, float64(t.WriteCapacityUnits), units, false)
}

func (s *Server) reserveCapacityUnits(tableName string, perSecondLimit float64, units float64, isRead bool) bool {
	if units <= 0 {
		return true
	}
	nowSec := time.Now().Unix()
	key := tableName
	if isRead {
		key += "|r"
	} else {
		key += "|w"
	}

	s.capMu.Lock()
	defer s.capMu.Unlock()

	window := s.capacityWindows[key]
	if window.second != nowSec {
		window = capacityWindow{second: nowSec}
	}
	used := window.writeUsed
	if isRead {
		used = window.readUsed
	}
	if used+units > perSecondLimit+0.00001 {
		s.capacityWindows[key] = window
		return false
	}
	if isRead {
		window.readUsed += units
	} else {
		window.writeUsed += units
	}
	s.capacityWindows[key] = window
	return true
}
