package pitr

import "github.com/jdillenkofer/pinax/internal/model"

const DefaultRecoveryPeriodInDays int64 = 35

func RestoreWindow(t model.Table, nowMs, lagMs int64) (int64, int64) {
	recoveryDays := t.PITR.RecoveryPeriodInDays
	if recoveryDays <= 0 {
		recoveryDays = DefaultRecoveryPeriodInDays
	}
	earliest := nowMs - (recoveryDays * 24 * 60 * 60 * 1000)
	if t.PITR.EnabledAt > 0 && t.PITR.EnabledAt > earliest {
		earliest = t.PITR.EnabledAt
	}
	latest := nowMs - lagMs
	if latest < earliest {
		latest = earliest
	}
	return earliest, latest
}

func ContinuousBackupsDescription(t model.Table, nowMs, lagMs int64) map[string]any {
	pitrStatus := model.PointInTimeRecoveryStatusDisabled
	recoveryDays := t.PITR.RecoveryPeriodInDays
	if recoveryDays <= 0 {
		recoveryDays = DefaultRecoveryPeriodInDays
	}
	pitrDesc := map[string]any{
		"PointInTimeRecoveryStatus": pitrStatus,
		"RecoveryPeriodInDays":      recoveryDays,
	}
	if t.PITR.Enabled {
		pitrStatus = model.PointInTimeRecoveryStatusEnabled
		earliest, latest := RestoreWindow(t, nowMs, lagMs)
		pitrDesc["PointInTimeRecoveryStatus"] = pitrStatus
		pitrDesc["EarliestRestorableDateTime"] = float64(earliest) / 1000.0
		pitrDesc["LatestRestorableDateTime"] = float64(latest) / 1000.0
	}
	return map[string]any{
		"ContinuousBackupsStatus":        model.ContinuousBackupsStatusEnabled,
		"PointInTimeRecoveryDescription": pitrDesc,
	}
}
