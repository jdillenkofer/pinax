package settings

import (
	"log/slog"
	"reflect"
	"strings"
	"time"
	"unsafe"
)

const defaultAuthenticationEnabled = true
const defaultRegion = "eu-central-1"
const defaultBindAddress = "0.0.0.0"
const defaultPort = 8000
const defaultMonitoringPort = 9090
const defaultMonitoringPortEnabled = true
const defaultDBPath = "./pinax.db"
const defaultAuthorizerPath = "./authorizer.lua"
const defaultTrustForwardedHeaders = false
const defaultTTLSweeperInterval = 5 * 60 * 1000000000 // 5 minutes in nanoseconds
const defaultPITRLatestRestorableLagMillis = 0

const mergableTagKey = "mergable"

type Credentials struct {
	AccessKeyId     string
	SecretAccessKey string
}

type Settings struct {
	authenticationEnabled         *bool         `mergable:""`
	credentials                   []Credentials `mergable:""`
	region                        *string       `mergable:""`
	bindAddress                   *string       `mergable:""`
	port                          *int          `mergable:""`
	monitoringPort                *int          `mergable:""`
	monitoringPortEnabled         *bool         `mergable:""`
	dbPath                        *string       `mergable:""`
	authorizerPath                *string       `mergable:""`
	trustForwardedHeaders         *bool         `mergable:""`
	trustedProxyCIDRs             []string      `mergable:""`
	logLevel                      *string       `mergable:""`
	ttlSweeperEnabled             *bool         `mergable:""`
	ttlSweeperInterval            *int          `mergable:""`
	pitrLatestRestorableLagMillis *int          `mergable:""`
}

func valueOrDefault[V any](v *V, defaultValue V) V {
	if v == nil {
		return defaultValue
	}
	return *v
}

func (s *Settings) AuthenticationEnabled() bool {
	return valueOrDefault(s.authenticationEnabled, defaultAuthenticationEnabled)
}

func (s *Settings) Credentials() []Credentials {
	if !s.AuthenticationEnabled() {
		return nil
	}
	if s.credentials == nil {
		return []Credentials{}
	}
	return s.credentials
}

func (s *Settings) Region() string { return valueOrDefault(s.region, defaultRegion) }

func (s *Settings) BindAddress() string { return valueOrDefault(s.bindAddress, defaultBindAddress) }

func (s *Settings) Port() int { return valueOrDefault(s.port, defaultPort) }

func (s *Settings) MonitoringPort() int {
	return valueOrDefault(s.monitoringPort, defaultMonitoringPort)
}

func (s *Settings) MonitoringPortEnabled() bool {
	return valueOrDefault(s.monitoringPortEnabled, defaultMonitoringPortEnabled)
}

func (s *Settings) DBPath() string { return valueOrDefault(s.dbPath, defaultDBPath) }

func (s *Settings) AuthorizerPath() string {
	return valueOrDefault(s.authorizerPath, defaultAuthorizerPath)
}

func (s *Settings) TrustForwardedHeaders() bool {
	return valueOrDefault(s.trustForwardedHeaders, defaultTrustForwardedHeaders)
}

func (s *Settings) TrustedProxyCIDRs() []string {
	if s.trustedProxyCIDRs == nil {
		return []string{}
	}
	return s.trustedProxyCIDRs
}

func (s *Settings) LogLevel() slog.Level {
	logLevel := valueOrDefault(s.logLevel, slog.LevelInfo.String())
	switch strings.ToUpper(logLevel) {
	case slog.LevelDebug.String():
		return slog.LevelDebug
	case slog.LevelInfo.String():
		return slog.LevelInfo
	case slog.LevelWarn.String():
		return slog.LevelWarn
	case slog.LevelError.String():
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

func (s *Settings) TTLSweeperEnabled() bool {
	return valueOrDefault(s.ttlSweeperEnabled, false)
}

func (s *Settings) TTLSweeperInterval() time.Duration {
	return time.Duration(valueOrDefault(s.ttlSweeperInterval, defaultTTLSweeperInterval)) * time.Nanosecond
}

func (s *Settings) PITRLatestRestorableLagMillis() int64 {
	v := valueOrDefault(s.pitrLatestRestorableLagMillis, defaultPITRLatestRestorableLagMillis)
	if v < 0 {
		return 0
	}
	return int64(v)
}

func getUnexportedField(field reflect.Value) interface{} {
	return reflect.NewAt(field.Type(), unsafe.Pointer(field.UnsafeAddr())).Elem().Interface()
}

func setUnexportedField(field reflect.Value, value interface{}) {
	reflect.NewAt(field.Type(), unsafe.Pointer(field.UnsafeAddr())).Elem().Set(reflect.ValueOf(value))
}

func isNilish(val any) bool {
	if val == nil {
		return true
	}

	v := reflect.ValueOf(val)
	k := v.Kind()
	switch k {
	case reflect.Chan, reflect.Func, reflect.Map, reflect.Pointer,
		reflect.UnsafePointer, reflect.Interface, reflect.Slice:
		return v.IsNil()
	}

	return false
}

func (s *Settings) merge(other *Settings) {
	fields := reflect.VisibleFields(reflect.TypeOf(other).Elem())
	sStruct := reflect.ValueOf(s).Elem()
	otherStruct := reflect.ValueOf(other).Elem()

	for _, field := range fields {
		if _, ok := field.Tag.Lookup(mergableTagKey); !ok {
			continue
		}
		sField := sStruct.FieldByName(field.Name)
		otherField := otherStruct.FieldByName(field.Name)

		if field.Type.Kind() == reflect.Pointer {
			otherFieldValue := getUnexportedField(otherField)
			if !isNilish(otherFieldValue) {
				setUnexportedField(sField, otherFieldValue)
			}
		} else {
			otherFieldValue := getUnexportedField(otherField)
			setUnexportedField(sField, otherFieldValue)
		}
	}
}

func mergeSettings(settings ...*Settings) *Settings {
	result := &Settings{}
	for _, setting := range settings {
		if setting == nil {
			continue
		}
		result.merge(setting)
	}
	return result
}

func LoadSettings(cmdArgs []string) (*Settings, error) {
	cmdArgsSettings, err := loadSettingsFromCmdArgs(cmdArgs)
	if err != nil {
		return nil, err
	}
	envSettings, err := loadSettingsFromEnv()
	if err != nil {
		return nil, err
	}
	return mergeSettings(cmdArgsSettings, envSettings), nil
}
