package settings

import (
	"os"
	"strconv"
	"strings"
)

const envKeyPrefix = "PINAX"

const authenticationEnabledEnvKey = envKeyPrefix + "_AUTHENTICATION_ENABLED"
const regionEnvKey = envKeyPrefix + "_REGION"
const bindAddressEnvKey = envKeyPrefix + "_BIND_ADDRESS"
const portEnvKey = envKeyPrefix + "_PORT"
const dbPathEnvKey = envKeyPrefix + "_DB_PATH"
const authorizerPathEnvKey = envKeyPrefix + "_AUTHORIZER_PATH"
const trustForwardedHeadersEnvKey = envKeyPrefix + "_TRUST_FORWARDED_HEADERS"
const trustedProxyCIDRsEnvKey = envKeyPrefix + "_TRUSTED_PROXY_CIDRS"
const logLevelEnvKey = envKeyPrefix + "_LOG_LEVEL"
const ttlSweeperEnabledEnvKey = envKeyPrefix + "_TTL_SWEEPER_ENABLED"
const ttlSweeperIntervalEnvKey = envKeyPrefix + "_TTL_SWEEPER_INTERVAL"
const pitrLatestRestorableLagMillisEnvKey = envKeyPrefix + "_PITR_LATEST_RESTORABLE_LAG_MS"

func getCredentialsFromEnv() []Credentials {
	var credentials []Credentials
	for i := 0; ; i++ {
		accessKeyID := getStringFromEnv(envKeyPrefix + "_CREDENTIALS_" + strconv.Itoa(i) + "_ACCESS_KEY_ID")
		secretAccessKey := getStringFromEnv(envKeyPrefix + "_CREDENTIALS_" + strconv.Itoa(i) + "_SECRET_ACCESS_KEY")

		if accessKeyID == nil || secretAccessKey == nil {
			if i == 0 {
				continue
			}
			break
		}

		credentials = append(credentials, Credentials{AccessKeyId: *accessKeyID, SecretAccessKey: *secretAccessKey})
	}
	return credentials
}

func getStringFromEnv(envKey string) *string {
	val := os.Getenv(envKey)
	if val == "" {
		return nil
	}
	return &val
}

func getIntFromEnv(envKey string) *int {
	val := os.Getenv(envKey)
	if val == "" {
		return nil
	}
	int64Val, err := strconv.ParseInt(val, 10, 32)
	if err != nil {
		return nil
	}
	intVal := int(int64Val)
	return &intVal
}

func getBoolFromEnv(envKey string) *bool {
	val := strings.ToLower(os.Getenv(envKey))
	if val == "" {
		return nil
	}
	retval := val == "1" || val == "t" || val == "true"
	return &retval
}

func getStringSliceFromEnv(envKey string) []string {
	val := os.Getenv(envKey)
	if val == "" {
		return nil
	}
	parts := strings.Split(val, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}

func loadSettingsFromEnv() (*Settings, error) {
	return &Settings{
		authenticationEnabled:         getBoolFromEnv(authenticationEnabledEnvKey),
		credentials:                   getCredentialsFromEnv(),
		region:                        getStringFromEnv(regionEnvKey),
		bindAddress:                   getStringFromEnv(bindAddressEnvKey),
		port:                          getIntFromEnv(portEnvKey),
		dbPath:                        getStringFromEnv(dbPathEnvKey),
		authorizerPath:                getStringFromEnv(authorizerPathEnvKey),
		trustForwardedHeaders:         getBoolFromEnv(trustForwardedHeadersEnvKey),
		trustedProxyCIDRs:             getStringSliceFromEnv(trustedProxyCIDRsEnvKey),
		logLevel:                      getStringFromEnv(logLevelEnvKey),
		ttlSweeperEnabled:             getBoolFromEnv(ttlSweeperEnabledEnvKey),
		ttlSweeperInterval:            getIntFromEnv(ttlSweeperIntervalEnvKey),
		pitrLatestRestorableLagMillis: getIntFromEnv(pitrLatestRestorableLagMillisEnvKey),
	}, nil
}
