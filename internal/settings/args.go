package settings

import (
	"flag"
	"strings"
)

func registerStringFlag(flagSet *flag.FlagSet, name string, defaultValue string, description string) func() *string {
	stringVar := flagSet.String(name, defaultValue, description)
	accessor := func() *string {
		found := false
		flagSet.Visit(func(f *flag.Flag) {
			if f.Name == name {
				found = true
			}
		})
		if !found {
			return nil
		}
		return stringVar
	}
	return accessor
}

func registerIntFlag(flagSet *flag.FlagSet, name string, defaultValue int, description string) func() *int {
	intVar := flagSet.Int(name, defaultValue, description)
	accessor := func() *int {
		found := false
		flagSet.Visit(func(f *flag.Flag) {
			if f.Name == name {
				found = true
			}
		})
		if !found {
			return nil
		}
		return intVar
	}
	return accessor
}

func registerBoolFlag(flagSet *flag.FlagSet, name string, defaultValue bool, description string) func() *bool {
	boolVar := flagSet.Bool(name, defaultValue, description)
	accessor := func() *bool {
		found := false
		flagSet.Visit(func(f *flag.Flag) {
			if f.Name == name {
				found = true
			}
		})
		if !found {
			return nil
		}
		return boolVar
	}
	return accessor
}

func loadSettingsFromCmdArgs(cmdArgs []string) (*Settings, error) {
	serveCommand := flag.NewFlagSet("pinax", flag.ExitOnError)
	authenticationEnabledAccessor := registerBoolFlag(serveCommand, "authenticationEnabled", defaultAuthenticationEnabled, "determines if authentication is enabled")
	regionAccessor := registerStringFlag(serveCommand, "region", defaultRegion, "the region for the DynamoDB API")
	bindAddressAccessor := registerStringFlag(serveCommand, "bindAddress", defaultBindAddress, "the address the server is bound to")
	portAccessor := registerIntFlag(serveCommand, "port", defaultPort, "the port for the DynamoDB API")
	monitoringPortAccessor := registerIntFlag(serveCommand, "monitoringPort", defaultMonitoringPort, "the monitoring port of pinax")
	monitoringPortEnabledAccessor := registerBoolFlag(serveCommand, "monitoringPortEnabled", defaultMonitoringPortEnabled, "determines if the monitoring port of pinax is enabled or not")
	dbPathAccessor := registerStringFlag(serveCommand, "dbPath", defaultDBPath, "the path to the sqlite database")
	authorizerPathAccessor := registerStringFlag(serveCommand, "authorizerPath", defaultAuthorizerPath, "the path to the authorizer script")
	trustForwardedHeadersAccessor := registerBoolFlag(serveCommand, "trustForwardedHeaders", defaultTrustForwardedHeaders, "trust client forwarding headers")
	trustedProxyCIDRsAccessor := registerStringFlag(serveCommand, "trustedProxyCIDRs", "", "comma-separated trusted proxy CIDR ranges")
	logLevelAccessor := registerStringFlag(serveCommand, "logLevel", "info", "the log level for the application")
	ttlSweeperEnabledAccessor := registerBoolFlag(serveCommand, "ttlSweeperEnabled", false, "enable TTL sweeper background task")
	ttlSweeperIntervalAccessor := registerIntFlag(serveCommand, "ttlSweeperInterval", defaultTTLSweeperInterval, "TTL sweeper interval in nanoseconds")
	pitrLatestRestorableLagMillisAccessor := registerIntFlag(serveCommand, "pitrLatestRestorableLagMillis", defaultPITRLatestRestorableLagMillis, "PITR latest restorable lag in milliseconds")

	if err := serveCommand.Parse(cmdArgs); err != nil {
		return nil, err
	}

	var trustedProxyCIDRs []string
	trustedProxyCIDRsRaw := trustedProxyCIDRsAccessor()
	if trustedProxyCIDRsRaw != nil {
		parts := strings.Split(*trustedProxyCIDRsRaw, ",")
		trustedProxyCIDRs = make([]string, 0, len(parts))
		for _, part := range parts {
			trimmed := strings.TrimSpace(part)
			if trimmed != "" {
				trustedProxyCIDRs = append(trustedProxyCIDRs, trimmed)
			}
		}
	}

	return &Settings{
		authenticationEnabled:         authenticationEnabledAccessor(),
		credentials:                   nil,
		region:                        regionAccessor(),
		bindAddress:                   bindAddressAccessor(),
		port:                          portAccessor(),
		monitoringPort:                monitoringPortAccessor(),
		monitoringPortEnabled:         monitoringPortEnabledAccessor(),
		dbPath:                        dbPathAccessor(),
		authorizerPath:                authorizerPathAccessor(),
		trustForwardedHeaders:         trustForwardedHeadersAccessor(),
		trustedProxyCIDRs:             trustedProxyCIDRs,
		logLevel:                      logLevelAccessor(),
		ttlSweeperEnabled:             ttlSweeperEnabledAccessor(),
		ttlSweeperInterval:            ttlSweeperIntervalAccessor(),
		pitrLatestRestorableLagMillis: pitrLatestRestorableLagMillisAccessor(),
	}, nil
}
