package quickfix

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
	"sync"

	goquickfix "github.com/quickfixgo/quickfix"
	"github.com/quickfixgo/quickfix/config"
	"github.com/quickfixgo/quickfix/datadictionary"

	"github.com/warpstreamlabs/bento/public/service"
)

const (
	fieldConnectionType = "connection_type"
	fieldSettingsFile   = "settings_file"
	fieldMessageFormat  = "message_format"
	fieldName           = "name"
)

// connConfig holds the connection fields shared by input and output. The FIX
// settings are parsed exactly once, here at construction: qfSettings and dd
// are the prepared results handed to the engine and the message converters.
type connConfig struct {
	name       string
	connType   string
	settings   string
	qfSettings *goquickfix.Settings
	dd         *datadictionary.DataDictionary
	tls        *fixTLSConfig
	store      fixStore
}

// parseConnConfig reads the shared connection fields from a parsed config,
// parses the FIX settings and loads the data dictionary, and assigns an
// anonymous unique name when the name field is empty.
func parseConnConfig(pConf *service.ParsedConfig, log *service.Logger) (connConfig, error) {
	var c connConfig
	var err error
	if c.name, err = pConf.FieldString(fieldName); err != nil {
		return c, err
	}
	if c.connType, err = pConf.FieldString(fieldConnectionType); err != nil {
		return c, err
	}
	var settingsFile string
	if settingsFile, err = pConf.FieldString(fieldSettingsFile); err != nil {
		return c, err
	}
	if c.settings, err = loadSettingsFile(settingsFile); err != nil {
		return c, err
	}
	if c.qfSettings, err = goquickfix.ParseSettings(strings.NewReader(c.settings)); err != nil {
		return c, fmt.Errorf("invalid `%s` %q: %w", fieldSettingsFile, settingsFile, err)
	}
	if err = rejectStoreSettingsKeys(c.qfSettings); err != nil {
		return c, err
	}
	// The data dictionary is loaded under one rule for both the input and the
	// output: whenever a dictionary is configured it is parsed here, for json
	// message conversion and for preserving repeating groups in raw mode.
	c.dd = loadDataDictionary(c.qfSettings, log)
	if c.tls, err = parseFixTLS(pConf); err != nil {
		return c, err
	}
	if c.store, err = parseStoreConfig(pConf); err != nil {
		return c, err
	}
	if c.name == "" {
		// Components without an explicit name get a unique identifier so they
		// each get their own engine and don't accidentally share connections.
		c.name = "_anon_" + randomID()
	}
	return c, nil
}

func randomID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// loadSettingsFile reads the QuickFIX cfg text from the given settings_file
// path. Any ${ENV_VAR} references in the file are expanded from the process
// environment, failing if a referenced variable is unset.
func loadSettingsFile(settingsFile string) (string, error) {
	if settingsFile == "" {
		return "", fmt.Errorf("`%s` must be set", fieldSettingsFile)
	}
	raw, err := os.ReadFile(settingsFile)
	if err != nil {
		return "", fmt.Errorf("failed to read `%s` %q: %w", fieldSettingsFile, settingsFile, err)
	}
	expanded, err := expandEnvStrict(string(raw))
	if err != nil {
		return "", fmt.Errorf("`%s` %q: %w", fieldSettingsFile, settingsFile, err)
	}
	return expanded, nil
}

// envVarRef matches a ${VAR} reference. Only the braced form is expanded; a
// bare `$VAR` is left untouched so literal dollar signs in cfg values survive.
var envVarRef = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)\}`)

// expandEnvStrict replaces ${VAR} references in s with the corresponding
// environment variable values, returning an error naming every referenced
// variable that is unset.
func expandEnvStrict(s string) (string, error) {
	missing := map[string]struct{}{}
	out := envVarRef.ReplaceAllStringFunc(s, func(match string) string {
		name := envVarRef.FindStringSubmatch(match)[1]
		if v, ok := os.LookupEnv(name); ok {
			return v
		}
		missing[name] = struct{}{}
		return ""
	})
	if len(missing) > 0 {
		names := make([]string, 0, len(missing))
		for n := range missing {
			names = append(names, n)
		}
		sort.Strings(names)
		return "", fmt.Errorf("required environment variable(s) not set: %s", strings.Join(names, ", "))
	}
	return out, nil
}

// loadDataDictionary extracts and parses the DataDictionary (or
// AppDataDictionary) referenced by the parsed FIX settings. Returns nil if no
// dictionary is configured or if parsing fails.
func loadDataDictionary(qfSettings *goquickfix.Settings, log *service.Logger) *datadictionary.DataDictionary {
	for _, s := range qfSettings.SessionSettings() {
		for _, key := range []string{config.AppDataDictionary, config.DataDictionary} {
			if path, err := s.Setting(key); err == nil && path != "" {
				dd, err := datadictionary.Parse(path)
				if err != nil {
					log.Warnf("Failed to parse FIX data dictionary %q: %v", path, err)
					return nil
				}
				return dd
			}
		}
	}
	return nil
}

// sharedEngineDescription documents the `name`-based engine sharing semantics,
// identical for the quickfix input and output specs.
const sharedEngineDescription = `When the ` + "`name`" + ` field is set, the underlying QuickFIX engine is shared with any other ` + "`quickfix`" + ` input or output that uses the same name. This allows a single FIX connection (initiator or acceptor) to be initiated by any input or output and reused by any combination of inputs and outputs. The first component to reference a shared name must supply ` + "`connection_type`" + ` and ` + "`settings_file`" + `; subsequent components referencing the same name may omit them, but if supplied they must match the originally registered values.`

// quickfixConnFields returns the config fields shared by the quickfix input
// and output specs.
func quickfixConnFields() []*service.ConfigField {
	return []*service.ConfigField{
		service.NewStringField(fieldName).
			Description("Optional name used to share a single QuickFIX engine across multiple `quickfix` inputs and outputs. Components referencing the same name reuse the same underlying acceptor or initiator connection, and must all be configured with identical `connection_type`, `settings_file` and `store`.").
			Default(""),
		service.NewStringEnumField(fieldConnectionType, "acceptor", "initiator").
			Description("Whether to listen for incoming connections (`acceptor`) or connect to a remote host (`initiator`)."),
		service.NewStringField(fieldSettingsFile).
			Description("Path to a file containing [QuickFIX/Go session settings](https://quickfixengine.org/go/documentation/getting-started/configuration.html) in the standard cfg format. `${ENV_VAR}` references in the file are expanded from the environment when the config is loaded, failing if a referenced variable is unset.").
			Default(""),
		storeSpecField(),
	}
}

// engineAttachment holds a component's lease on a shared FIX engine and
// implements the Connect/Close lifecycle shared by the input and the output.
type engineAttachment struct {
	mu     sync.Mutex
	handle *engineHandle
}

// connect acquires a handle to the shared engine. It is idempotent.
func (a *engineAttachment) connect(cfg connConfig, log *service.Logger, sub fixSubscriber) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.handle != nil {
		return nil
	}
	h, err := acquireEngine(cfg, log, sub)
	if err != nil {
		return err
	}
	a.handle = h
	return nil
}

// disconnect releases the engine handle. It is idempotent.
func (a *engineAttachment) disconnect() {
	a.mu.Lock()
	defer a.mu.Unlock()

	h := a.handle
	a.handle = nil
	h.release()
}

// get returns the current engine handle, or nil when not connected.
func (a *engineAttachment) get() *engineHandle {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.handle
}
