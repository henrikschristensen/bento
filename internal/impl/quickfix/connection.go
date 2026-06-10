package quickfix

import (
	"crypto/rand"
	"encoding/hex"
	"strings"

	goquickfix "codeberg.org/hsctech/quickfix"
	"codeberg.org/hsctech/quickfix/config"
	"codeberg.org/hsctech/quickfix/datadictionary"

	"github.com/warpstreamlabs/bento/public/service"
)

const (
	fieldConnectionType = "connection_type"
	fieldSettings       = "settings"
	fieldMessageFormat  = "message_format"
	fieldName           = "name"
)

// connConfig holds the connection fields shared by input and output.
type connConfig struct {
	name     string
	connType string
	settings string
}

// parseConnConfig reads the shared connection fields from a parsed config and
// assigns an anonymous unique name when the name field is empty.
func parseConnConfig(pConf *service.ParsedConfig) (connConfig, error) {
	var c connConfig
	var err error
	if c.name, err = pConf.FieldString(fieldName); err != nil {
		return c, err
	}
	if c.connType, err = pConf.FieldString(fieldConnectionType); err != nil {
		return c, err
	}
	if c.settings, err = pConf.FieldString(fieldSettings); err != nil {
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

// loadDataDictionary attempts to extract and parse the DataDictionary (or
// AppDataDictionary) path from a QuickFIX settings string. Returns nil if no
// dictionary is configured or if parsing fails.
func loadDataDictionary(settings string, log *service.Logger) *datadictionary.DataDictionary {
	if settings == "" {
		return nil
	}
	qfSettings, err := goquickfix.ParseSettings(strings.NewReader(settings))
	if err != nil {
		return nil
	}
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

// connectEngine acquires (or creates) the shared engine for cfg, subscribes
// sub to it, starts it if not already running, and returns the engine.
func connectEngine(cfg connConfig, log *service.Logger, sub fixSubscriber) (*sharedEngine, error) {
	eng, err := acquireSharedEngine(cfg.name, cfg.connType, cfg.settings, log)
	if err != nil {
		return nil, err
	}
	eng.subscribe(sub)
	if err := eng.start(); err != nil {
		eng.unsubscribe(sub)
		releaseSharedEngine(cfg.name)
		return nil, err
	}
	return eng, nil
}

// disconnectEngine unsubscribes sub and releases the shared engine.
// The caller is responsible for setting their engine reference to nil afterward.
func disconnectEngine(name string, eng *sharedEngine, sub fixSubscriber) {
	if eng != nil {
		eng.unsubscribe(sub)
		releaseSharedEngine(name)
	}
}
