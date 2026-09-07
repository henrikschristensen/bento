package quickfix

import (
	"errors"

	"fmt"

	goquickfix "github.com/quickfixgo/quickfix"
	"github.com/quickfixgo/quickfix/config"

	"github.com/warpstreamlabs/bento/public/service"
)

const fieldTLS = "tls"

// fixTLSConfig holds the subset of bento's standard TLS field that can be
// expressed as QuickFIX session settings. It is parsed from raw config values
// (rather than a built *tls.Config) so that inline PEM blocks and file paths
// can be forwarded to the engine verbatim.
type fixTLSConfig struct {
	enabled            bool
	insecureSkipVerify bool
	serverName         string
	rootCAs            string
	rootCAsFile        string
	clientCert         string
	clientKey          string
	clientCertFile     string
	clientKeyFile      string
}

// parseFixTLS reads the `tls` field as QuickFIX-translatable settings. It
// returns nil when TLS is not enabled. FieldTLSToggled is invoked at the end
// purely for its validation (PEM structure, cert/key pairing, readable
// files), after the structural checks above give clearer errors.
func parseFixTLS(pConf *service.ParsedConfig) (*fixTLSConfig, error) {
	tlsConf := pConf.Namespace(fieldTLS)

	enabled, err := tlsConf.FieldBool("enabled")
	if err != nil {
		return nil, err
	}
	if !enabled {
		return nil, nil
	}

	t := &fixTLSConfig{enabled: true}
	if t.insecureSkipVerify, err = tlsConf.FieldBool("skip_cert_verify"); err != nil {
		return nil, err
	}
	if t.serverName, err = tlsConf.FieldString("server_name"); err != nil {
		return nil, err
	}
	if t.rootCAs, err = tlsConf.FieldString("root_cas"); err != nil {
		return nil, err
	}
	if t.rootCAsFile, err = tlsConf.FieldString("root_cas_file"); err != nil {
		return nil, err
	}

	renegotiation, err := tlsConf.FieldBool("enable_renegotiation")
	if err != nil {
		return nil, err
	}
	if renegotiation {
		return nil, errors.New("tls.enable_renegotiation is not supported by quickfix components")
	}

	certs, err := tlsConf.FieldAnyList("client_certs")
	if err != nil {
		return nil, err
	}
	if len(certs) > 1 {
		return nil, fmt.Errorf("quickfix components support at most one tls client certificate, found %d", len(certs))
	}
	if len(certs) == 1 {
		c := certs[0]
		if t.clientCert, err = c.FieldString("cert"); err != nil {
			return nil, err
		}
		if t.clientKey, err = c.FieldString("key"); err != nil {
			return nil, err
		}
		if t.clientCertFile, err = c.FieldString("cert_file"); err != nil {
			return nil, err
		}
		if t.clientKeyFile, err = c.FieldString("key_file"); err != nil {
			return nil, err
		}
		password, err := c.FieldString("password")
		if err != nil {
			return nil, err
		}
		if password != "" {
			return nil, errors.New("tls client certificate key passwords are not supported by quickfix components")
		}
		inline := t.clientCert != "" || t.clientKey != ""
		files := t.clientCertFile != "" || t.clientKeyFile != ""
		switch {
		case inline && files:
			return nil, errors.New("tls client certificate must be configured with either inline `cert`/`key` or `cert_file`/`key_file`, not both")
		case inline && (t.clientCert == "" || t.clientKey == ""):
			return nil, errors.New("tls client certificate requires both `cert` and `key`")
		case files && (t.clientCertFile == "" || t.clientKeyFile == ""):
			return nil, errors.New("tls client certificate requires both `cert_file` and `key_file`")
		}
	}

	if _, _, err := pConf.FieldTLSToggled(fieldTLS); err != nil {
		return nil, err
	}

	return t, nil
}

// applyTLSSettings translates t into QuickFIX Socket* session settings. The
// keys are set on the global (DEFAULT) settings: Settings.SessionSettings()
// returns deep-copied clones, so mutating per-session settings would be
// discarded, whereas global settings are overlaid into every session by the
// engine itself. A consequence of quickfix's DEFAULT/SESSION semantics is
// that a Socket* key set explicitly inside a [SESSION] section of the user's
// FIX settings takes precedence over the value translated here.
func applyTLSSettings(qfSettings *goquickfix.Settings, t *fixTLSConfig) {
	gs := qfSettings.GlobalSettings()
	gs.Set(config.SocketUseSSL, "Y")
	if t.insecureSkipVerify {
		gs.Set(config.SocketInsecureSkipVerify, "Y")
	}
	if t.serverName != "" {
		gs.Set(config.SocketServerName, t.serverName)
	}
	if t.rootCAsFile != "" {
		gs.Set(config.SocketCAFile, t.rootCAsFile)
	}
	if t.rootCAs != "" {
		gs.SetRaw(config.SocketCABytes, []byte(t.rootCAs))
	}
	if t.clientCertFile != "" {
		gs.Set(config.SocketCertificateFile, t.clientCertFile)
	}
	if t.clientKeyFile != "" {
		gs.Set(config.SocketPrivateKeyFile, t.clientKeyFile)
	}
	if t.clientCert != "" {
		gs.SetRaw(config.SocketCertificateBytes, []byte(t.clientCert))
	}
	if t.clientKey != "" {
		gs.SetRaw(config.SocketPrivateKeyBytes, []byte(t.clientKey))
	}
}
