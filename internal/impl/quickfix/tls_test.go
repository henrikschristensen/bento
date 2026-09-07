package quickfix

import (
	"strings"
	"testing"

	goquickfix "github.com/quickfixgo/quickfix"
	"github.com/quickfixgo/quickfix/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testFIXSettings = `[DEFAULT]
ConnectionType=initiator
HeartBtInt=30
BeginString=FIX.4.4

[SESSION]
SenderCompID=CLIENT
TargetCompID=SERVER
SocketConnectHost=localhost
SocketConnectPort=5001
`

func mustParseSettings(t *testing.T, s string) *goquickfix.Settings {
	t.Helper()
	parsed, err := goquickfix.ParseSettings(strings.NewReader(s))
	require.NoError(t, err)
	return parsed
}

func sessionSetting(t *testing.T, s *goquickfix.Settings, key string) string {
	t.Helper()
	for _, ss := range s.SessionSettings() {
		v, err := ss.Setting(key)
		require.NoError(t, err)
		return v
	}
	t.Fatalf("no session settings found")
	return ""
}

func hasSessionSetting(s *goquickfix.Settings, key string) bool {
	for _, ss := range s.SessionSettings() {
		return ss.HasSetting(key)
	}
	return false
}

func TestApplyTLSSettingsMinimal(t *testing.T) {
	settings := mustParseSettings(t, testFIXSettings)

	applyTLSSettings(settings, &fixTLSConfig{enabled: true})

	assert.Equal(t, "Y", sessionSetting(t, settings, config.SocketUseSSL))
	assert.False(t, hasSessionSetting(settings, config.SocketInsecureSkipVerify))
	assert.False(t, hasSessionSetting(settings, config.SocketServerName))
	assert.False(t, hasSessionSetting(settings, config.SocketCAFile))
}

func TestApplyTLSSettingsFullInline(t *testing.T) {
	settings := mustParseSettings(t, testFIXSettings)

	applyTLSSettings(settings, &fixTLSConfig{
		enabled:            true,
		insecureSkipVerify: true,
		serverName:         "fix.example.com",
		rootCAs:            "CA-PEM",
		clientCert:         "CERT-PEM",
		clientKey:          "KEY-PEM",
	})

	assert.Equal(t, "Y", sessionSetting(t, settings, config.SocketUseSSL))
	assert.Equal(t, "Y", sessionSetting(t, settings, config.SocketInsecureSkipVerify))
	assert.Equal(t, "fix.example.com", sessionSetting(t, settings, config.SocketServerName))
	assert.Equal(t, "CA-PEM", sessionSetting(t, settings, config.SocketCABytes))
	assert.Equal(t, "CERT-PEM", sessionSetting(t, settings, config.SocketCertificateBytes))
	assert.Equal(t, "KEY-PEM", sessionSetting(t, settings, config.SocketPrivateKeyBytes))
}

func TestApplyTLSSettingsFiles(t *testing.T) {
	settings := mustParseSettings(t, testFIXSettings)

	applyTLSSettings(settings, &fixTLSConfig{
		enabled:        true,
		rootCAsFile:    "/etc/ssl/ca.pem",
		clientCertFile: "/etc/ssl/cert.pem",
		clientKeyFile:  "/etc/ssl/key.pem",
	})

	assert.Equal(t, "/etc/ssl/ca.pem", sessionSetting(t, settings, config.SocketCAFile))
	assert.Equal(t, "/etc/ssl/cert.pem", sessionSetting(t, settings, config.SocketCertificateFile))
	assert.Equal(t, "/etc/ssl/key.pem", sessionSetting(t, settings, config.SocketPrivateKeyFile))
	assert.False(t, hasSessionSetting(settings, config.SocketCABytes))
}

func TestApplyTLSSettingsMultiSession(t *testing.T) {
	settings := mustParseSettings(t, testFIXSettings+`
[SESSION]
SenderCompID=CLIENT2
TargetCompID=SERVER2
SocketConnectHost=localhost
SocketConnectPort=5002
`)

	applyTLSSettings(settings, &fixTLSConfig{enabled: true, serverName: "fix.example.com"})

	for _, ss := range settings.SessionSettings() {
		v, err := ss.Setting(config.SocketUseSSL)
		require.NoError(t, err)
		assert.Equal(t, "Y", v)
		sn, err := ss.Setting(config.SocketServerName)
		require.NoError(t, err)
		assert.Equal(t, "fix.example.com", sn)
	}
}

func TestParseFixTLSDisabled(t *testing.T) {
	pConf, err := quickfixInputSpec().ParseYAML(`
connection_type: initiator
`, nil)
	require.NoError(t, err)

	tlsCfg, err := parseFixTLS(pConf)
	require.NoError(t, err)
	assert.Nil(t, tlsCfg)
}

func TestParseFixTLSEnabled(t *testing.T) {
	pConf, err := quickfixInputSpec().ParseYAML(`
connection_type: initiator
tls:
  enabled: true
  skip_cert_verify: true
  server_name: fix.example.com
`, nil)
	require.NoError(t, err)

	tlsCfg, err := parseFixTLS(pConf)
	require.NoError(t, err)
	require.NotNil(t, tlsCfg)
	assert.True(t, tlsCfg.insecureSkipVerify)
	assert.Equal(t, "fix.example.com", tlsCfg.serverName)
}

func TestParseFixTLSRejectsMultipleClientCerts(t *testing.T) {
	pConf, err := quickfixInputSpec().ParseYAML(`
connection_type: initiator
tls:
  enabled: true
  client_certs:
    - cert: CERT1
      key: KEY1
    - cert: CERT2
      key: KEY2
`, nil)
	require.NoError(t, err)

	_, err = parseFixTLS(pConf)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "client certificate")
}

func TestParseFixTLSRejectsRenegotiation(t *testing.T) {
	pConf, err := quickfixInputSpec().ParseYAML(`
connection_type: initiator
tls:
  enabled: true
  enable_renegotiation: true
`, nil)
	require.NoError(t, err)

	_, err = parseFixTLS(pConf)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "renegotiation")
}
