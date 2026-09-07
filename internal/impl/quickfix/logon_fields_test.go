package quickfix

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	goquickfix "github.com/quickfixgo/quickfix"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBuildLogonFields verifies that the credential and custom application
// settings a counterparty like LSEG requires are mapped onto the correct FIX
// tags, that session-level settings override globals, and that sessions without
// any of the settings are omitted so their logons stay untouched.
func TestBuildLogonFields(t *testing.T) {
	settings := `[DEFAULT]
ConnectionType=initiator
HeartBtInt=30
BeginString=FIXT.1.1
DefaultApplVerID=FIX.5.0SP1
DefaultCstmApplVerID=01.015.00
ApplicationSystemName=JYBCFIXTCRUAT
ApplicationSystemVersion=REC_JYBC_FIX_TCR

[SESSION]
SenderCompID=JYBCFIXTCRUAT
TargetCompID=RTNSFIXUAT
SocketConnectHost=127.0.0.1
SocketConnectPort=1234
Username=jybcfixtcruatLBN
Password=`

	parsed, err := goquickfix.ParseSettings(strings.NewReader(settings))
	require.NoError(t, err)

	fields := buildLogonFields(parsed)
	require.Len(t, fields, 1)

	var got map[goquickfix.Tag]string
	for _, f := range fields {
		got = f
	}

	assert.Equal(t, "jybcfixtcruatLBN", got[goquickfix.Tag(553)], "Username -> 553")
	assert.Equal(t, "01.015.00", got[goquickfix.Tag(1408)], "DefaultCstmApplVerID -> 1408 (from DEFAULT)")
	assert.Equal(t, "JYBCFIXTCRUAT", got[goquickfix.Tag(1603)], "ApplicationSystemName -> 1603")
	assert.Equal(t, "REC_JYBC_FIX_TCR", got[goquickfix.Tag(1604)], "ApplicationSystemVersion -> 1604")
	_, hasPassword := got[goquickfix.Tag(554)]
	assert.False(t, hasPassword, "Password present but empty (UAT/medio) must not emit 554")
}

// TestBuildLogonFieldsAbsentWhenUnconfigured proves a session with none of the
// logon settings (e.g. the celer session) produces no entry, guaranteeing its
// logon is not modified.
func TestBuildLogonFieldsAbsentWhenUnconfigured(t *testing.T) {
	parsed, err := goquickfix.ParseSettings(strings.NewReader(initiatorCfg(1234)))
	require.NoError(t, err)

	fields := buildLogonFields(parsed)
	assert.Empty(t, fields, "session without logon settings must not inject any fields")
}

// initiatorCfgWithLogonFields is initiatorCfg augmented with the LSEG-style
// logon settings so the outgoing Logon should carry tags 553/1408/1603/1604.
func initiatorCfgWithLogonFields(port int) string {
	return fmt.Sprintf(`[DEFAULT]
ConnectionType=initiator
HeartBtInt=30
SenderCompID=CLIENT
TargetCompID=SERVER
BeginString=FIX.4.4
ReconnectInterval=1

[SESSION]
SocketConnectHost=127.0.0.1
SocketConnectPort=%d
Username=jybcfixtcruatLBN
DefaultCstmApplVerID=01.015.00
ApplicationSystemName=JYBCFIXTCRUAT
ApplicationSystemVersion=REC_JYBC_FIX_TCR`, port)
}

// TestLogonFieldsInjectedOnWire stands up a real session and asserts the
// injected credential/application fields appear on the outgoing Logon (35=A)
// captured in the DEBUG wire log.
func TestLogonFieldsInjectedOnWire(t *testing.T) {
	buf := &syncBuffer{}
	res := resourcesAtLevel(buf, slog.LevelDebug)

	port := freePort(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	inputConf, err := quickfixInputSpec().ParseYAML(fmt.Sprintf(`
connection_type: acceptor
settings_file: %q
`, writeTempCfg(t, acceptorCfg(port))), nil)
	require.NoError(t, err)

	inp, err := newQuickfixInputFromParsed(inputConf, res)
	require.NoError(t, err)
	require.NoError(t, inp.Connect(ctx))
	t.Cleanup(func() { _ = inp.Close(ctx) })

	outputConf, err := quickfixOutputSpec().ParseYAML(fmt.Sprintf(`
connection_type: initiator
settings_file: %q
`, writeTempCfg(t, initiatorCfgWithLogonFields(port))), nil)
	require.NoError(t, err)

	out, err := newQuickfixOutputFromParsed(outputConf, res)
	require.NoError(t, err)
	require.NoError(t, out.Connect(ctx))
	t.Cleanup(func() { _ = out.Close(ctx) })

	require.Eventually(t, func() bool {
		return hasLoggedOn(out)
	}, 15*time.Second, 100*time.Millisecond, "timed out waiting for FIX session logon")

	logs := buf.String()
	// Isolate the outgoing Logon line.
	var logonLine string
	for line := range strings.SplitSeq(logs, "\n") {
		if strings.Contains(line, "FIX outgoing:") && strings.Contains(line, "35=A") {
			logonLine = line
			break
		}
	}
	require.NotEmpty(t, logonLine, "expected an outgoing Logon (35=A) in the DEBUG wire log")

	assert.Contains(t, logonLine, "553=jybcfixtcruatLBN", "Username (553) must be on the Logon")
	assert.Contains(t, logonLine, "1408=01.015.00", "DefaultCstmApplVerID (1408) must be on the Logon")
	assert.Contains(t, logonLine, "1603=JYBCFIXTCRUAT", "ApplicationSystemName (1603) must be on the Logon")
	assert.Contains(t, logonLine, "1604=REC_JYBC_FIX_TCR", "ApplicationSystemVersion (1604) must be on the Logon")
}
