package quickfix

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	goquickfix "github.com/quickfixgo/quickfix"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/service"
)

// freePort returns an available TCP port on localhost.
func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()
	return port
}

// buildRawFIX constructs a properly serialised FIX message (with correct
// BodyLength and CheckSum) using the quickfix library so it can be parsed back
// without error.
func buildRawFIX(beginString, msgType, senderCompID, targetCompID string, bodyTags map[goquickfix.Tag]string) []byte {
	msg := goquickfix.NewMessage()
	msg.Header.SetField(goquickfix.Tag(8), goquickfix.FIXString(beginString))
	msg.Header.SetField(goquickfix.Tag(35), goquickfix.FIXString(msgType))
	msg.Header.SetField(goquickfix.Tag(49), goquickfix.FIXString(senderCompID))
	msg.Header.SetField(goquickfix.Tag(56), goquickfix.FIXString(targetCompID))
	for tag, val := range bodyTags {
		msg.Body.SetField(tag, goquickfix.FIXString(val))
	}
	return msg.Bytes()
}

// hasLoggedOn reports whether the output's shared engine currently has a
// logged-on FIX session.
func hasLoggedOn(o *quickfixOutput) bool {
	h := o.att.get()
	return h != nil && h.hasLoggedOnSessions()
}

// acceptorCfg returns QuickFIX settings for a server (acceptor) session.
func acceptorCfg(port int) string {
	return fmt.Sprintf(`[DEFAULT]
ConnectionType=acceptor
HeartBtInt=30
SenderCompID=SERVER
TargetCompID=CLIENT
BeginString=FIX.4.4

[SESSION]
SocketAcceptPort=%d`, port)
}

// initiatorCfg returns QuickFIX settings for a client (initiator) session.
func initiatorCfg(port int) string {
	return fmt.Sprintf(`[DEFAULT]
ConnectionType=initiator
HeartBtInt=30
SenderCompID=CLIENT
TargetCompID=SERVER
BeginString=FIX.4.4
ReconnectInterval=1

[SESSION]
SocketConnectHost=127.0.0.1
SocketConnectPort=%d`, port)
}

// acceptorCfg2 returns QuickFIX settings for a second server session with different comp IDs.
func acceptorCfg2(port int) string {
	return fmt.Sprintf(`[DEFAULT]
ConnectionType=acceptor
HeartBtInt=30
SenderCompID=SERVER2
TargetCompID=CLIENT2
BeginString=FIX.4.4

[SESSION]
SocketAcceptPort=%d`, port)
}

// initiatorCfg2 returns QuickFIX settings for a second client session with different comp IDs.
func initiatorCfg2(port int) string {
	return fmt.Sprintf(`[DEFAULT]
ConnectionType=initiator
HeartBtInt=30
SenderCompID=CLIENT2
TargetCompID=SERVER2
BeginString=FIX.4.4
ReconnectInterval=1

[SESSION]
SocketConnectHost=127.0.0.1
SocketConnectPort=%d`, port)
}

// --- Config spec tests -------------------------------------------------------

func TestQuickfixInputConfigSpec(t *testing.T) {
	conf, err := quickfixInputSpec().ParseYAML(fmt.Sprintf(`
connection_type: acceptor
settings_file: %q
buffer_size: 500
`, writeTempCfg(t, acceptorCfg(5001))), nil)
	require.NoError(t, err)

	r, err := newQuickfixInputFromParsed(conf, service.MockResources())
	require.NoError(t, err)

	assert.Equal(t, "acceptor", r.cfg.connType)
	assert.Equal(t, 500, r.bufferSize)
}

func TestQuickfixOutputConfigSpec(t *testing.T) {
	conf, err := quickfixOutputSpec().ParseYAML(fmt.Sprintf(`
connection_type: initiator
settings_file: %q
`, writeTempCfg(t, initiatorCfg(5001))), nil)
	require.NoError(t, err)

	w, err := newQuickfixOutputFromParsed(conf, service.MockResources())
	require.NoError(t, err)

	assert.Equal(t, "initiator", w.cfg.connType)
}

func TestQuickfixInputInvalidSettings(t *testing.T) {
	conf, err := quickfixInputSpec().ParseYAML(fmt.Sprintf(`
connection_type: acceptor
settings_file: %q
`, writeTempCfg(t, "not valid cfg")), nil)
	require.NoError(t, err)

	// The FIX settings are parsed at construction, so invalid cfg fails here
	// rather than at Connect time.
	_, err = newQuickfixInputFromParsed(conf, service.MockResources())
	assert.Error(t, err)
}

// --- Output behaviour tests --------------------------------------------------

func TestQuickfixOutputPipeDelimiterConversion(t *testing.T) {
	// Build a valid SOH-delimited FIX message then convert to pipe-delimited,
	// simulating input a user might supply.
	sohMsg := buildRawFIX("FIX.4.4", "D", "CLIENT", "SERVER", map[goquickfix.Tag]string{
		goquickfix.Tag(11): "ORDER001",
	})
	pipeMsg := bytes.ReplaceAll(sohMsg, []byte("\x01"), []byte("|"))

	// The real conversion must restore the original SOH message.
	assert.Equal(t, sohMsg, normaliseSOH(pipeMsg))
	// SOH-delimited input passes through untouched.
	assert.Equal(t, sohMsg, normaliseSOH(sohMsg))

	// A pipe-delimited payload must parse back into an equivalent message.
	parsed := goquickfix.NewMessage()
	require.NoError(t, parseRawFIX(parsed, pipeMsg, nil))
	assert.Equal(t, "ORDER001", mustBodyString(t, parsed, 11))
}

func mustBodyString(t *testing.T, msg *goquickfix.Message, tag goquickfix.Tag) string {
	t.Helper()
	v, err := msg.Body.GetString(tag)
	require.NoError(t, err)
	return v
}

// --- Round-trip test ---------------------------------------------------------

// TestQuickfixRoundTrip starts an acceptor input and an initiator output,
// waits for the FIX session to log on, then sends an application message from
// the output and verifies it is received by the input.
func TestQuickfixRoundTrip(t *testing.T) {
	port := freePort(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Input: acceptor (server).
	inputConf, err := quickfixInputSpec().ParseYAML(fmt.Sprintf(`
connection_type: acceptor
settings_file: %q
`, writeTempCfg(t, acceptorCfg(port))), nil)
	require.NoError(t, err)

	inp, err := newQuickfixInputFromParsed(inputConf, service.MockResources())
	require.NoError(t, err)
	require.NoError(t, inp.Connect(ctx))
	t.Cleanup(func() { _ = inp.Close(ctx) })

	// Output: initiator (client).
	outputConf, err := quickfixOutputSpec().ParseYAML(fmt.Sprintf(`
connection_type: initiator
settings_file: %q
`, writeTempCfg(t, initiatorCfg(port))), nil)
	require.NoError(t, err)

	out, err := newQuickfixOutputFromParsed(outputConf, service.MockResources())
	require.NoError(t, err)
	require.NoError(t, out.Connect(ctx))
	t.Cleanup(func() { _ = out.Close(ctx) })

	// Wait for the FIX logon handshake to complete.
	require.Eventually(t, func() bool {
		return hasLoggedOn(out)
	}, 15*time.Second, 100*time.Millisecond, "timed out waiting for FIX session logon")

	// Send an application-level New Order Single (MsgType=D) from the output.
	rawFIX := buildRawFIX("FIX.4.4", "D", "CLIENT", "SERVER", map[goquickfix.Tag]string{
		goquickfix.Tag(11): "ORDER001",
	})
	require.NoError(t, out.Write(ctx, service.NewMessage(rawFIX)))

	// Read the message on the acceptor input.
	readCtx, readCancel := context.WithTimeout(ctx, 10*time.Second)
	defer readCancel()

	msg, ackFn, err := inp.Read(readCtx)
	require.NoError(t, err)
	require.NoError(t, ackFn(ctx, nil))

	body, err := msg.AsBytes()
	require.NoError(t, err)
	bodyStr := string(body)

	assert.Contains(t, bodyStr, "35=D\x01")
	assert.Contains(t, bodyStr, "11=ORDER001\x01")
}

// --- JSON pipeline round-trip test -------------------------------------------

// TestQuickfixJSONPipelineRoundTrip simulates a bento pipeline where:
//  1. An initiator output sends a raw FIX New Order Single to an acceptor input
//     configured with message_format=json.
//  2. The acceptor input emits the message as JSON; the test modifies a field
//     (ClOrdID) to simulate a bloblang/processor transform.
//  3. The modified JSON is written to a second initiator output configured with
//     message_format=json, which re-encodes it to FIX and sends it to a second
//     acceptor input (raw format).
//  4. The second acceptor verifies that the field modification is present in the
//     received FIX message.
func TestQuickfixJSONPipelineRoundTrip(t *testing.T) {
	port1 := freePort(t)
	port2 := freePort(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// --- Session 1: raw initiator output → json acceptor input ---------------

	inp1Conf, err := quickfixInputSpec().ParseYAML(fmt.Sprintf(`
connection_type: acceptor
message_format: json
settings_file: %q
`, writeTempCfg(t, acceptorCfg(port1))), nil)
	require.NoError(t, err)
	inp1, err := newQuickfixInputFromParsed(inp1Conf, service.MockResources())
	require.NoError(t, err)
	require.NoError(t, inp1.Connect(ctx))
	t.Cleanup(func() { _ = inp1.Close(ctx) })

	out1Conf, err := quickfixOutputSpec().ParseYAML(fmt.Sprintf(`
connection_type: initiator
message_format: raw
settings_file: %q
`, writeTempCfg(t, initiatorCfg(port1))), nil)
	require.NoError(t, err)
	out1, err := newQuickfixOutputFromParsed(out1Conf, service.MockResources())
	require.NoError(t, err)
	require.NoError(t, out1.Connect(ctx))
	t.Cleanup(func() { _ = out1.Close(ctx) })

	// --- Session 2: json initiator output → raw acceptor input ---------------

	inp2Conf, err := quickfixInputSpec().ParseYAML(fmt.Sprintf(`
connection_type: acceptor
message_format: raw
settings_file: %q
`, writeTempCfg(t, acceptorCfg2(port2))), nil)
	require.NoError(t, err)
	inp2, err := newQuickfixInputFromParsed(inp2Conf, service.MockResources())
	require.NoError(t, err)
	require.NoError(t, inp2.Connect(ctx))
	t.Cleanup(func() { _ = inp2.Close(ctx) })

	out2Conf, err := quickfixOutputSpec().ParseYAML(fmt.Sprintf(`
connection_type: initiator
message_format: json
settings_file: %q
`, writeTempCfg(t, initiatorCfg2(port2))), nil)
	require.NoError(t, err)
	out2, err := newQuickfixOutputFromParsed(out2Conf, service.MockResources())
	require.NoError(t, err)
	require.NoError(t, out2.Connect(ctx))
	t.Cleanup(func() { _ = out2.Close(ctx) })

	// Wait for both FIX sessions to establish.
	require.Eventually(t, func() bool {
		return hasLoggedOn(out1) && hasLoggedOn(out2)
	}, 15*time.Second, 100*time.Millisecond, "timed out waiting for FIX sessions to log on")

	// Step 1: send a raw New Order Single via out1.
	rawFIX := buildRawFIX("FIX.4.4", "D", "CLIENT", "SERVER", map[goquickfix.Tag]string{
		goquickfix.Tag(11): "ORDER001", // ClOrdID
		goquickfix.Tag(55): "AAPL",     // Symbol
	})
	require.NoError(t, out1.Write(ctx, service.NewMessage(rawFIX)))

	// Step 2: read the JSON message from inp1.
	readCtx, readCancel := context.WithTimeout(ctx, 10*time.Second)
	defer readCancel()

	jsonMsg, ackFn, err := inp1.Read(readCtx)
	require.NoError(t, err)
	require.NoError(t, ackFn(ctx, nil))

	jsonBytes, err := jsonMsg.AsBytes()
	require.NoError(t, err)

	// Verify it really is JSON with the expected fields.
	var envelope map[string]map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(jsonBytes, &envelope), "input should emit valid JSON")
	body := envelope["Body"]
	require.NotNil(t, body)

	var clOrdID string
	require.NoError(t, json.Unmarshal(body["11"], &clOrdID))
	assert.Equal(t, "ORDER001", clOrdID)

	// Step 3: simulate a pipeline transform — change ClOrdID to "ORDER002" and
	// reroute to the second session's comp IDs.
	body["11"] = json.RawMessage(`"ORDER002"`)
	envelope["Body"] = body
	header := envelope["Header"]
	header["49"] = json.RawMessage(`"CLIENT2"`) // SenderCompID
	header["56"] = json.RawMessage(`"SERVER2"`) // TargetCompID
	envelope["Header"] = header
	modifiedJSON, err := json.Marshal(envelope)
	require.NoError(t, err)

	// Step 4: send the modified JSON via out2 (json mode).
	require.NoError(t, out2.Write(ctx, service.NewMessage(modifiedJSON)))

	// Step 5: read the resulting raw FIX from inp2 and assert the modification.
	readCtx2, readCancel2 := context.WithTimeout(ctx, 10*time.Second)
	defer readCancel2()

	fixMsg, ackFn2, err := inp2.Read(readCtx2)
	require.NoError(t, err)
	require.NoError(t, ackFn2(ctx, nil))

	fixBytes, err := fixMsg.AsBytes()
	require.NoError(t, err)
	fixStr := string(fixBytes)

	assert.Contains(t, fixStr, "11=ORDER002\x01", "ClOrdID should reflect the pipeline modification")
	assert.Contains(t, fixStr, "55=AAPL\x01", "Symbol should be preserved through the JSON round-trip")
	assert.Contains(t, fixStr, "35=D\x01", "MsgType should be preserved")
}

// --- Shared connection tests -------------------------------------------------

// TestQuickfixSharedConnectionRoundTrip verifies that an input and an output
// can share a single underlying QuickFIX engine via the `name` field, and that
// the connection can be initiated by either component.
func TestQuickfixSharedConnectionRoundTrip(t *testing.T) {
	port := freePort(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Server side: a single acceptor shared by the receiving input and the
	// sending output, started via the output's Connect call first.
	serverSettings := acceptorCfg(port)

	serverSettingsPath := writeTempCfg(t, serverSettings)

	serverOutConf, err := quickfixOutputSpec().ParseYAML(fmt.Sprintf(`
name: server
connection_type: acceptor
settings_file: %q
`, serverSettingsPath), nil)
	require.NoError(t, err)
	serverOut, err := newQuickfixOutputFromParsed(serverOutConf, service.MockResources())
	require.NoError(t, err)
	require.NoError(t, serverOut.Connect(ctx))
	t.Cleanup(func() { _ = serverOut.Close(ctx) })

	serverInConf, err := quickfixInputSpec().ParseYAML(fmt.Sprintf(`
name: server
connection_type: acceptor
settings_file: %q
`, serverSettingsPath), nil)
	require.NoError(t, err)
	serverIn, err := newQuickfixInputFromParsed(serverInConf, service.MockResources())
	require.NoError(t, err)
	require.NoError(t, serverIn.Connect(ctx))
	t.Cleanup(func() { _ = serverIn.Close(ctx) })

	// Both server-side components must refer to the same shared engine.
	assert.Same(t, serverOut.att.get().eng, serverIn.att.get().eng, "shared engine should be reused")

	// Client side: initiator output (anonymous, no shared name).
	clientOutConf, err := quickfixOutputSpec().ParseYAML(fmt.Sprintf(`
connection_type: initiator
settings_file: %q
`, writeTempCfg(t, initiatorCfg(port))), nil)
	require.NoError(t, err)
	clientOut, err := newQuickfixOutputFromParsed(clientOutConf, service.MockResources())
	require.NoError(t, err)
	require.NoError(t, clientOut.Connect(ctx))
	t.Cleanup(func() { _ = clientOut.Close(ctx) })

	// Wait for the FIX logon handshake on both ends.
	require.Eventually(t, func() bool {
		return hasLoggedOn(clientOut) && hasLoggedOn(serverOut)
	}, 15*time.Second, 100*time.Millisecond, "timed out waiting for FIX session logon")

	// Client -> server: the message must be delivered to the shared input on
	// the server side, even though the output started the connection.
	rawFIX := buildRawFIX("FIX.4.4", "D", "CLIENT", "SERVER", map[goquickfix.Tag]string{
		goquickfix.Tag(11): "ORDER_SHARED",
	})
	require.NoError(t, clientOut.Write(ctx, service.NewMessage(rawFIX)))

	readCtx, readCancel := context.WithTimeout(ctx, 10*time.Second)
	defer readCancel()
	msg, ackFn, err := serverIn.Read(readCtx)
	require.NoError(t, err)
	require.NoError(t, ackFn(ctx, nil))

	body, err := msg.AsBytes()
	require.NoError(t, err)
	assert.Contains(t, string(body), "11=ORDER_SHARED\x01")

	// Server -> client: the shared output sends back via the same engine.
	reply := buildRawFIX("FIX.4.4", "8", "SERVER", "CLIENT", map[goquickfix.Tag]string{
		goquickfix.Tag(11): "REPLY_SHARED",
	})
	require.NoError(t, serverOut.Write(ctx, service.NewMessage(reply)))
}

// TestQuickfixSharedConnectionMismatchedSettings verifies that joining an
// existing shared connection with different settings is rejected.
func TestQuickfixSharedConnectionMismatchedSettings(t *testing.T) {
	port := freePort(t)
	otherPort := freePort(t)

	ctx := context.Background()

	firstConf, err := quickfixInputSpec().ParseYAML(fmt.Sprintf(`
name: conflict
connection_type: acceptor
settings_file: %q
`, writeTempCfg(t, acceptorCfg(port))), nil)
	require.NoError(t, err)
	first, err := newQuickfixInputFromParsed(firstConf, service.MockResources())
	require.NoError(t, err)
	require.NoError(t, first.Connect(ctx))
	t.Cleanup(func() { _ = first.Close(ctx) })

	// Same name but different settings — must fail.
	secondConf, err := quickfixOutputSpec().ParseYAML(fmt.Sprintf(`
name: conflict
connection_type: acceptor
settings_file: %q
`, writeTempCfg(t, acceptorCfg(otherPort))), nil)
	require.NoError(t, err)
	second, err := newQuickfixOutputFromParsed(secondConf, service.MockResources())
	require.NoError(t, err)
	err = second.Connect(ctx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "settings")

	// Same name but different connection_type — must fail.
	thirdConf, err := quickfixOutputSpec().ParseYAML(fmt.Sprintf(`
name: conflict
connection_type: initiator
settings_file: %q
`, writeTempCfg(t, initiatorCfg(port))), nil)
	require.NoError(t, err)
	third, err := newQuickfixOutputFromParsed(thirdConf, service.MockResources())
	require.NoError(t, err)
	err = third.Connect(ctx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "connection_type")
}

// --- settings_file resolution tests -----------------------------------------

func writeTempCfg(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "session.cfg")
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
	return path
}

func TestQuickfixSettingsFileInterpolation(t *testing.T) {
	t.Setenv("TEST_SENDER_COMPID", "OPENINTEGRATION")
	t.Setenv("TEST_CONNECT_PORT", "5111")
	path := writeTempCfg(t, `[DEFAULT]
ConnectionType=initiator
HeartBtInt=30
SenderCompID=${TEST_SENDER_COMPID}
TargetCompID=CELERTECH
BeginString=FIX.4.4

[SESSION]
SocketConnectHost=localhost
SocketConnectPort=${TEST_CONNECT_PORT}`)

	conf, err := quickfixInputSpec().ParseYAML(fmt.Sprintf(`
connection_type: initiator
settings_file: %q
`, path), nil)
	require.NoError(t, err)

	r, err := newQuickfixInputFromParsed(conf, service.MockResources())
	require.NoError(t, err)

	assert.Contains(t, r.cfg.settings, "SenderCompID=OPENINTEGRATION")
	assert.Contains(t, r.cfg.settings, "SocketConnectPort=5111")
	assert.NotContains(t, r.cfg.settings, "${")
}

func TestQuickfixSettingsFileLeavesBareDollarUntouched(t *testing.T) {
	t.Setenv("TEST_SENDER_COMPID", "OPENINTEGRATION")
	path := writeTempCfg(t, `[DEFAULT]
ConnectionType=initiator
SenderCompID=${TEST_SENDER_COMPID}
# a literal $PATH-like token must not be treated as a variable
BeginString=FIX.4.4

[SESSION]
SocketConnectPort=5001`)

	conf, err := quickfixInputSpec().ParseYAML(fmt.Sprintf(`
connection_type: initiator
settings_file: %q
`, path), nil)
	require.NoError(t, err)

	r, err := newQuickfixInputFromParsed(conf, service.MockResources())
	require.NoError(t, err)
	assert.Contains(t, r.cfg.settings, "SenderCompID=OPENINTEGRATION")
	assert.Contains(t, r.cfg.settings, "$PATH-like")
}

func TestQuickfixSettingsFileMissingEnvVar(t *testing.T) {
	path := writeTempCfg(t, `[DEFAULT]
ConnectionType=initiator
SenderCompID=${TEST_DEFINITELY_UNSET_VAR}
BeginString=FIX.4.4

[SESSION]
SocketConnectPort=5001`)

	conf, err := quickfixInputSpec().ParseYAML(fmt.Sprintf(`
connection_type: initiator
settings_file: %q
`, path), nil)
	require.NoError(t, err)

	_, err = newQuickfixInputFromParsed(conf, service.MockResources())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "TEST_DEFINITELY_UNSET_VAR")
}

func TestQuickfixSettingsFileMissing(t *testing.T) {
	conf, err := quickfixInputSpec().ParseYAML(`
connection_type: initiator
settings_file: /no/such/path/session.cfg
`, nil)
	require.NoError(t, err)

	_, err = newQuickfixInputFromParsed(conf, service.MockResources())
	require.Error(t, err)
}

func TestQuickfixSettingsFileNotProvided(t *testing.T) {
	conf, err := quickfixInputSpec().ParseYAML(`
connection_type: initiator
`, nil)
	require.NoError(t, err)

	_, err = newQuickfixInputFromParsed(conf, service.MockResources())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "settings_file")
}
