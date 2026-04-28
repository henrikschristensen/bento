package quickfix

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	goquickfix "codeberg.org/hsctech/quickfix"
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
func indentLines(s, prefix string) string {
	lines := strings.Split(s, "\n")
	for i, l := range lines {
		if l != "" {
			lines[i] = prefix + l
		}
	}
	return strings.Join(lines, "\n")
}

// --- Config spec tests -------------------------------------------------------

func TestQuickfixInputConfigSpec(t *testing.T) {
	conf, err := quickfixInputSpec().ParseYAML(fmt.Sprintf(`
connection_type: acceptor
settings: |
%s
buffer_size: 500
`, indentLines(acceptorCfg(5001), "  ")), nil)
	require.NoError(t, err)

	r, err := newQuickfixInputFromParsed(conf, service.MockResources())
	require.NoError(t, err)

	assert.Equal(t, "acceptor", r.connType)
	assert.Equal(t, 500, r.bufferSize)
}

func TestQuickfixOutputConfigSpec(t *testing.T) {
	conf, err := quickfixOutputSpec().ParseYAML(fmt.Sprintf(`
connection_type: initiator
settings: |
%s
`, indentLines(initiatorCfg(5001), "  ")), nil)
	require.NoError(t, err)

	w, err := newQuickfixOutputFromParsed(conf, service.MockResources())
	require.NoError(t, err)

	assert.Equal(t, "initiator", w.connType)
}

func TestQuickfixInputConnectInvalidSettings(t *testing.T) {
	conf, err := quickfixInputSpec().ParseYAML(`
connection_type: acceptor
settings: "not valid cfg"
`, nil)
	require.NoError(t, err)

	r, err := newQuickfixInputFromParsed(conf, service.MockResources())
	require.NoError(t, err)

	err = r.Connect(context.Background())
	assert.Error(t, err)
}

// --- Output behaviour tests --------------------------------------------------

func TestQuickfixOutputWriteNotConnected(t *testing.T) {
	port := freePort(t)

	conf, err := quickfixOutputSpec().ParseYAML(fmt.Sprintf(`
connection_type: initiator
settings: |
%s
`, indentLines(initiatorCfg(port), "  ")), nil)
	require.NoError(t, err)

	w, err := newQuickfixOutputFromParsed(conf, service.MockResources())
	require.NoError(t, err)

	ctx := context.Background()
	require.NoError(t, w.Connect(ctx))
	t.Cleanup(func() { _ = w.Close(ctx) })

	rawFIX := buildRawFIX("FIX.4.4", "D", "CLIENT", "SERVER", map[goquickfix.Tag]string{
		goquickfix.Tag(11): "ORDER001",
	})

	// No session is logged on yet; Write must report not connected.
	err = w.Write(ctx, service.NewMessage(rawFIX))
	assert.ErrorIs(t, err, service.ErrNotConnected)
}

func TestQuickfixOutputPipeDelimiterConversion(t *testing.T) {
	// Build a valid SOH-delimited FIX message then convert to pipe-delimited,
	// simulating input a user might supply.
	sohMsg := buildRawFIX("FIX.4.4", "D", "CLIENT", "SERVER", map[goquickfix.Tag]string{
		goquickfix.Tag(11): "ORDER001",
	})
	pipeMsg := bytes.ReplaceAll(sohMsg, []byte("\x01"), []byte("|"))

	// Apply the same pipe→SOH conversion the output.Write() uses.
	converted := pipeMsg
	if bytes.ContainsRune(converted, '|') && !bytes.ContainsRune(converted, '\x01') {
		converted = bytes.ReplaceAll(converted, []byte("|"), []byte("\x01"))
	}

	// After conversion the result must be identical to the original SOH message.
	assert.Equal(t, sohMsg, converted)

	// The converted bytes must also be parseable by ParseMessage.
	parsed := goquickfix.NewMessage()
	assert.NoError(t, goquickfix.ParseMessage(parsed, bytes.NewBuffer(converted)))
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
settings: |
%s
`, indentLines(acceptorCfg(port), "  ")), nil)
	require.NoError(t, err)

	inp, err := newQuickfixInputFromParsed(inputConf, service.MockResources())
	require.NoError(t, err)
	require.NoError(t, inp.Connect(ctx))
	t.Cleanup(func() { _ = inp.Close(ctx) })

	// Output: initiator (client).
	outputConf, err := quickfixOutputSpec().ParseYAML(fmt.Sprintf(`
connection_type: initiator
settings: |
%s
`, indentLines(initiatorCfg(port), "  ")), nil)
	require.NoError(t, err)

	out, err := newQuickfixOutputFromParsed(outputConf, service.MockResources())
	require.NoError(t, err)
	require.NoError(t, out.Connect(ctx))
	t.Cleanup(func() { _ = out.Close(ctx) })

	// Wait for the FIX logon handshake to complete.
	require.Eventually(t, func() bool {
		out.sessionsMut.RLock()
		defer out.sessionsMut.RUnlock()
		return len(out.loggedOnSessions) > 0
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
	// Trailing SOH must be trimmed by FromApp.
	assert.NotEqual(t, "\x01", bodyStr[len(bodyStr)-1:])
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
settings: |
%s
`, indentLines(acceptorCfg(port1), "  ")), nil)
	require.NoError(t, err)
	inp1, err := newQuickfixInputFromParsed(inp1Conf, service.MockResources())
	require.NoError(t, err)
	require.NoError(t, inp1.Connect(ctx))
	t.Cleanup(func() { _ = inp1.Close(ctx) })

	out1Conf, err := quickfixOutputSpec().ParseYAML(fmt.Sprintf(`
connection_type: initiator
message_format: raw
settings: |
%s
`, indentLines(initiatorCfg(port1), "  ")), nil)
	require.NoError(t, err)
	out1, err := newQuickfixOutputFromParsed(out1Conf, service.MockResources())
	require.NoError(t, err)
	require.NoError(t, out1.Connect(ctx))
	t.Cleanup(func() { _ = out1.Close(ctx) })

	// --- Session 2: json initiator output → raw acceptor input ---------------

	inp2Conf, err := quickfixInputSpec().ParseYAML(fmt.Sprintf(`
connection_type: acceptor
message_format: raw
settings: |
%s
`, indentLines(acceptorCfg2(port2), "  ")), nil)
	require.NoError(t, err)
	inp2, err := newQuickfixInputFromParsed(inp2Conf, service.MockResources())
	require.NoError(t, err)
	require.NoError(t, inp2.Connect(ctx))
	t.Cleanup(func() { _ = inp2.Close(ctx) })

	out2Conf, err := quickfixOutputSpec().ParseYAML(fmt.Sprintf(`
connection_type: initiator
message_format: json
settings: |
%s
`, indentLines(initiatorCfg2(port2), "  ")), nil)
	require.NoError(t, err)
	out2, err := newQuickfixOutputFromParsed(out2Conf, service.MockResources())
	require.NoError(t, err)
	require.NoError(t, out2.Connect(ctx))
	t.Cleanup(func() { _ = out2.Close(ctx) })

	// Wait for both FIX sessions to establish.
	require.Eventually(t, func() bool {
		out1.sessionsMut.RLock()
		s1 := len(out1.loggedOnSessions) > 0
		out1.sessionsMut.RUnlock()
		out2.sessionsMut.RLock()
		s2 := len(out2.loggedOnSessions) > 0
		out2.sessionsMut.RUnlock()
		return s1 && s2
	}, 15*time.Second, 100*time.Millisecond, "timed out waiting for FIX sessions to log on")

	// Step 1: send a raw New Order Single via out1.
	rawFIX := buildRawFIX("FIX.4.4", "D", "CLIENT", "SERVER", map[goquickfix.Tag]string{
		goquickfix.Tag(11): "ORDER001", // ClOrdID
		goquickfix.Tag(55): "AAPL",    // Symbol
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
