package quickfix

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	goquickfix "github.com/quickfixgo/quickfix"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/service"
)

const testEngineSettings = `[DEFAULT]
ConnectionType=acceptor
BeginString=FIX.4.4

[SESSION]
SenderCompID=A
TargetCompID=B
`

var testEngineSessionID = goquickfix.SessionID{
	BeginString:  "FIX.4.4",
	SenderCompID: "A",
	TargetCompID: "B",
}

// fakeBackend is the in-memory adapter at the engineBackend seam. It simulates
// logon of a fixed set of sessions on Start and records everything the module
// does, so engine lifecycle, refcounting, replay and send routing can be
// tested without loopback FIX sessions.
type fakeBackend struct {
	app      goquickfix.Application
	sessions []goquickfix.SessionID
	startErr error

	mu      sync.Mutex
	started bool
	stops   int
	sent    []*goquickfix.Message
}

func (f *fakeBackend) Start() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.startErr != nil {
		return f.startErr
	}
	f.started = true
	for _, sid := range f.sessions {
		f.app.OnLogon(sid)
	}
	return nil
}

func (f *fakeBackend) Stop() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.started = false
	f.stops++
}

func (f *fakeBackend) SendToTarget(msg *goquickfix.Message, sessionID goquickfix.SessionID) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sent = append(f.sent, msg)
	return nil
}

func (f *fakeBackend) sentCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.sent)
}

// installFakeBackend points the engine module at fb for the duration of the
// test and resets the engine registry afterwards so nothing leaks between
// tests.
func installFakeBackend(t *testing.T, fb *fakeBackend) {
	t.Helper()
	prev := newEngineBackend
	newEngineBackend = func(app goquickfix.Application, connType string, qfSettings *goquickfix.Settings, store fixStore, log *service.Logger) (engineBackend, error) {
		fb.app = app
		return fb, nil
	}
	t.Cleanup(func() {
		newEngineBackend = prev
		resetSharedEngines()
	})
}

type recordingSubscriber struct {
	mu      sync.Mutex
	logons  []goquickfix.SessionID
	logouts []goquickfix.SessionID
}

func (r *recordingSubscriber) onLogon(sid goquickfix.SessionID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.logons = append(r.logons, sid)
}

func (r *recordingSubscriber) onLogout(sid goquickfix.SessionID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.logouts = append(r.logouts, sid)
}

func (r *recordingSubscriber) fromApp(*goquickfix.Message, goquickfix.SessionID) goquickfix.MessageRejectError {
	return nil
}

func (r *recordingSubscriber) logonCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.logons)
}

func testLogger() *service.Logger {
	return service.MockResources().Logger()
}

// testConnConfig builds a connConfig for the shared test settings, with the
// settings parsed as parseConnConfig would.
func testConnConfig(name string) connConfig {
	qf, err := goquickfix.ParseSettings(strings.NewReader(testEngineSettings))
	if err != nil {
		panic(err)
	}
	return connConfig{name: name, connType: "acceptor", settings: testEngineSettings, qfSettings: qf}
}

func TestAcquireEngineRequiresConnectionDetails(t *testing.T) {
	installFakeBackend(t, &fakeBackend{})

	_, err := acquireEngine(connConfig{name: "nope"}, testLogger(), &recordingSubscriber{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "connection_type and settings_file")
}

func TestAcquireEngineConflicts(t *testing.T) {
	installFakeBackend(t, &fakeBackend{})

	cfg := testConnConfig("conf")
	h1, err := acquireEngine(cfg, testLogger(), &recordingSubscriber{})
	require.NoError(t, err)
	t.Cleanup(h1.release)

	_, err = acquireEngine(connConfig{name: "conf", connType: "initiator", settings: testEngineSettings}, testLogger(), &recordingSubscriber{})
	require.ErrorContains(t, err, "connection_type")

	_, err = acquireEngine(connConfig{name: "conf", connType: "acceptor", settings: testEngineSettings + "HeartBtInt=10\n"}, testLogger(), &recordingSubscriber{})
	require.ErrorContains(t, err, "settings")

	_, err = acquireEngine(connConfig{name: "conf", connType: "acceptor", settings: testEngineSettings, tls: &fixTLSConfig{enabled: true}}, testLogger(), &recordingSubscriber{})
	require.ErrorContains(t, err, "tls")
}

func TestAcquireEngineTLSMismatch(t *testing.T) {
	installFakeBackend(t, &fakeBackend{})

	cfg := testConnConfig("tlsconn")
	cfg.tls = &fixTLSConfig{enabled: true, serverName: "fix.example.com"}
	h1, err := acquireEngine(cfg, testLogger(), &recordingSubscriber{})
	require.NoError(t, err)
	t.Cleanup(h1.release)

	different := cfg
	different.tls = &fixTLSConfig{enabled: true, serverName: "other.example.com"}
	_, err = acquireEngine(different, testLogger(), &recordingSubscriber{})
	require.ErrorContains(t, err, "tls")

	// Omitting tls inherits the first registrant's config.
	inherit := cfg
	inherit.tls = nil
	h2, err := acquireEngine(inherit, testLogger(), &recordingSubscriber{})
	require.NoError(t, err)
	h2.release()
}

func TestEngineRefcountLifecycle(t *testing.T) {
	fb := &fakeBackend{sessions: []goquickfix.SessionID{testEngineSessionID}}
	installFakeBackend(t, fb)

	cfg := testConnConfig("shared")

	h1, err := acquireEngine(cfg, testLogger(), &recordingSubscriber{})
	require.NoError(t, err)
	h2, err := acquireEngine(cfg, testLogger(), &recordingSubscriber{})
	require.NoError(t, err)

	assert.Same(t, h1.eng, h2.eng, "two handles on one name share the engine")
	assert.True(t, fb.started)

	h1.release()
	assert.Equal(t, 0, fb.stops, "engine must keep running while a handle remains")

	h2.release()
	assert.Equal(t, 1, fb.stops, "last release stops the engine")
	assert.Empty(t, sharedReg, "last release removes the engine from the registry")

	// A later acquire gets a fresh engine and starts it again.
	h3, err := acquireEngine(cfg, testLogger(), &recordingSubscriber{})
	require.NoError(t, err)
	assert.NotSame(t, h1.eng, h3.eng)
	assert.True(t, fb.started)
	h3.release()
}

func TestAcquireEngineStartFailure(t *testing.T) {
	fb := &fakeBackend{startErr: errors.New("boom")}
	installFakeBackend(t, fb)

	cfg := testConnConfig("failing")

	_, err := acquireEngine(cfg, testLogger(), &recordingSubscriber{})
	require.ErrorContains(t, err, "boom")

	assert.Equal(t, 1, fb.stops, "failed start must stop the partially-started backend")
	assert.Empty(t, sharedReg, "failed acquire must not leave the engine registered")

	fb.mu.Lock()
	fb.startErr = nil
	fb.mu.Unlock()

	h, err := acquireEngine(cfg, testLogger(), &recordingSubscriber{})
	require.NoError(t, err, "acquire can be retried after a start failure")
	h.release()
}

func TestSubscribeReplaysLoggedOnSessions(t *testing.T) {
	fb := &fakeBackend{sessions: []goquickfix.SessionID{testEngineSessionID}}
	installFakeBackend(t, fb)

	cfg := testConnConfig("replay")

	sub1 := &recordingSubscriber{}
	h1, err := acquireEngine(cfg, testLogger(), sub1)
	require.NoError(t, err)
	t.Cleanup(h1.release)
	require.Equal(t, 1, sub1.logonCount())

	// A subscriber attaching after the session logged on sees the logon state.
	sub2 := &recordingSubscriber{}
	h2, err := acquireEngine(cfg, testLogger(), sub2)
	require.NoError(t, err)
	t.Cleanup(h2.release)

	require.Equal(t, 1, sub2.logonCount())
	assert.Equal(t, testEngineSessionID, sub2.logons[0])
}

func TestHandleSendValidation(t *testing.T) {
	fb := &fakeBackend{sessions: []goquickfix.SessionID{testEngineSessionID}}
	installFakeBackend(t, fb)

	cfg := testConnConfig("sender")
	h, err := acquireEngine(cfg, testLogger(), &recordingSubscriber{})
	require.NoError(t, err)
	t.Cleanup(h.release)

	newMsg := func(beginString, sender, target string) *goquickfix.Message {
		m := goquickfix.NewMessage()
		m.Header.SetString(tagBeginString, beginString)
		m.Header.SetString(tagSenderCompID, sender)
		m.Header.SetString(tagTargetCompID, target)
		return m
	}

	// Happy path: the session belongs to this engine and is logged on.
	require.NoError(t, h.send(newMsg("FIX.4.4", "A", "B")))
	assert.Equal(t, 1, fb.sentCount())

	// A session this engine doesn't own must fail, not silently route through
	// quickfixgo's global registry.
	err = h.send(newMsg("FIX.4.4", "A", "ZZZ"))
	require.ErrorContains(t, err, "not logged on")
	assert.Equal(t, 1, fb.sentCount(), "rejected message must not reach the backend")

	// A message without routing headers cannot be sent at all.
	err = h.send(goquickfix.NewMessage())
	require.ErrorContains(t, err, "missing header field")
}

func TestQuickfixOutputWriteNotConnected(t *testing.T) {
	// No sessions configured on the fake, so nothing ever logs on.
	installFakeBackend(t, &fakeBackend{})

	conf, err := quickfixOutputSpec().ParseYAML(`
connection_type: initiator
settings_file: `+writeTempCfg(t, testEngineSettings)+`
`, nil)
	require.NoError(t, err)

	w, err := newQuickfixOutputFromParsed(conf, service.MockResources())
	require.NoError(t, err)

	ctx := context.Background()
	require.NoError(t, w.Connect(ctx))
	t.Cleanup(func() { _ = w.Close(ctx) })

	rawFIX := buildRawFIX("FIX.4.4", "D", "CLIENT", "SERVER", map[goquickfix.Tag]string{
		goquickfix.Tag(11): "ORDER001",
	})
	err = w.Write(ctx, service.NewMessage(rawFIX))
	assert.ErrorIs(t, err, service.ErrNotConnected)
}

func TestQuickfixOutputWriteSendsViaOwnedSession(t *testing.T) {
	fb := &fakeBackend{sessions: []goquickfix.SessionID{{
		BeginString:  "FIX.4.4",
		SenderCompID: "CLIENT",
		TargetCompID: "SERVER",
	}}}
	installFakeBackend(t, fb)

	conf, err := quickfixOutputSpec().ParseYAML(`
connection_type: initiator
settings_file: `+writeTempCfg(t, testEngineSettings)+`
`, nil)
	require.NoError(t, err)

	w, err := newQuickfixOutputFromParsed(conf, service.MockResources())
	require.NoError(t, err)

	ctx := context.Background()
	require.NoError(t, w.Connect(ctx))
	t.Cleanup(func() { _ = w.Close(ctx) })

	rawFIX := buildRawFIX("FIX.4.4", "D", "CLIENT", "SERVER", map[goquickfix.Tag]string{
		goquickfix.Tag(11): "ORDER001",
	})
	require.NoError(t, w.Write(ctx, service.NewMessage(rawFIX)))
	assert.Equal(t, 1, fb.sentCount())

	// A message addressed to a session the engine doesn't own is rejected.
	foreign := buildRawFIX("FIX.4.4", "D", "CLIENT", "SOMEWHERE_ELSE", map[goquickfix.Tag]string{
		goquickfix.Tag(11): "ORDER002",
	})
	err = w.Write(ctx, service.NewMessage(foreign))
	require.ErrorContains(t, err, "not logged on")
	assert.Equal(t, 1, fb.sentCount())
}
