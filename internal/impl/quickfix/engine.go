package quickfix

import (
	"fmt"
	"slices"
	"sync"

	goquickfix "github.com/quickfixgo/quickfix"

	"github.com/warpstreamlabs/bento/public/service"
)

// fixSubscriber is implemented by inputs and outputs that attach to a shared
// FIX engine. Callbacks from the engine's Application interface are fanned out
// to every subscriber so that a single underlying connection can serve any
// combination of inputs and outputs.
type fixSubscriber interface {
	onLogon(goquickfix.SessionID)
	onLogout(goquickfix.SessionID)
	fromApp(*goquickfix.Message, goquickfix.SessionID) goquickfix.MessageRejectError
}

//------------------------------------------------------------------------------
// engineBackend — the private seam behind the shared FIX engine

// engineBackend is the minimal surface the session module needs from an
// underlying FIX engine. Two adapters exist: quickfixBackend in production and
// an in-memory fake in tests, which allows lifecycle, refcounting, replay and
// send routing to be tested without standing up loopback FIX sessions.
type engineBackend interface {
	Start() error
	// Stop must be safe to call after a failed Start.
	Stop()
	SendToTarget(msg *goquickfix.Message, sessionID goquickfix.SessionID) error
}

// backendFactory constructs the backend for an engine about to be started.
type backendFactory func(app goquickfix.Application, connType string, qfSettings *goquickfix.Settings, store fixStore, log *service.Logger) (engineBackend, error)

// newEngineBackend is the backend factory used for newly created engines. It is
// a package-level variable so tests can substitute a fake factory; production
// code always sees newQuickfixBackend.
var newEngineBackend backendFactory = newQuickfixBackend

// quickfixBackend adapts a quickfixgo acceptor or initiator to engineBackend.
// SendToTarget routes through quickfixgo's process-wide session registry, which
// is unavoidable — the library exposes no per-session handle — but the session
// module validates ownership before a send ever reaches this point.
type quickfixBackend struct {
	start func() error
	stop  func()
}

func (b *quickfixBackend) Start() error { return b.start() }
func (b *quickfixBackend) Stop()        { b.stop() }

func (b *quickfixBackend) SendToTarget(msg *goquickfix.Message, sessionID goquickfix.SessionID) error {
	return goquickfix.SendToTarget(msg, sessionID)
}

func newQuickfixBackend(app goquickfix.Application, connType string, qfSettings *goquickfix.Settings, store fixStore, log *service.Logger) (engineBackend, error) {
	storeFactory := goquickfix.NewMemoryStoreFactory()
	if store != nil {
		f, err := store.storeFactory(qfSettings)
		if err != nil {
			return nil, err
		}
		storeFactory = f
	}
	logFactory := newBentoLogFactory(log)

	switch connType {
	case "acceptor":
		a, err := goquickfix.NewAcceptor(app, storeFactory, qfSettings, logFactory)
		if err != nil {
			return nil, err
		}
		return &quickfixBackend{start: a.Start, stop: a.Stop}, nil
	case "initiator":
		i, err := goquickfix.NewInitiator(app, storeFactory, qfSettings, logFactory)
		if err != nil {
			return nil, err
		}
		return &quickfixBackend{start: i.Start, stop: i.Stop}, nil
	default:
		return nil, fmt.Errorf("unknown connection_type %q", connType)
	}
}

//------------------------------------------------------------------------------
// sharedEngine

// sharedEngine wraps a single QuickFIX acceptor or initiator and multiplexes
// the Application callbacks across any number of attached subscribers.
type sharedEngine struct {
	name     string
	connType string
	settings string

	// qfSettings is the parsed form of settings, prepared once at
	// construction. start() consumes it directly rather than re-parsing.
	qfSettings *goquickfix.Settings
	tls        *fixTLSConfig
	store      fixStore

	log *service.Logger

	mu      sync.Mutex
	backend engineBackend
	started bool
	refs    int

	subsMu sync.RWMutex
	subs   []fixSubscriber

	// loggedOnMu guards loggedOnSessions.
	loggedOnMu       sync.RWMutex
	loggedOnSessions map[goquickfix.SessionID]struct{}

	// logonFields holds extra fields to append to the outgoing Logon message,
	// keyed by session. It is populated once during start() (before the engine
	// is started and any callback can fire) and is read-only thereafter, so it
	// needs no additional locking.
	logonFields map[goquickfix.SessionID]map[goquickfix.Tag]string
}

var (
	sharedReg   = map[string]*sharedEngine{}
	sharedRegMu sync.Mutex
)

// engineHandle is a refcounted lease on a shared FIX engine. Inputs and outputs
// interact with the engine exclusively through a handle: subscribing to
// callbacks, sending messages, and releasing the lease. The engine is stopped
// and forgotten once the last handle is released.
type engineHandle struct {
	eng *sharedEngine
	sub fixSubscriber
}

// acquireEngine returns a handle to the shared engine for cfg, creating and
// starting it if necessary, and subscribes sub to its callbacks. When the name
// refers to an existing engine the connection_type, settings and tls config (if
// supplied) must match the originally registered values.
func acquireEngine(cfg connConfig, log *service.Logger, sub fixSubscriber) (*engineHandle, error) {
	sharedRegMu.Lock()
	eng, ok := sharedReg[cfg.name]
	if !ok {
		if cfg.connType == "" || cfg.settings == "" {
			sharedRegMu.Unlock()
			return nil, fmt.Errorf("no existing quickfix connection named %q; the first input or output to reference a shared connection must provide connection_type and settings_file", cfg.name)
		}
		eng = &sharedEngine{
			name:             cfg.name,
			connType:         cfg.connType,
			settings:         cfg.settings,
			qfSettings:       cfg.qfSettings,
			tls:              cfg.tls,
			store:            cfg.store,
			log:              log,
			loggedOnSessions: make(map[goquickfix.SessionID]struct{}),
		}
		sharedReg[cfg.name] = eng
	} else {
		if cfg.connType != "" && cfg.connType != eng.connType {
			sharedRegMu.Unlock()
			return nil, fmt.Errorf("connection_type %q for shared quickfix connection %q conflicts with previously registered value %q", cfg.connType, cfg.name, eng.connType)
		}
		if cfg.settings != "" && cfg.settings != eng.settings {
			sharedRegMu.Unlock()
			return nil, fmt.Errorf("settings for shared quickfix connection %q conflict with the previously registered settings", cfg.name)
		}
		if cfg.tls != nil {
			if eng.tls == nil {
				sharedRegMu.Unlock()
				return nil, fmt.Errorf("tls config for shared quickfix connection %q conflicts with the previously registered connection, which has tls disabled", cfg.name)
			}
			if *cfg.tls != *eng.tls {
				sharedRegMu.Unlock()
				return nil, fmt.Errorf("tls config for shared quickfix connection %q conflicts with the previously registered tls config", cfg.name)
			}
		}
		if cfg.store != nil {
			if eng.store == nil {
				sharedRegMu.Unlock()
				return nil, fmt.Errorf("store config for shared quickfix connection %q conflicts with the previously registered connection, which has no store configured", cfg.name)
			}
			if !cfg.store.equal(eng.store) {
				sharedRegMu.Unlock()
				return nil, fmt.Errorf("store config for shared quickfix connection %q conflicts with the previously registered store config", cfg.name)
			}
		}
	}
	eng.refs++
	sharedRegMu.Unlock()

	eng.subscribe(sub)
	if err := eng.start(); err != nil {
		eng.unsubscribe(sub)
		releaseEngine(cfg.name)
		return nil, err
	}
	return &engineHandle{eng: eng, sub: sub}, nil
}

// releaseEngine decrements the engine's reference count and stops the
// underlying engine once the last user has released it.
func releaseEngine(name string) {
	sharedRegMu.Lock()
	defer sharedRegMu.Unlock()

	eng, ok := sharedReg[name]
	if !ok {
		return
	}
	eng.refs--
	if eng.refs > 0 {
		return
	}
	eng.stop()
	delete(sharedReg, name)
}

// resetSharedEngines stops every engine and clears the registry. It exists for
// tests, so a failing test cannot leak engines into the rest of the process.
func resetSharedEngines() {
	sharedRegMu.Lock()
	defer sharedRegMu.Unlock()
	for name, eng := range sharedReg {
		eng.stop()
		delete(sharedReg, name)
	}
}

// release unsubscribes the handle's subscriber and releases the engine. It is
// idempotent and safe to call on a nil handle.
func (h *engineHandle) release() {
	if h == nil || h.eng == nil {
		return
	}
	h.eng.unsubscribe(h.sub)
	releaseEngine(h.eng.name)
	h.eng = nil
}

// hasLoggedOnSessions reports whether the engine currently has any logged-on
// FIX session.
func (h *engineHandle) hasLoggedOnSessions() bool {
	h.eng.loggedOnMu.RLock()
	defer h.eng.loggedOnMu.RUnlock()
	return len(h.eng.loggedOnSessions) > 0
}

// send routes msg to the FIX session identified by its BeginString,
// SenderCompID and TargetCompID header fields. The session must belong to this
// engine and be logged on; otherwise the send fails rather than silently
// routing through quickfixgo's process-wide session registry to a session owned
// by a different engine.
func (h *engineHandle) send(msg *goquickfix.Message) error {
	sid, err := sessionIDFromMessage(msg)
	if err != nil {
		return err
	}

	h.eng.loggedOnMu.RLock()
	_, loggedOn := h.eng.loggedOnSessions[sid]
	h.eng.loggedOnMu.RUnlock()
	if !loggedOn {
		return fmt.Errorf("FIX session %s is not logged on to shared quickfix connection %q", sid, h.eng.name)
	}

	h.eng.mu.Lock()
	backend := h.eng.backend
	h.eng.mu.Unlock()
	if backend == nil {
		return service.ErrNotConnected
	}
	return backend.SendToTarget(msg, sid)
}

const (
	tagBeginString  goquickfix.Tag = 8
	tagSenderCompID goquickfix.Tag = 49
	tagTargetCompID goquickfix.Tag = 56
)

// sessionIDFromMessage derives the target session from a message's header
// fields, mirroring the routing quickfixgo's global Send performs.
func sessionIDFromMessage(msg *goquickfix.Message) (goquickfix.SessionID, error) {
	var sid goquickfix.SessionID
	for _, f := range []struct {
		tag  goquickfix.Tag
		dest *string
		name string
	}{
		{tagBeginString, &sid.BeginString, "BeginString"},
		{tagSenderCompID, &sid.SenderCompID, "SenderCompID"},
		{tagTargetCompID, &sid.TargetCompID, "TargetCompID"},
	} {
		v, err := msg.Header.GetString(f.tag)
		if err != nil {
			return sid, fmt.Errorf("FIX message is missing header field %s (%d)", f.name, int(f.tag))
		}
		*f.dest = v
	}
	return sid, nil
}

func (e *sharedEngine) subscribe(s fixSubscriber) {
	e.subsMu.Lock()
	if slices.Contains(e.subs, s) {
		e.subsMu.Unlock()
		return
	}
	e.subs = append(e.subs, s)
	e.subsMu.Unlock()

	// Replay currently-logged-on sessions to the new subscriber so that
	// components which subscribe after the FIX session is established (e.g.
	// lazily-initialized output resources) see the correct logon state and do
	// not incorrectly report ErrNotConnected. The read lock is held through the
	// replay so a concurrent OnLogout cannot leave the subscriber with stale
	// logon state; subscriber onLogon callbacks are cheap and never re-enter
	// the engine.
	e.loggedOnMu.RLock()
	defer e.loggedOnMu.RUnlock()
	for sid := range e.loggedOnSessions {
		s.onLogon(sid)
	}
}

func (e *sharedEngine) unsubscribe(s fixSubscriber) {
	e.subsMu.Lock()
	defer e.subsMu.Unlock()
	for i, existing := range e.subs {
		if existing == s {
			e.subs = append(e.subs[:i], e.subs[i+1:]...)
			return
		}
	}
}

// start initialises and starts the underlying acceptor or initiator. It is
// idempotent; subsequent calls are no-ops once the engine is running. If
// Start() fails after the backend has already registered sessions globally in
// goquickfix, the partially-constructed backend is stopped so the caller can
// safely retry.
func (e *sharedEngine) start() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.started {
		return nil
	}

	qfSettings := e.qfSettings
	if qfSettings == nil {
		return fmt.Errorf("shared quickfix connection %q has no parsed settings", e.name)
	}

	if e.tls != nil {
		applyTLSSettings(qfSettings, e.tls)
	}

	e.logonFields = buildLogonFields(qfSettings)

	backend, err := newEngineBackend(e, e.connType, qfSettings, e.store, e.log)
	if err != nil {
		return err
	}
	if err := backend.Start(); err != nil {
		backend.Stop()
		return err
	}
	e.backend = backend
	e.started = true
	return nil
}

func (e *sharedEngine) stop() {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.backend != nil {
		e.backend.Stop()
		e.backend = nil
	}
	e.started = false
}

//------------------------------------------------------------------------------
// Logon field injection

// logonSettingFields maps quickfix session-settings keys to the FIX tags that
// carry them on the outgoing Logon (35=A) message. Counterparties such as
// LSEG/Refinitiv require these credential and custom application-version fields
// in the Logon; when the corresponding settings are absent (e.g. the celer
// session) nothing is added and the logon is unchanged.
var logonSettingFields = []struct {
	setting string
	tag     goquickfix.Tag
}{
	{"Username", 553},
	{"Password", 554},
	{"DefaultCstmApplVerID", 1408},
	{"ApplicationSystemName", 1603},
	{"ApplicationSystemVersion", 1604},
}

const (
	tagMsgType   goquickfix.Tag = 35
	msgTypeLogon                = "A"
)

// buildLogonFields precomputes, for each configured session, the extra fields
// that ToAdmin must append to the outgoing Logon message. Session-level
// settings take precedence over global ([DEFAULT]) settings. Sessions with none
// of the configured logon settings are omitted so their logons are untouched.
func buildLogonFields(settings *goquickfix.Settings) map[goquickfix.SessionID]map[goquickfix.Tag]string {
	out := map[goquickfix.SessionID]map[goquickfix.Tag]string{}
	global := settings.GlobalSettings()
	for sid, ss := range settings.SessionSettings() {
		fields := map[goquickfix.Tag]string{}
		for _, lf := range logonSettingFields {
			val := lookupSetting(ss, global, lf.setting)
			if val != "" {
				fields[lf.tag] = val
			}
		}
		if len(fields) > 0 {
			out[sid] = fields
		}
	}
	return out
}

// lookupSetting returns the value of a setting, preferring the session-level
// value and falling back to the global ([DEFAULT]) value. An empty string is
// returned when the setting is absent or blank.
func lookupSetting(session, global *goquickfix.SessionSettings, key string) string {
	if session != nil && session.HasSetting(key) {
		if v, err := session.Setting(key); err == nil {
			return v
		}
	}
	if global != nil && global.HasSetting(key) {
		if v, err := global.Setting(key); err == nil {
			return v
		}
	}
	return ""
}

//------------------------------------------------------------------------------
// goquickfix.Application — fans out to every attached subscriber.

func (e *sharedEngine) OnCreate(sessionID goquickfix.SessionID) {}

func (e *sharedEngine) OnLogon(sessionID goquickfix.SessionID) {
	e.loggedOnMu.Lock()
	e.loggedOnSessions[sessionID] = struct{}{}
	e.loggedOnMu.Unlock()

	e.subsMu.RLock()
	subs := append([]fixSubscriber(nil), e.subs...)
	e.subsMu.RUnlock()
	for _, s := range subs {
		s.onLogon(sessionID)
	}
}

func (e *sharedEngine) OnLogout(sessionID goquickfix.SessionID) {
	e.loggedOnMu.Lock()
	delete(e.loggedOnSessions, sessionID)
	e.loggedOnMu.Unlock()

	e.subsMu.RLock()
	subs := append([]fixSubscriber(nil), e.subs...)
	e.subsMu.RUnlock()
	for _, s := range subs {
		s.onLogout(sessionID)
	}
}

func (e *sharedEngine) ToAdmin(message *goquickfix.Message, sessionID goquickfix.SessionID) {
	fields := e.logonFields[sessionID]
	if len(fields) == 0 {
		return
	}

	msgType, err := message.Header.GetString(tagMsgType)
	if err != nil || msgType != msgTypeLogon {
		return
	}

	for tag, val := range fields {
		message.Body.SetString(tag, val)
	}
}

func (e *sharedEngine) ToApp(message *goquickfix.Message, sessionID goquickfix.SessionID) error {
	return nil
}

func (e *sharedEngine) FromAdmin(message *goquickfix.Message, sessionID goquickfix.SessionID) goquickfix.MessageRejectError {
	return nil
}

// FromApp dispatches inbound application messages to every subscriber. The
// first non-nil reject error returned is propagated back to the QuickFIX
// engine. Subscribers that aren't interested in inbound messages (e.g. outputs)
// should return nil.
func (e *sharedEngine) FromApp(message *goquickfix.Message, sessionID goquickfix.SessionID) goquickfix.MessageRejectError {
	e.subsMu.RLock()
	subs := append([]fixSubscriber(nil), e.subs...)
	e.subsMu.RUnlock()
	for _, s := range subs {
		if rej := s.fromApp(message, sessionID); rej != nil {
			return rej
		}
	}
	return nil
}
