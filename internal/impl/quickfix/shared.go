package quickfix

import (
	"crypto/tls"
	"fmt"
	"strings"
	"sync"

	goquickfix "codeberg.org/hsctech/quickfix"
	"codeberg.org/hsctech/quickfix/config"
	natsstore "codeberg.org/hsctech/quickfix/store/nats"

	"github.com/warpstreamlabs/bento/public/service"
)

// fixSubscriber is implemented by inputs and outputs that attach to a shared
// QuickFIX engine. Callbacks from the engine's Application interface are fanned
// out to every subscriber so that a single underlying connection can serve any
// combination of inputs and outputs.
type fixSubscriber interface {
	onLogon(goquickfix.SessionID)
	onLogout(goquickfix.SessionID)
	fromApp(*goquickfix.Message, goquickfix.SessionID) goquickfix.MessageRejectError
}

// sharedEngine wraps a single QuickFIX acceptor or initiator and multiplexes
// the Application callbacks across any number of attached subscribers.
type sharedEngine struct {
	name     string
	connType string
	settings string
	tlsConf  *tls.Config

	log *service.Logger

	mu        sync.Mutex
	acceptor  *goquickfix.Acceptor
	initiator *goquickfix.Initiator
	started   bool
	refs      int

	subsMu sync.RWMutex
	subs   []fixSubscriber

	// loggedOnMu guards loggedOnSessions.
	loggedOnMu      sync.RWMutex
	loggedOnSessions map[goquickfix.SessionID]struct{}
}

var (
	sharedReg   = map[string]*sharedEngine{}
	sharedRegMu sync.Mutex
)

// acquireSharedEngine returns a shared engine for the given name, creating it
// if necessary. When the name refers to an existing engine the connection_type
// and settings (if supplied) must match the originally registered values.
// For shared connections the first registrant's tlsConf is used; subsequent
// components may pass nil to inherit it.
//
// The reference count is incremented on every successful call; callers must
// invoke releaseSharedEngine when they are finished using the engine.
func acquireSharedEngine(name, connType, settings string, tlsConf *tls.Config, log *service.Logger) (*sharedEngine, error) {
	sharedRegMu.Lock()
	defer sharedRegMu.Unlock()

	eng, ok := sharedReg[name]
	if !ok {
		if connType == "" || settings == "" {
			return nil, fmt.Errorf("no existing quickfix connection named %q; the first input or output to reference a shared connection must provide connection_type and settings", name)
		}
		eng = &sharedEngine{
			name:             name,
			connType:         connType,
			settings:         settings,
			tlsConf:          tlsConf,
			log:              log,
			loggedOnSessions: make(map[goquickfix.SessionID]struct{}),
		}
		sharedReg[name] = eng
	} else {
		if connType != "" && connType != eng.connType {
			return nil, fmt.Errorf("connection_type %q for shared quickfix connection %q conflicts with previously registered value %q", connType, name, eng.connType)
		}
		if settings != "" && settings != eng.settings {
			return nil, fmt.Errorf("settings for shared quickfix connection %q conflict with the previously registered settings", name)
		}
	}
	eng.refs++
	return eng, nil
}

// releaseSharedEngine decrements the reference count and stops the underlying
// engine once the last user has released it.
func releaseSharedEngine(name string) {
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

func (e *sharedEngine) subscribe(s fixSubscriber) {
	e.subsMu.Lock()
	for _, existing := range e.subs {
		if existing == s {
			e.subsMu.Unlock()
			return
		}
	}
	e.subs = append(e.subs, s)
	e.subsMu.Unlock()

	// Replay currently-logged-on sessions to the new subscriber so that
	// components which subscribe after the FIX session is established (e.g.
	// lazily-initialized output resources) see the correct logon state and do
	// not incorrectly report ErrNotConnected.
	e.loggedOnMu.RLock()
	sessions := make([]goquickfix.SessionID, 0, len(e.loggedOnSessions))
	for sid := range e.loggedOnSessions {
		sessions = append(sessions, sid)
	}
	e.loggedOnMu.RUnlock()
	for _, sid := range sessions {
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
// Start() fails after NewAcceptor/NewInitiator has already registered sessions
// globally in goquickfix, the partially-constructed engine is stopped so the
// caller can safely retry.
func (e *sharedEngine) start() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.started {
		return nil
	}

	qfSettings, err := goquickfix.ParseSettings(strings.NewReader(e.settings))
	if err != nil {
		return err
	}

	storeFactory := selectStoreFactory(qfSettings)
	logFactory := newBentoLogFactory(e.log)

	switch e.connType {
	case "acceptor":
		a, err := goquickfix.NewAcceptor(e, storeFactory, qfSettings, logFactory)
		if err != nil {
			return err
		}
		if e.tlsConf != nil {
			a.SetTLSConfig(e.tlsConf)
		}
		if err := a.Start(); err != nil {
			// NewAcceptor has already registered the sessions in goquickfix's
			// global session registry; Stop() unregisters them so subsequent
			// retries don't fail with "Duplicate SessionID".
			a.Stop()
			return err
		}
		e.acceptor = a
	case "initiator":
		i, err := goquickfix.NewInitiator(e, storeFactory, qfSettings, logFactory)
		if err != nil {
			return err
		}
		if e.tlsConf != nil {
			i.SetTLSConfig(e.tlsConf)
		}
		if err := i.Start(); err != nil {
			i.Stop()
			return err
		}
		e.initiator = i
	default:
		return fmt.Errorf("unknown connection_type %q", e.connType)
	}
	e.started = true
	return nil
}

func (e *sharedEngine) stop() {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.acceptor != nil {
		e.acceptor.Stop()
		e.acceptor = nil
	}
	if e.initiator != nil {
		e.initiator.Stop()
		e.initiator = nil
	}
	e.started = false
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

func (e *sharedEngine) ToAdmin(message *goquickfix.Message, sessionID goquickfix.SessionID) {}

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

// selectStoreFactory returns the QuickFIX MessageStoreFactory implied by the
// parsed FIX settings. If the NATS Object Store URL setting is present in any
// session (or in the global defaults), the NATS-backed factory is used so FIX
// session state survives process restarts. Otherwise the in-memory factory is
// used.
func selectStoreFactory(qfSettings *goquickfix.Settings) goquickfix.MessageStoreFactory {
	if hasSetting(qfSettings, config.NatsStoreURL) {
		return natsstore.NewStoreFactory(qfSettings)
	}
	return goquickfix.NewMemoryStoreFactory()
}

func hasSetting(qfSettings *goquickfix.Settings, key string) bool {
	if v, err := qfSettings.GlobalSettings().Setting(key); err == nil && v != "" {
		return true
	}
	for _, s := range qfSettings.SessionSettings() {
		if v, err := s.Setting(key); err == nil && v != "" {
			return true
		}
	}
	return false
}
