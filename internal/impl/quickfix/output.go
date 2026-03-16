package quickfix

import (
	"bytes"
	"context"
	"strings"
	"sync"

	goquickfix "github.com/quickfixgo/quickfix"

	"github.com/warpstreamlabs/bento/public/service"
)

func quickfixOutputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Summary("Sends FIX messages using the QuickFIX/Go engine. Operates as an acceptor (server) or initiator (client). The message payload must be a raw FIX string; SOH (`\\x01`) and pipe (`|`) delimiters are both accepted.").
		Categories("Network").
		Fields(
			service.NewStringEnumField(fieldConnectionType, "acceptor", "initiator").
				Description("Whether to listen for incoming connections (`acceptor`) or connect to a remote host (`initiator`)."),
			service.NewStringField(fieldSettings).
				Description("QuickFIX/Go session settings in the standard cfg format.").
				Example(`[DEFAULT]
ConnectionType=initiator
HeartBtInt=30
SenderCompID=CLIENT
TargetCompID=SERVER
BeginString=FIX.4.4

[SESSION]
SocketConnectHost=localhost
SocketConnectPort=5001`).
				Example(`[DEFAULT]
ConnectionType=acceptor
HeartBtInt=30
SenderCompID=SERVER
TargetCompID=CLIENT
BeginString=FIX.4.4

[SESSION]
SocketAcceptPort=5001`),
		)
}

func init() {
	err := service.RegisterOutput("quickfix", quickfixOutputSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Output, int, error) {
			w, err := newQuickfixOutputFromParsed(conf, mgr)
			return w, 1, err
		})
	if err != nil {
		panic(err)
	}
}

type quickfixOutput struct {
	log      *service.Logger
	connType string
	settings string

	sessionsMut      sync.RWMutex
	loggedOnSessions map[goquickfix.SessionID]struct{}

	engineMut sync.Mutex
	acceptor  *goquickfix.Acceptor
	initiator *goquickfix.Initiator
}

func newQuickfixOutputFromParsed(pConf *service.ParsedConfig, mgr *service.Resources) (*quickfixOutput, error) {
	w := &quickfixOutput{
		log:              mgr.Logger(),
		loggedOnSessions: make(map[goquickfix.SessionID]struct{}),
	}

	var err error
	if w.connType, err = pConf.FieldString(fieldConnectionType); err != nil {
		return nil, err
	}
	if w.settings, err = pConf.FieldString(fieldSettings); err != nil {
		return nil, err
	}
	return w, nil
}

//------------------------------------------------------------------------------
// quickfix.Application interface

func (w *quickfixOutput) OnCreate(sessionID goquickfix.SessionID) {}

func (w *quickfixOutput) OnLogon(sessionID goquickfix.SessionID) {
	w.log.Infof("FIX session logged on: %s", sessionID)
	w.sessionsMut.Lock()
	w.loggedOnSessions[sessionID] = struct{}{}
	w.sessionsMut.Unlock()
}

func (w *quickfixOutput) OnLogout(sessionID goquickfix.SessionID) {
	w.log.Infof("FIX session logged out: %s", sessionID)
	w.sessionsMut.Lock()
	delete(w.loggedOnSessions, sessionID)
	w.sessionsMut.Unlock()
}

func (w *quickfixOutput) ToAdmin(message *goquickfix.Message, sessionID goquickfix.SessionID) {}

func (w *quickfixOutput) ToApp(message *goquickfix.Message, sessionID goquickfix.SessionID) error {
	return nil
}

func (w *quickfixOutput) FromAdmin(message *goquickfix.Message, sessionID goquickfix.SessionID) goquickfix.MessageRejectError {
	return nil
}

func (w *quickfixOutput) FromApp(message *goquickfix.Message, sessionID goquickfix.SessionID) goquickfix.MessageRejectError {
	return nil
}

//------------------------------------------------------------------------------
// service.Output interface

func (w *quickfixOutput) Connect(ctx context.Context) error {
	w.engineMut.Lock()
	defer w.engineMut.Unlock()

	if w.acceptor != nil || w.initiator != nil {
		return nil
	}

	qfSettings, err := goquickfix.ParseSettings(strings.NewReader(w.settings))
	if err != nil {
		return err
	}

	storeFactory := goquickfix.NewMemoryStoreFactory()
	logFactory := goquickfix.NewNullLogFactory()

	switch w.connType {
	case "acceptor":
		a, err := goquickfix.NewAcceptor(w, storeFactory, qfSettings, logFactory)
		if err != nil {
			return err
		}
		if err := a.Start(); err != nil {
			return err
		}
		w.acceptor = a
	case "initiator":
		i, err := goquickfix.NewInitiator(w, storeFactory, qfSettings, logFactory)
		if err != nil {
			return err
		}
		if err := i.Start(); err != nil {
			return err
		}
		w.initiator = i
	}
	return nil
}

// Write sends a raw FIX string message via the active QuickFIX session.
// The message is routed by its BeginString, SenderCompID and TargetCompID
// header fields. The session assigns sequence numbers and recalculates
// BodyLength and CheckSum automatically.
func (w *quickfixOutput) Write(ctx context.Context, msg *service.Message) error {
	w.sessionsMut.RLock()
	hasSession := len(w.loggedOnSessions) > 0
	w.sessionsMut.RUnlock()

	if !hasSession {
		return service.ErrNotConnected
	}

	rawBytes, err := msg.AsBytes()
	if err != nil {
		return err
	}

	// Accept pipe-delimited (human-readable) format as well as SOH-delimited.
	if bytes.ContainsRune(rawBytes, '|') && !bytes.ContainsRune(rawBytes, '\x01') {
		rawBytes = bytes.ReplaceAll(rawBytes, []byte("|"), []byte("\x01"))
	}

	fixMsg := goquickfix.NewMessage()
	if err := goquickfix.ParseMessage(fixMsg, bytes.NewBuffer(rawBytes)); err != nil {
		return err
	}

	return goquickfix.Send(fixMsg)
}

func (w *quickfixOutput) Close(ctx context.Context) error {
	w.engineMut.Lock()
	defer w.engineMut.Unlock()

	if w.acceptor != nil {
		w.acceptor.Stop()
		w.acceptor = nil
	}
	if w.initiator != nil {
		w.initiator.Stop()
		w.initiator = nil
	}
	return nil
}
