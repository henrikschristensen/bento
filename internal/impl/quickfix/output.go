package quickfix

import (
	"bytes"
	"context"
	"strings"
	"sync"

	goquickfix "codeberg.org/hsctech/quickfix"
	"codeberg.org/hsctech/quickfix/datadictionary"

	"github.com/warpstreamlabs/bento/public/service"
)

func quickfixOutputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Summary("Sends FIX messages using the QuickFIX/Go engine. Operates as an acceptor (server) or initiator (client). The message payload must be a raw FIX string (SOH or pipe delimiters) or a JSON object when `message_format` is set to `json`.").
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
			service.NewStringEnumField(fieldMessageFormat, "raw", "json").
				Description("The format of the incoming message payload. `raw` expects a wire-format FIX string (SOH or pipe delimited). `json` expects a JSON object with `Header`, `Body`, and `Trailer` sections as produced by the quickfix input.").
				Default("raw"),
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
	log           *service.Logger
	connType      string
	settings      string
	messageFormat string
	dd            *datadictionary.DataDictionary

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
	if w.messageFormat, err = pConf.FieldString(fieldMessageFormat); err != nil {
		return nil, err
	}

	if w.messageFormat == "json" {
		w.dd = loadDataDictionary(w.settings, mgr.Logger())
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
	logFactory := newBentoLogFactory(w.log)

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

// Write sends a FIX message via the active QuickFIX session.
// In "raw" mode the payload must be a FIX wire string (SOH or pipe delimited).
// In "json" mode the payload must be a JSON object with Header/Body/Trailer
// sections as produced by the quickfix input in json mode.
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

	fixMsg := goquickfix.NewMessage()

	if w.messageFormat == "json" {
		if err := fixMsg.FromJSON(rawBytes, w.dd); err != nil {
			return err
		}
	} else {
		// Accept pipe-delimited (human-readable) format as well as SOH-delimited.
		if bytes.ContainsRune(rawBytes, '|') && !bytes.ContainsRune(rawBytes, '\x01') {
			rawBytes = bytes.ReplaceAll(rawBytes, []byte("|"), []byte("\x01"))
		}
		if err := goquickfix.ParseMessage(fixMsg, bytes.NewBuffer(rawBytes)); err != nil {
			return err
		}
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
