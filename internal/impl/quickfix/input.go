package quickfix

import (
	"bytes"
	"context"
	"strings"
	"sync"

	goquickfix "github.com/quickfixgo/quickfix"

	"github.com/warpstreamlabs/bento/public/service"
)

const (
	fieldConnectionType = "connection_type"
	fieldSettings       = "settings"
	fieldBufferSize     = "buffer_size"
)

func quickfixInputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Summary("Receives FIX messages using the QuickFIX/Go engine. Operates as an acceptor (server) or initiator (client). Each application-level message is emitted as a raw FIX string with SOH (`\\x01`) delimiters.").
		Categories("Network").
		Fields(
			service.NewStringEnumField(fieldConnectionType, "acceptor", "initiator").
				Description("Whether to listen for incoming connections (`acceptor`) or connect to a remote host (`initiator`)."),
			service.NewStringField(fieldSettings).
				Description("QuickFIX/Go session settings in the standard cfg format.").
				Example(`[DEFAULT]
ConnectionType=acceptor
HeartBtInt=30
SenderCompID=SERVER
TargetCompID=CLIENT
BeginString=FIX.4.4

[SESSION]
SocketAcceptPort=5001`).
				Example(`[DEFAULT]
ConnectionType=initiator
HeartBtInt=30
SenderCompID=CLIENT
TargetCompID=SERVER
BeginString=FIX.4.4

[SESSION]
SocketConnectHost=localhost
SocketConnectPort=5001`),
			service.NewIntField(fieldBufferSize).
				Description("The size of the internal channel buffer for received messages.").
				Default(1000),
			service.NewAutoRetryNacksToggleField(),
		)
}

func init() {
	err := service.RegisterInput("quickfix", quickfixInputSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
			r, err := newQuickfixInputFromParsed(conf, mgr)
			if err != nil {
				return nil, err
			}
			return service.AutoRetryNacksToggled(conf, r)
		})
	if err != nil {
		panic(err)
	}
}

type quickfixInput struct {
	log        *service.Logger
	connType   string
	settings   string
	bufferSize int

	msgChan   chan *service.Message
	closeOnce sync.Once
	closeChan chan struct{}

	engineMut sync.Mutex
	acceptor  *goquickfix.Acceptor
	initiator *goquickfix.Initiator
}

func newQuickfixInputFromParsed(pConf *service.ParsedConfig, mgr *service.Resources) (*quickfixInput, error) {
	r := &quickfixInput{
		log:       mgr.Logger(),
		closeChan: make(chan struct{}),
	}

	var err error
	if r.connType, err = pConf.FieldString(fieldConnectionType); err != nil {
		return nil, err
	}
	if r.settings, err = pConf.FieldString(fieldSettings); err != nil {
		return nil, err
	}
	if r.bufferSize, err = pConf.FieldInt(fieldBufferSize); err != nil {
		return nil, err
	}
	r.msgChan = make(chan *service.Message, r.bufferSize)
	return r, nil
}

//------------------------------------------------------------------------------
// quickfix.Application interface

func (r *quickfixInput) OnCreate(sessionID goquickfix.SessionID) {}

func (r *quickfixInput) OnLogon(sessionID goquickfix.SessionID) {
	r.log.Infof("FIX session logged on: %s", sessionID)
}

func (r *quickfixInput) OnLogout(sessionID goquickfix.SessionID) {
	r.log.Infof("FIX session logged out: %s", sessionID)
}

func (r *quickfixInput) ToAdmin(message *goquickfix.Message, sessionID goquickfix.SessionID) {}

func (r *quickfixInput) ToApp(message *goquickfix.Message, sessionID goquickfix.SessionID) error {
	return nil
}

func (r *quickfixInput) FromAdmin(message *goquickfix.Message, sessionID goquickfix.SessionID) goquickfix.MessageRejectError {
	return nil
}

// FromApp is called by QuickFIX for every application-level message received.
func (r *quickfixInput) FromApp(message *goquickfix.Message, sessionID goquickfix.SessionID) goquickfix.MessageRejectError {
	msg := service.NewMessage(bytes.TrimRight(message.Bytes(), "\x01"))
	select {
	case r.msgChan <- msg:
	default:
		r.log.Warnf("Message buffer full, dropping FIX message from session %s", sessionID)
	}
	return nil
}

//------------------------------------------------------------------------------
// service.Input interface

func (r *quickfixInput) Connect(ctx context.Context) error {
	r.engineMut.Lock()
	defer r.engineMut.Unlock()

	if r.acceptor != nil || r.initiator != nil {
		return nil
	}

	qfSettings, err := goquickfix.ParseSettings(strings.NewReader(r.settings))
	if err != nil {
		return err
	}

	storeFactory := goquickfix.NewMemoryStoreFactory()
	logFactory := goquickfix.NewNullLogFactory()

	switch r.connType {
	case "acceptor":
		a, err := goquickfix.NewAcceptor(r, storeFactory, qfSettings, logFactory)
		if err != nil {
			return err
		}
		if err := a.Start(); err != nil {
			return err
		}
		r.acceptor = a
	case "initiator":
		i, err := goquickfix.NewInitiator(r, storeFactory, qfSettings, logFactory)
		if err != nil {
			return err
		}
		if err := i.Start(); err != nil {
			return err
		}
		r.initiator = i
	}
	return nil
}

func (r *quickfixInput) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	select {
	case msg, ok := <-r.msgChan:
		if !ok {
			return nil, nil, service.ErrEndOfInput
		}
		return msg, func(ctx context.Context, err error) error { return nil }, nil
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	case <-r.closeChan:
		return nil, nil, service.ErrEndOfInput
	}
}

func (r *quickfixInput) Close(ctx context.Context) error {
	r.closeOnce.Do(func() {
		r.engineMut.Lock()
		defer r.engineMut.Unlock()

		if r.acceptor != nil {
			r.acceptor.Stop()
			r.acceptor = nil
		}
		if r.initiator != nil {
			r.initiator.Stop()
			r.initiator = nil
		}
		close(r.closeChan)
	})
	return nil
}
