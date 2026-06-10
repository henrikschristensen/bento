package quickfix

import (
	"context"
	"sync"

	goquickfix "codeberg.org/hsctech/quickfix"
	"codeberg.org/hsctech/quickfix/datadictionary"

	"github.com/warpstreamlabs/bento/public/service"
)

const fieldBufferSize = "buffer_size"

func quickfixInputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Summary("Receives FIX messages using the QuickFIX/Go engine. Operates as an acceptor (server) or initiator (client). Each application-level message is emitted as a raw FIX string with SOH (`\\x01`) delimiters, or as a JSON object when `message_format` is set to `json`.").
		Description(`When the `+"`name`"+` field is set, the underlying QuickFIX engine is shared with any other ` + "`quickfix`" + ` input or output that uses the same name. This allows a single FIX connection (initiator or acceptor) to be initiated by any input or output and reused by any combination of inputs and outputs. The first component to reference a shared name must supply ` + "`connection_type`" + ` and ` + "`settings`" + `; subsequent components referencing the same name may omit them, but if supplied they must match the originally registered values.`).
		Categories("Network").
		Fields(
			service.NewStringField(fieldName).
				Description("Optional name used to share a single QuickFIX engine across multiple `quickfix` inputs and outputs. Components referencing the same name reuse the same underlying acceptor or initiator connection, and must all be configured with identical `connection_type` and `settings`.").
				Default(""),
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
			service.NewStringEnumField(fieldMessageFormat, "raw", "json").
				Description("The format in which received FIX messages are emitted. `raw` emits the wire-format FIX string with SOH delimiters. `json` serialises each message to a JSON object with `Header`, `Body`, and `Trailer` sections with fieldname/value pairs.").
				Default("raw"),
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
	log           *service.Logger
	cfg           connConfig
	bufferSize    int
	messageFormat string
	dd            *datadictionary.DataDictionary

	msgChan   chan *service.Message
	closeOnce sync.Once
	closeChan chan struct{}

	engineMut sync.Mutex
	engine    *sharedEngine
}

func newQuickfixInputFromParsed(pConf *service.ParsedConfig, mgr *service.Resources) (*quickfixInput, error) {
	r := &quickfixInput{
		log:       mgr.Logger(),
		closeChan: make(chan struct{}),
	}

	var err error
	if r.cfg, err = parseConnConfig(pConf); err != nil {
		return nil, err
	}
	if r.bufferSize, err = pConf.FieldInt(fieldBufferSize); err != nil {
		return nil, err
	}
	if r.messageFormat, err = pConf.FieldString(fieldMessageFormat); err != nil {
		return nil, err
	}

	if r.messageFormat == "json" {
		r.dd = loadDataDictionary(r.cfg.settings, mgr.Logger())
	}

	r.msgChan = make(chan *service.Message, r.bufferSize)
	return r, nil
}

//------------------------------------------------------------------------------
// fixSubscriber

func (r *quickfixInput) onLogon(sessionID goquickfix.SessionID) {
	r.log.Infof("FIX session logged on: %s", sessionID)
}

func (r *quickfixInput) onLogout(sessionID goquickfix.SessionID) {
	r.log.Infof("FIX session logged out: %s", sessionID)
}

// fromApp is called by the shared engine for every application-level message
// received on any session.
func (r *quickfixInput) fromApp(message *goquickfix.Message, sessionID goquickfix.SessionID) goquickfix.MessageRejectError {
	var bentoMsg *service.Message
	if r.messageFormat == "json" {
		jsonBytes, err := message.ToJSON(r.dd)
		if err != nil {
			r.log.Errorf("Failed to serialise FIX message to JSON from session %s: %v", sessionID, err)
			return nil
		}
		bentoMsg = service.NewMessage(jsonBytes)
	} else {
		bentoMsg = service.NewMessage(message.Bytes())
	}
	select {
	case r.msgChan <- bentoMsg:
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

	if r.engine != nil {
		return nil
	}

	eng, err := connectEngine(r.cfg, r.log, r)
	if err != nil {
		return err
	}
	r.engine = eng
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

		disconnectEngine(r.cfg.name, r.engine, r)
		r.engine = nil
		close(r.closeChan)
	})
	return nil
}
