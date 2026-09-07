package quickfix

import (
	"context"
	"sync"

	goquickfix "github.com/quickfixgo/quickfix"

	"github.com/warpstreamlabs/bento/public/service"
)

const fieldBufferSize = "buffer_size"

func quickfixInputSpec() *service.ConfigSpec {
	fields := append(quickfixConnFields(),
		service.NewIntField(fieldBufferSize).
			Description("The size of the internal channel buffer for received messages.").
			Default(1000),
		service.NewStringEnumField(fieldMessageFormat, "raw", "json").
			Description("The format in which received FIX messages are emitted. `raw` emits the wire-format FIX string with SOH delimiters. `json` serialises each message to a JSON object with `Header`, `Body`, and `Trailer` sections with fieldname/value pairs.").
			Default("raw"),
		service.NewTLSToggledField("tls"),
		service.NewAutoRetryNacksToggleField(),
	)
	return service.NewConfigSpec().
		Summary("Receives FIX messages using the [QuickFIX/Go engine](https://quickfixengine.org/go). Operates as an acceptor (server) or initiator (client). Each application-level message is emitted as a raw FIX string with SOH (`\\x01`) delimiters, or as a JSON object when `message_format` is set to `json`.").
		Description(sharedEngineDescription).
		Categories("Network").
		Fields(fields...)
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

	msgChan   chan *service.Message
	closeOnce sync.Once
	closeChan chan struct{}

	att engineAttachment
}

func newQuickfixInputFromParsed(pConf *service.ParsedConfig, mgr *service.Resources) (*quickfixInput, error) {
	r := &quickfixInput{
		log:       mgr.Logger(),
		closeChan: make(chan struct{}),
	}

	var err error
	if r.cfg, err = parseConnConfig(pConf, mgr.Logger()); err != nil {
		return nil, err
	}
	if r.bufferSize, err = pConf.FieldInt(fieldBufferSize); err != nil {
		return nil, err
	}
	if r.messageFormat, err = pConf.FieldString(fieldMessageFormat); err != nil {
		return nil, err
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
	payload, err := fixToBentoPayload(message, r.messageFormat, r.cfg.dd)
	if err != nil {
		r.log.Errorf("Failed to convert FIX message from session %s: %v", sessionID, err)
		return nil
	}
	select {
	case r.msgChan <- service.NewMessage(payload):
	default:
		r.log.Warnf("Message buffer full, dropping FIX message from session %s", sessionID)
	}
	return nil
}

//------------------------------------------------------------------------------
// service.Input interface

func (r *quickfixInput) Connect(ctx context.Context) error {
	return r.att.connect(r.cfg, r.log, r)
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
		r.att.disconnect()
		close(r.closeChan)
	})
	return nil
}
