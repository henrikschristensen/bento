package quickfix

import (
	"context"

	goquickfix "github.com/quickfixgo/quickfix"

	"github.com/warpstreamlabs/bento/public/service"
)

func quickfixOutputSpec() *service.ConfigSpec {
	fields := append(quickfixConnFields(),
		service.NewStringEnumField(fieldMessageFormat, "raw", "json").
			Description("The format of the incoming message payload. `raw` expects a wire-format FIX string (SOH or pipe delimited). `json` expects a JSON object with `Header`, `Body`, and `Trailer` sections as produced by the quickfix input.").
			Default("raw"),
		service.NewTLSToggledField("tls"),
	)
	return service.NewConfigSpec().
		Summary("Sends FIX messages using the [QuickFIX/Go engine](https://quickfixengine.org/go). Operates as an acceptor (server) or initiator (client). The message payload must be a raw FIX string (SOH or pipe delimiters) or a JSON object when `message_format` is set to `json`.").
		Description(sharedEngineDescription).
		Categories("Network").
		Fields(fields...)
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
	cfg           connConfig
	messageFormat string

	att engineAttachment
}

func newQuickfixOutputFromParsed(pConf *service.ParsedConfig, mgr *service.Resources) (*quickfixOutput, error) {
	w := &quickfixOutput{
		log: mgr.Logger(),
	}

	var err error
	if w.cfg, err = parseConnConfig(pConf, mgr.Logger()); err != nil {
		return nil, err
	}
	if w.messageFormat, err = pConf.FieldString(fieldMessageFormat); err != nil {
		return nil, err
	}

	return w, nil
}

//------------------------------------------------------------------------------
// fixSubscriber

func (w *quickfixOutput) onLogon(sessionID goquickfix.SessionID) {
	w.log.Infof("FIX session logged on: %s", sessionID)
}

func (w *quickfixOutput) onLogout(sessionID goquickfix.SessionID) {
	w.log.Infof("FIX session logged out: %s", sessionID)
}

// fromApp is a no-op for outputs; inbound messages are dispatched by the
// shared engine but outputs don't consume them.
func (w *quickfixOutput) fromApp(message *goquickfix.Message, sessionID goquickfix.SessionID) goquickfix.MessageRejectError {
	return nil
}

//------------------------------------------------------------------------------
// service.Output interface

func (w *quickfixOutput) Connect(ctx context.Context) error {
	return w.att.connect(w.cfg, w.log, w)
}

// Write sends a FIX message via the active QuickFIX session.
// In "raw" mode the payload must be a FIX wire string (SOH or pipe delimited).
// In "json" mode the payload must be a JSON object with Header/Body/Trailer
// sections as produced by the quickfix input in json mode.
func (w *quickfixOutput) Write(ctx context.Context, msg *service.Message) error {
	h := w.att.get()
	if h == nil || !h.hasLoggedOnSessions() {
		return service.ErrNotConnected
	}

	rawBytes, err := msg.AsBytes()
	if err != nil {
		return err
	}

	fixMsg := goquickfix.NewMessage()
	if err := bentoToFIXMessage(fixMsg, rawBytes, w.messageFormat, w.cfg.dd); err != nil {
		return err
	}

	return h.send(fixMsg)
}

func (w *quickfixOutput) Close(ctx context.Context) error {
	w.att.disconnect()
	return nil
}
