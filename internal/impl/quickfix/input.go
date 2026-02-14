package quickfix

import (
	"context"
	"sync"

	"github.com/quickfixgo/quickfix"
	"github.com/quickfixgo/quickfix/datadictionary"
	"github.com/warpstreamlabs/bento/public/service"
)

const (
	quickfixInputApplicationType = "application_type"
	quickfixInputConfigFile      = "config_file"
	quickfixInputMsgStoreType    = "message_store_type"
	quickfixInputLoggerType      = "logger_type"
)

type quickfixInput struct {
	log                   *service.Logger
	nm                    *service.Resources
	appType               fixAppType
	storeType             messageStoreType
	logType               logFactoryType
	config                string
	settings              *quickfix.Settings
	appDictionaries       map[quickfix.SessionID]*datadictionary.DataDictionary
	transportDictionaries map[quickfix.SessionID]*datadictionary.DataDictionary
	eventChan             chan quickfixEvent
	mut                   sync.Mutex
}

type quickfixEventType string

const (
	createEvent    quickfixEventType = "create"
	logonEvent     quickfixEventType = "logon"
	logoutEvent    quickfixEventType = "logout"
	toAdminEvent   quickfixEventType = "to_admin"
	toAppEvent     quickfixEventType = "to_app"
	fromAdminEvent quickfixEventType = "from_admin"
	fromAppEvent   quickfixEventType = "from_app"
)

type quickfixEvent struct {
	sessionId quickfix.SessionID
	eventType quickfixEventType
	message   *quickfix.Message
}

func NewQuickfixEvent(id quickfix.SessionID, t quickfixEventType, message *quickfix.Message) quickfixEvent {
	var msg *quickfix.Message = nil
	if message != nil {
		msg = quickfix.NewMessage()
		message.CopyInto(msg)
	}
	return quickfixEvent{
		eventType: t,
		message:   msg,
	}
}

func quickfixInputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Summary("Quickfix compatiple input. Operates as initiator or acceptor, depending on config.").
		Description("")
}

func init() {
	err := service.RegisterInput("quickfix", quickfixInputConfig(),
		func(pConf *service.ParsedConfig, res *service.Resources) (service.Input, error) {
			r, err := quickfixInputFromParsed(pConf, res)
			if err != nil {
				return nil, err
			}
			return r, nil
		})
	if err != nil {
		panic(err)
	}
}

func quickfixInputFromParsed(conf *service.ParsedConfig, nm *service.Resources) (*quickfixInput, error) {
	appType, err := conf.FieldString(quickfixInputApplicationType)
	if err != nil {
		return nil, err
	}
	configFile, err := conf.FieldString(quickfixInputConfigFile)
	if err != nil {
		return nil, err
	}

	storeType, err := conf.FieldString(quickfixInputMsgStoreType)
	if err != nil {
		return nil, err
	}

	logType, err := conf.FieldString(quickfixInputLoggerType)
	if err != nil {
		return nil, err
	}

	settings, err := GetSettings(configFile)
	if err != nil {
		return nil, err
	}

	return &quickfixInput{
		nm:        nm,
		log:       nm.Logger(),
		appType:   fixAppType(appType),
		config:    configFile,
		storeType: messageStoreType(storeType),
		logType:   logFactoryType(logType),
		settings:  settings,
	}, nil
}

func (q *quickfixInput) Connect(ctx context.Context) error {
	q.mut.Lock()
	defer q.mut.Unlock()

	fixapp, err := GetOrCreateFixApp(q.appType, q.storeType, q.logType, q, q.config)
	if err != nil {
		return err
	}

	eventChan := make(chan quickfixEvent)
	q.eventChan = eventChan

	fixapp.Start()

	return err
}

func (q *quickfixInput) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	q.mut.Lock()
	eventChan := q.eventChan
	q.mut.Unlock()

	if eventChan == nil {
		return nil, nil, service.ErrNotConnected
	}

	select {
	case msg, open := <-eventChan:
		if !open {
			q.mut.Lock()
			q.eventChan = nil
			q.mut.Unlock()
			return nil, nil, service.ErrEndOfInput
		}

		message := service.NewMessage(msg.message.ToJSON(true, q.appDictionaries[msg.sessionId])
		message.MetaSetMut("quickfix_session_id", msg.sessionId.String())
		message.MetaSetMut("quickfix_event", msg.eventType)

		return message, func(ctx context.Context, res error) error {
			return nil
		}, nil
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	}
}

func (q *quickfixInput) Close(ctx context.Context) error {
	q.mut.Lock()
	defer q.mut.Unlock()

	close(q.eventChan)

	fixapp, err := GetOrCreateFixApp(q.appType, q.storeType, q.logType, q, q.config)
	if err != nil {
		return err
	}

	fixapp.Stop()

	return nil
}

func (q *quickfixInput) OnCreate(sessionID quickfix.SessionID) {
	session := q.settings.SessionSettings()[sessionID]
	if session.HasSetting("DataDictionary") {
		file, err := session.Setting("DataDictionary")
		if err == nil {
			dict, err := datadictionary.Parse(file)
			if err == nil {
				q.appDictionaries[sessionID] = dict
				q.transportDictionaries[sessionID] = dict
			}
		}
	}
	if session.HasSetting("TransportDataDictionary") {
		file, err := session.Setting("TransportDataDictionary")
		if err == nil {
			dict, err := datadictionary.Parse(file)
			if err == nil {
				q.transportDictionaries[sessionID] = dict
			}
		}
	}

	event := NewQuickfixEvent(sessionID, createEvent, nil)

	q.eventChan <- event
}

func (q *quickfixInput) OnLogon(sessionID quickfix.SessionID) {
	event := NewQuickfixEvent(sessionID, logonEvent, nil)

	q.eventChan <- event
}

func (q *quickfixInput) OnLogout(sessionID quickfix.SessionID) {
	event := NewQuickfixEvent(sessionID, logoutEvent, nil)

	q.eventChan <- event
}

func (q *quickfixInput) ToAdmin(message *quickfix.Message, sessionID quickfix.SessionID) {
	event := NewQuickfixEvent(sessionID, toAdminEvent, message)

	q.eventChan <- event
}

func (q *quickfixInput) ToApp(message *quickfix.Message, sessionID quickfix.SessionID) error {
	event := NewQuickfixEvent(sessionID, toAppEvent, message)

	q.eventChan <- event

	return nil
}

func (q *quickfixInput) FromAdmin(message *quickfix.Message, sessionID quickfix.SessionID) quickfix.MessageRejectError {
	event := NewQuickfixEvent(sessionID, fromAdminEvent, message)

	q.eventChan <- event

	return nil
}

func (q *quickfixInput) FromApp(message *quickfix.Message, sessionID quickfix.SessionID) quickfix.MessageRejectError {
	event := NewQuickfixEvent(sessionID, fromAppEvent, message)

	q.eventChan <- event

	return nil
}
