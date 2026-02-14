package quickfix

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/quickfixgo/quickfix"
	"github.com/quickfixgo/quickfix/datadictionary"
	filelog "github.com/quickfixgo/quickfix/log/file"
	"github.com/quickfixgo/quickfix/log/screen"
	filestore "github.com/quickfixgo/quickfix/store/file"
	"github.com/warpstreamlabs/bento/public/service"
)

const (
	quickfixInputApplicationType = "application_type"
	quickfixInputConfigFile      = "config_file"
	quickfixInputMsgStoreType    = "message_store_type"
	quickfixInputLoggerType      = "logger_type"
)

type fixAppType string

const (
	acceptorType  fixAppType = "acceptor"
	initiatorType fixAppType = "initiator"
)

type logFactoryType string

const (
	logFactoryFile   logFactoryType = "file"
	logFactoryScreen logFactoryType = "screen"
)

type messageStoreType string

const (
	messageStoreMem  messageStoreType = "memory"
	messageStoreFile messageStoreType = "file"
)

type quickfixInput struct {
	log                   *service.Logger
	nm                    *service.Resources
	appType               fixAppType
	storeType             messageStoreType
	logType               logFactoryType
	settings              *quickfix.Settings
	acceptor              *quickfix.Acceptor
	initiator             *quickfix.Initiator
	appDictionaries       map[quickfix.SessionID]*datadictionary.DataDictionary
	transportDictionaries map[quickfix.SessionID]*datadictionary.DataDictionary
	eventChan             chan quickfixEvent
	cMut                  sync.Mutex
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

	configPath, err := filepath.Abs(configFile)
	if err != nil {
		return nil, err
	}
	c, err := os.Open(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open config file: %v", err)
	}
	defer c.Close()

	// Create settings from config file
	settings, err := quickfix.ParseSettings(c)
	if err != nil {
		return nil, fmt.Errorf("failed to parse settings: %v", err)
	}

	return &quickfixInput{
		nm:        nm,
		log:       nm.Logger(),
		appType:   fixAppType(appType),
		settings:  settings,
		storeType: messageStoreType(storeType),
		logType:   logFactoryType(logType),
		acceptor:  nil,
		initiator: nil,
	}, nil
}

func createMessageStoreFactory(t messageStoreType, s *quickfix.Settings) quickfix.MessageStoreFactory {
	switch t {
	case messageStoreMem:
		return quickfix.NewMemoryStoreFactory()
	default:
		return filestore.NewStoreFactory(s)
	}
}

func createLogFactory(t logFactoryType, s *quickfix.Settings) (quickfix.LogFactory, error) {
	switch t {
	case logFactoryFile:
		return filelog.NewLogFactory(s)
	default:
		f := screen.NewLogFactory()
		return f, nil
	}
}

func (q *quickfixInput) Connect(ctx context.Context) error {
	q.cMut.Lock()
	defer q.cMut.Unlock()

	eventChan := make(chan quickfixEvent)
	q.eventChan = eventChan

	logFactory, err := createLogFactory(q.logType, q.settings)
	if err != nil {
		return err
	}

	switch q.appType {
	case acceptorType:
		q.acceptor, err = quickfix.NewAcceptor(q, createMessageStoreFactory(q.storeType, q.settings), q.settings, logFactory)
		if err != nil {
			return err
		}
		err = q.acceptor.Start()
	case initiatorType:
		q.initiator, err = quickfix.NewInitiator(q, createMessageStoreFactory(q.storeType, q.settings), q.settings, logFactory)
		if err != nil {
			return err
		}
		err = q.initiator.Start()
	}

	return err
}

func (q *quickfixInput) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	q.cMut.Lock()
	eventChan := q.eventChan
	q.cMut.Unlock()

	if eventChan == nil {
		return nil, nil, service.ErrNotConnected
	}

	select {
	case msg, open := <-eventChan:
		if !open {
			q.cMut.Lock()
			q.eventChan = nil
			q.acceptor = nil
			q.initiator = nil
			q.cMut.Unlock()
			return nil, nil, service.ErrEndOfInput
		}

		message := service.NewMessage(nil)
		message.MetaSetMut("quickfix_session_id", msg.sessionId.String())
		message.MetaSetMut("quickfix_event", msg.eventType)
		message.SetBytes([]byte(msg.message.String()))

		return message, func(ctx context.Context, res error) error {
			return nil
		}, nil
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	}
}

func (q *quickfixInput) Close(ctx context.Context) error {
	q.cMut.Lock()
	defer q.cMut.Unlock()

	if q.acceptor != nil {
		q.acceptor.Stop()
	}

	if q.initiator != nil {
		q.initiator.Stop()
	}

	return nil
}

func (q *quickfixInput) OnCreate(sessionID quickfix.SessionID) {
	q.cMut.Lock()
	defer q.cMut.Unlock()

	session := q.settings.SessionSettings()[sessionID]
	if session.HasSetting("DataDictionary") {
		file, err := session.Setting("DataDictionary")
		if err == nil {
			dict, err := datadictionary.Parse(file)
			if err == nil {
				q.appDictionaries[sessionID] = dict
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
	q.cMut.Lock()
	defer q.cMut.Unlock()

	event := NewQuickfixEvent(sessionID, logonEvent, nil)

	q.eventChan <- event
}

func (q *quickfixInput) OnLogout(sessionID quickfix.SessionID) {
	q.cMut.Lock()
	defer q.cMut.Unlock()

	event := NewQuickfixEvent(sessionID, logoutEvent, nil)

	q.eventChan <- event
}

func (q *quickfixInput) ToAdmin(message *quickfix.Message, sessionID quickfix.SessionID) {
	q.cMut.Lock()
	defer q.cMut.Unlock()

	event := NewQuickfixEvent(sessionID, logoutEvent, message)

	q.eventChan <- event
}

func (q *quickfixInput) ToApp(message *quickfix.Message, sessionID quickfix.SessionID) error {
	q.cMut.Lock()
	defer q.cMut.Unlock()

	event := NewQuickfixEvent(sessionID, logoutEvent, message)

	q.eventChan <- event

	return nil
}

func (q *quickfixInput) FromAdmin(message *quickfix.Message, sessionID quickfix.SessionID) quickfix.MessageRejectError {
	q.cMut.Lock()
	defer q.cMut.Unlock()

	event := NewQuickfixEvent(sessionID, logoutEvent, message)

	q.eventChan <- event

	return nil
}

func (q *quickfixInput) FromApp(message *quickfix.Message, sessionID quickfix.SessionID) quickfix.MessageRejectError {
	q.cMut.Lock()
	defer q.cMut.Unlock()

	event := NewQuickfixEvent(sessionID, logoutEvent, message)

	q.eventChan <- event

	return nil
}
