package quickfix

import (
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/quickfixgo/quickfix"
	filelog "github.com/quickfixgo/quickfix/log/file"
	"github.com/quickfixgo/quickfix/log/screen"
	filestore "github.com/quickfixgo/quickfix/store/file"
)

var (
	fixappstore     = make(map[string]*fixapp)
	fixappstorelock sync.Mutex
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

type fixappState string

const (
	fixappstatestarted fixappState = "started"
	fixappstatestopped fixappState = "stopped"
)

type fixapp struct {
	chksum    []byte
	acceptor  *quickfix.Acceptor
	initiator *quickfix.Initiator
	state     fixappState
	refcount  int
}

func GetSettings(configFile string) (*quickfix.Settings, error) {
	configPath, err := filepath.Abs(configFile)
	if err != nil {
		return nil, err
	}
	c, err := os.Open(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open config file: %v", err)
	}
	defer c.Close()
	s, err := quickfix.ParseSettings(c)
	if err != nil {
		return nil, fmt.Errorf("failed to parse settings: %v", err)
	}
	return s, nil
}

func GetOrCreateFixApp(appType fixAppType, msgStore messageStoreType, logType logFactoryType, app quickfix.Application, configFile string) (*fixapp, error) {
	chk, err := createChecksum(configFile)
	if err != nil {
		return nil, err
	}

	fixappstorelock.Lock()
	defer fixappstorelock.Unlock()

	a, ok := fixappstore[string(chk)]
	if ok {
		return a, nil
	}

	f := &fixapp{
		acceptor:  nil,
		initiator: nil,
		chksum:    chk,
		state:     fixappstatestopped,
	}

	s, err := GetSettings(configFile)
	if err != nil {
		return nil, err
	}

	store, err := createMessageStoreFactory(msgStore, s)
	if err != nil {
		return nil, err
	}

	log, err := createLogFactory(logType, s)
	if err != nil {
		return nil, err
	}

	switch appType {
	case acceptorType:
		f.acceptor, err = quickfix.NewAcceptor(app, store, s, log)
	case initiatorType:
		f.initiator, err = quickfix.NewInitiator(app, store, s, log)
	}
	if err != nil {
		return nil, err
	}

	fixappstore[string(chk)] = f

	return f, nil
}

func (f *fixapp) Start() error {
	fixappstorelock.Lock()
	defer fixappstorelock.Unlock()

	var err error
	if f.state != fixappstatestarted {
		if f.acceptor != nil {
			err = f.acceptor.Start()
		} else if f.initiator != nil {
			err = f.initiator.Start()
		} else {
			err = fmt.Errorf("cannot start uninitialized fixapp")
		}
		f.state = fixappstatestarted
	}
	f.refcount++

	return err
}

func (f *fixapp) Stop() {
	fixappstorelock.Lock()
	defer fixappstorelock.Unlock()

	if f.state == fixappstatestopped || f.refcount > 1 {
		f.refcount--
		return
	}

	if f.acceptor != nil {
		f.acceptor.Stop()
	} else if f.initiator != nil {
		f.initiator.Stop()
	}

	f.acceptor = nil
	f.initiator = nil
}

func createChecksum(configPath string) ([]byte, error) {
	c, err := os.Open(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open config file: %v", err)
	}
	defer c.Close()
	hasher := sha256.New()
	t, err := io.ReadAll(c)
	hasher.Write(t)

	return hasher.Sum(nil), nil
}

func createMessageStoreFactory(t messageStoreType, s *quickfix.Settings) (quickfix.MessageStoreFactory, error) {
	switch t {
	case messageStoreMem:
		return quickfix.NewMemoryStoreFactory(), nil
	case messageStoreFile:
		return filestore.NewStoreFactory(s), nil
	default:
		return nil, fmt.Errorf("cannot create message store of type %v", t)
	}
}

func createLogFactory(t logFactoryType, s *quickfix.Settings) (quickfix.LogFactory, error) {
	switch t {
	case logFactoryFile:
		return filelog.NewLogFactory(s)
	case logFactoryScreen:
		f := screen.NewLogFactory()
		return f, nil
	default:
		return nil, fmt.Errorf("cannot create logger of type %v", t)
	}
}
