//go:build !wasm

package io

import (
	"context"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"

	"github.com/warpstreamlabs/bento/internal/filepath"
	"github.com/warpstreamlabs/bento/public/service"
)

const (
	fsEventInputFieldPaths = "paths"
)

func fsEventInputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("Local").
		Summary(`Detects filesystem events. Emits empty messages with metadata describing the event`).
		Description(`
### Metadata

This input adds the following metadata fields to each message:

` + "```text" + `
- path
- operation
- mod_time_unix
- mod_time (RFC3339)
` + "```" + `

You can access these metadata fields using
[function interpolation](/docs/configuration/interpolation#bloblang-queries).`).
		Fields(
			service.NewStringListField(fsEventInputFieldPaths).
				Description("A list of paths to monitor for file changes. Glob patterns are supported, including super globs (double star)."),
		)
}

func init() {
	err := service.RegisterInput("fsevent", fsEventInputSpec(),
		func(pConf *service.ParsedConfig, res *service.Resources) (service.Input, error) {
			r, err := fsEventWatcherFromParsed(pConf, res)
			if err != nil {
				return nil, err
			}
			return r, nil
		})
	if err != nil {
		panic(err)
	}
}

type fsEventWatcher struct {
	log       *service.Logger
	nm        *service.Resources
	watcher   *fsnotify.Watcher
	eventChan chan watcherEventMsg
	cMut      sync.Mutex
	paths     []string
}

type watcherEventMsg struct {
	event         fsnotify.Event
	timestampUnix int64
}

func fsEventWatcherFromParsed(conf *service.ParsedConfig, nm *service.Resources) (*fsEventWatcher, error) {
	paths, err := conf.FieldStringList(fsEventInputFieldPaths)
	if err != nil {
		return nil, err
	}

	expandedPaths, err := filepath.Globs(nm.FS(), paths)
	if err != nil {
		return nil, err
	}

	return &fsEventWatcher{
		nm:    nm,
		log:   nm.Logger(),
		paths: expandedPaths,
	}, nil
}

func (f *fsEventWatcher) Connect(ctx context.Context) error {
	f.cMut.Lock()
	defer f.cMut.Unlock()

	if f.watcher != nil {
		return nil
	}

	eventChan := make(chan watcherEventMsg)

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		f.log.Errorf("Failed to instantiate new filesystem watcher: %s", err)
		return err
	}

	for _, path := range f.paths {
		err = watcher.Add(path)
		if err != nil {
			f.log.Errorf("Failed to add path %v: %s", path, err)
			return err
		}
	}

	go func() {
		for {
			select {
			case event, ok := <-watcher.Events:
				if !ok {
					return
				}
				msg := watcherEventMsg{
					event:         event,
					timestampUnix: time.Now().Unix(),
				}
				f.eventChan <- msg
			case err, ok := <-watcher.Errors:
				if !ok {
					return
				}
				f.log.Errorf("error:", err)
			}
		}
	}()

	f.watcher = watcher
	f.eventChan = eventChan
	return nil
}

func (f *fsEventWatcher) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	f.cMut.Lock()
	eventChan := f.eventChan
	f.cMut.Unlock()

	select {
	case msg, open := <-eventChan:
		if !open {
			f.cMut.Lock()
			f.eventChan = nil
			f.watcher = nil
			f.cMut.Unlock()
			return nil, nil, service.ErrNotConnected
		}

		message := service.NewMessage(nil)
		message.MetaSetMut("fsevent_operation", msg.event.Op.String())
		message.MetaSetMut("fsevent_path", msg.event.Name)
		message.MetaSetMut("fsevent_mod_time_unix", msg.timestampUnix)
		timestamp := time.Unix(msg.timestampUnix, 0).Format(time.RFC3339)
		message.MetaSetMut("fsevent_mod_time", timestamp)

		return message, func(ctx context.Context, res error) error {
			return nil
		}, nil
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	}
}

func (f *fsEventWatcher) Close(ctx context.Context) error {
	f.cMut.Lock()
	defer f.cMut.Unlock()

	err := f.watcher.Close()

	return err
}
