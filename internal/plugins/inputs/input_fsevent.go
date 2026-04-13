package inputs

import (
	"context"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/warpstreamlabs/bento/public/service"
)

const (
	fsEventInputFieldPaths                     = "paths"
	fsEventInputFieldIsRecursive               = "is_recursive"
	fsEventInputFieldWatchNewSubdirs           = "watch_new_subdirs"
	fsEventInputFieldWriteDedupTimeout         = "write_dedup_timeout"
	fsEventInputFieldExtensions                = "extensions"
	fsEventInputFieldExistingFilesPollInterval = "existing_files_poll_interval"
	fsEventInputFieldExistingFilesMinAge       = "existing_files_min_age"
)

func fsEventInputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("Local").
		Summary(`Detects filesystem events. Emits empty messages with metadata describing the event`).
		Description(`
### Metadata

This input adds the following metadata fields to each message:

`+"```text"+`
- fsevent_path
- fsevent_operation
- fsevent_mod_time_unix
- fsevent_mod_time (RFC3339)
- fsevent_is_dir
`+"```"+`

You can access these metadata fields using
[function interpolation](/docs/configuration/interpolation#bloblang-queries).`+`

|Operation|Cause|
|---------|-----|
|CREATE   |A new pathname was created.|
|WRITE    |The pathname was written to; this does *not* mean the write has finished, and a write can be followed by more writes.|
|WRITTEN  |The pathname was written to; if write deduplication timeout was set > 0, this fires when last write duration expires.|
|REMOVE   |The path was removed; any watches on it will be removed. Some "remove" operations may trigger a RENAME if the file is actually moved (for example "remove to trash" is often a rename).|
|RENAME   |The path was renamed to something else. Any watches on it will be removed.|
|CHMOD    |File attributes were changed. It's generally not recommended to take action on this event, as it may get triggered very frequently by some software. For example, Spotlight indexing on macOS, anti-virus software, backup software, etc.|
`).
		Fields(
			service.NewStringListField(fsEventInputFieldPaths).
				Description("A list of paths to monitor for file changes."),
			service.NewBoolField(fsEventInputFieldIsRecursive).
				Description("If set, subdirs of configured paths will be watched too.").
				Default(false),
			service.NewBoolField(fsEventInputFieldWatchNewSubdirs).
				Description("If set, events from subdirs created after the input is started, will also be watched.").
				Default(false),
			service.NewDurationField(fsEventInputFieldWriteDedupTimeout).
				Description("If set, no events will be fired until last file write was 'timeout' ago, then the WRITTEN event fires.").
				Default("0s"),
			service.NewStringListField(fsEventInputFieldExtensions).
				Description("An optional list of file extensions to filter events for (e.g. `[\".txt\", \".log\"]`). If empty, events for all files are emitted.").
				Default([]any{}),
			service.NewDurationField(fsEventInputFieldExistingFilesPollInterval).
				Description("If set to a value greater than zero, the configured paths are scanned repeatedly at this interval and CREATE events are emitted for files not yet seen. Files are tracked across scans to avoid duplicate events.").
				Default("0s"),
			service.NewDurationField(fsEventInputFieldExistingFilesMinAge).
				Description("When `existing_files_poll_interval` is active, only files whose last modification time is older than this duration are emitted. Useful to skip files that are still being written.").
				Default("0s"),
		)
}

func init() {
	RegisterFsEventInput()
}

func RegisterFsEventInput() {
	err := service.RegisterInput("fsevent", fsEventInputSpec(),
		func(pConf *service.ParsedConfig, res *service.Resources) (service.Input, error) {
			return fsEventWatcherFromParsed(pConf, res)
		})
	if err != nil {
		panic(err)
	}
}

type fsEventWatcher struct {
	log                       *service.Logger
	nm                        *service.Resources
	watcher                   *fsnotify.Watcher
	eventChan                 chan watcherEventMsg
	closeCh                   chan struct{}
	cMut                      sync.RWMutex
	paths                     []string
	recursive                 bool
	watchNewSubdirs           bool
	writeDedupTimeout         time.Duration
	writeDedupTimers          map[string]*time.Timer
	extensions                map[string]struct{}
	existingFilesPollInterval time.Duration
	existingFilesMinAge       time.Duration
	scannedFiles              map[string]struct{}
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

	wn, err := conf.FieldBool(fsEventInputFieldWatchNewSubdirs)
	if err != nil {
		return nil, err
	}

	recursive, err := conf.FieldBool(fsEventInputFieldIsRecursive)
	if err != nil {
		return nil, err
	}

	wdt, err := conf.FieldDuration(fsEventInputFieldWriteDedupTimeout)
	if err != nil {
		return nil, err
	}

	extList, err := conf.FieldStringList(fsEventInputFieldExtensions)
	if err != nil {
		return nil, err
	}
	extMap := make(map[string]struct{}, len(extList))
	for _, ext := range extList {
		if len(ext) > 0 && ext[0] != '.' {
			ext = "." + ext
		}
		extMap[ext] = struct{}{}
	}

	pollInterval, err := conf.FieldDuration(fsEventInputFieldExistingFilesPollInterval)
	if err != nil {
		return nil, err
	}

	minAge, err := conf.FieldDuration(fsEventInputFieldExistingFilesMinAge)
	if err != nil {
		return nil, err
	}

	return &fsEventWatcher{
		nm:                        nm,
		log:                       nm.Logger(),
		paths:                     paths,
		recursive:                 recursive,
		watchNewSubdirs:           wn,
		writeDedupTimeout:         wdt,
		writeDedupTimers:          make(map[string]*time.Timer),
		extensions:                extMap,
		existingFilesPollInterval: pollInterval,
		existingFilesMinAge:       minAge,
		scannedFiles:              make(map[string]struct{}),
	}, nil
}

func (f *fsEventWatcher) matchesExtension(path string) bool {
	if len(f.extensions) == 0 {
		return true
	}
	_, ok := f.extensions[filepath.Ext(path)]
	return ok
}

func (f *fsEventWatcher) scanExistingFiles(eventChan chan watcherEventMsg, closeCh chan struct{}) {
	now := time.Now()
	for _, p := range f.paths {
		_ = filepath.WalkDir(p, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return nil
			}
			if d.IsDir() {
				if !f.recursive && path != p {
					return filepath.SkipDir
				}
				return nil
			}
			if !f.matchesExtension(path) {
				return nil
			}
			info, err := d.Info()
			if err != nil {
				return nil
			}
			if f.existingFilesMinAge > 0 && now.Sub(info.ModTime()) < f.existingFilesMinAge {
				return nil
			}
			f.cMut.Lock()
			_, alreadyEmitted := f.scannedFiles[path]
			if !alreadyEmitted {
				f.scannedFiles[path] = struct{}{}
			}
			f.cMut.Unlock()
			if alreadyEmitted {
				return nil
			}
			select {
			case eventChan <- watcherEventMsg{
				event:         fsnotify.Event{Op: fsnotify.Create, Name: path},
				timestampUnix: info.ModTime().Unix(),
			}:
			case <-closeCh:
				return filepath.SkipAll
			}
			return nil
		})
	}
}

func (f *fsEventWatcher) handleWriteDeduplication(e fsnotify.Event) {
	// We just want to watch for file creation, so ignore everything
	// outside of Create and Write.
	if !e.Has(fsnotify.Create) && !e.Has(fsnotify.Write) {
		return
	}

	if !f.matchesExtension(e.Name) {
		return
	}

	// Get timer.
	f.cMut.Lock()
	defer f.cMut.Unlock()
	t, ok := f.writeDedupTimers[e.Name]

	// No timer yet, so create one.
	if !ok {
		t = time.AfterFunc(math.MaxInt64, func() {
			f.cMut.Lock()
			delete(f.writeDedupTimers, e.Name)
			f.cMut.Unlock()

			msg := watcherEventMsg{
				event: fsnotify.Event{
					Op:   fsnotify.Create,
					Name: e.Name,
				},
				timestampUnix: time.Now().Unix(),
			}

			f.eventChan <- msg
		})

		t.Stop()

		f.writeDedupTimers[e.Name] = t
	}

	t.Reset(f.writeDedupTimeout)
}

func (f *fsEventWatcher) Connect(ctx context.Context) error {
	f.cMut.Lock()
	defer f.cMut.Unlock()

	if f.watcher != nil {
		return nil
	}

	eventChan := make(chan watcherEventMsg)
	closeCh := make(chan struct{})

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
		if f.recursive {
			_ = filepath.WalkDir(path, func(path string, d fs.DirEntry, err error) error {
				if d.IsDir() {
					_ = watcher.Add(path)
				}
				return nil
			})
		}
	}

	// If configured, start a recurring scan that emits CREATE events for
	// pre-existing (and newly appearing) files whose age exceeds existingFilesMinAge.
	if f.existingFilesPollInterval > 0 {
		go func() {
			ticker := time.NewTicker(f.existingFilesPollInterval)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					f.scanExistingFiles(eventChan, closeCh)
				case <-closeCh:
					return
				}
			}
		}()
	}

	go func() {
		defer close(eventChan)

		for {
			select {
			case event, ok := <-watcher.Events:
				if !ok {
					return
				}

				// a create could be a new subdir. if enabled we will check
				if f.watchNewSubdirs && event.Has(fsnotify.Create) {
					st, err := os.Stat(event.Name)
					if err != nil {
						f.log.Warnf("Cannot check for new subpath: %s", err)
						continue
					}

					// if it is a file dont add it.
					if !st.IsDir() {
						continue
					}

					// adding the same path more than once is a noop,
					// so safe even though it is already in the watchlist
					if err := f.watcher.Add(event.Name); err != nil {
						f.log.Warnf("Failed to add path %v: %s", event.Name, err)
					}
				}

				// Evict removed/renamed files from the scanned set so they are
				// re-emitted if they reappear during a future poll scan.
				if event.Has(fsnotify.Remove) || event.Has(fsnotify.Rename) {
					f.cMut.Lock()
					delete(f.scannedFiles, event.Name)
					f.cMut.Unlock()
				}

				if f.writeDedupTimeout != 0 {
					f.handleWriteDeduplication(event)
				} else {
					if !f.matchesExtension(event.Name) {
						continue
					}
					msg := watcherEventMsg{
						event:         event,
						timestampUnix: time.Now().Unix(),
					}

					f.eventChan <- msg
				}
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
	f.closeCh = closeCh
	return nil
}

func (f *fsEventWatcher) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	f.cMut.RLock()
	eventChan := f.eventChan
	f.cMut.RUnlock()

	if eventChan == nil {
		return nil, nil, service.ErrNotConnected
	}

	select {
	case msg, open := <-eventChan:
		if !open {
			f.cMut.Lock()
			f.eventChan = nil
			f.watcher = nil
			f.cMut.Unlock()
			return nil, nil, service.ErrEndOfInput
		}

		message := service.NewMessage(nil)
		message.MetaSetMut("fsevent_operation", msg.event.Op.String())
		message.MetaSetMut("fsevent_path", msg.event.Name)
		message.MetaSetMut("fsevent_mod_time_unix", msg.timestampUnix)
		timestamp := time.Unix(msg.timestampUnix, 0).Format(time.RFC3339)
		message.MetaSetMut("fsevent_mod_time", timestamp)

		// Try to get file info, but don't fail if the file doesn't exist (e.g., for DELETE events)
		st, err := os.Stat(msg.event.Name)
		if err == nil {
			message.MetaSetMut("fsevent_is_dir", st.IsDir())
		} else {
			// For deleted files, we can't stat them, so set is_dir to false
			message.MetaSetMut("fsevent_is_dir", false)
		}

		// check to see if the watchlist is empty.
		if f.watcher != nil && len(f.watcher.WatchList()) == 0 {
			f.log.Warn("Nothing being watched. Closing the input.")
			go f.Close(context.TODO())
		}

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

	if f.closeCh != nil {
		close(f.closeCh)
		f.closeCh = nil
	}

	var err error
	if f.watcher != nil {
		err = f.watcher.Close()
		f.watcher = nil
	}
	return err
}
