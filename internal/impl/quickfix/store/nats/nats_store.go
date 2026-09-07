// Package nats provides a MessageStore implementation backed by a NATS
// JetStream Object Store bucket. All sessions share a single bucket; each
// session's data is namespaced via an object-name prefix derived from the
// SessionID.
package nats

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	pkgerrors "github.com/pkg/errors"

	"github.com/quickfixgo/quickfix"
)

// Config configures the NATS-Object-Store-backed FIX message store.
type Config struct {
	// URLs is a comma-separated list of NATS server URLs.
	URLs string
	// CredentialsFile is an optional path to a NATS credentials (.creds) file.
	CredentialsFile string
	// Token is an optional auth token.
	Token string
	// TLSConfig is an optional TLS configuration for the NATS connection.
	TLSConfig *tls.Config
	// Bucket is the JetStream Object Store bucket used for message storage.
	// Defaults to "quickfix".
	Bucket string
	// CompactionInterval is the interval at which Object Store objects are
	// compacted. Zero disables background compaction.
	CompactionInterval time.Duration
	// Replicas is the number of JetStream replicas kept for the bucket.
	// Defaults to 1.
	Replicas int
}

const (
	// DefaultBucket is the JetStream Object Store bucket used when none is
	// configured.
	DefaultBucket = "quickfix"

	defaultOpTimeout          = 10 * time.Second
	defaultCompactionInterval = 10 * time.Second
	defaultReplicas           = 1

	sessionSuffix = "session"
	bodySuffix    = "body"
	headerSuffix  = "header"
	messagePrefix = "msg"

	// dateLayout is the YYYYMMDD suffix appended to rotated body/header
	// objects so each calendar day gets its own pair.
	dateLayout = "20060102"
)

// nonAlnum matches characters that are not safe to use directly inside a
// NATS object name segment; we keep the prefix conservative for readability.
var nonAlnum = regexp.MustCompile(`[^A-Za-z0-9]+`)

type natsStoreFactory struct {
	cfg Config
}

type natsStore struct {
	sessionID     quickfix.SessionID
	cache         quickfix.MessageStore
	nc            *nats.Conn
	js            jetstream.JetStream
	obj           jetstream.ObjectStore
	bucket        string
	sessionObject string
	bodyPrefix    string
	headerPrefix  string
	messagePrefix string
	opTimeout     time.Duration

	now func() time.Time

	compactionInterval time.Duration
	compactMu          sync.Mutex
	stopCompaction     chan struct{}
	compactionDone     chan struct{}
}

type sessionRecord struct {
	CreationTime   time.Time `json:"creation_time"`
	IncomingSeqNum int       `json:"incoming_seq_num"`
	OutgoingSeqNum int       `json:"outgoing_seq_num"`
}

// NewStoreFactory returns a NATS-Object-Store-backed implementation of
// MessageStoreFactory. The config is validated up front so an invalid value
// fails before any NATS connection is attempted.
func NewStoreFactory(cfg Config) (quickfix.MessageStoreFactory, error) {
	if cfg.URLs == "" {
		return nil, errors.New("at least one NATS server URL is required")
	}
	if cfg.Bucket == "" {
		cfg.Bucket = DefaultBucket
	}
	if cfg.Replicas == 0 {
		cfg.Replicas = defaultReplicas
	}
	if cfg.Replicas < 1 {
		return nil, fmt.Errorf("replicas must be a positive integer, got %d", cfg.Replicas)
	}
	return natsStoreFactory{cfg: cfg}, nil
}

// Create creates a new MessageStore implementation backed by NATS Object
// Store for the given session.
func (f natsStoreFactory) Create(sessionID quickfix.SessionID) (quickfix.MessageStore, error) {
	var opts []nats.Option
	if f.cfg.CredentialsFile != "" {
		opts = append(opts, nats.UserCredentials(f.cfg.CredentialsFile))
	}
	if f.cfg.Token != "" {
		opts = append(opts, nats.Token(f.cfg.Token))
	}
	if f.cfg.TLSConfig != nil {
		opts = append(opts, nats.Secure(f.cfg.TLSConfig))
	}

	return newNatsStore(sessionID, f.cfg.URLs, f.cfg.Bucket, opts, f.cfg.CompactionInterval, f.cfg.Replicas)
}

func sessionPrefix(sid quickfix.SessionID) string {
	parts := []string{
		sid.BeginString,
		sid.SenderCompID, sid.SenderSubID, sid.SenderLocationID,
		sid.TargetCompID, sid.TargetSubID, sid.TargetLocationID,
		sid.Qualifier,
	}
	for i, p := range parts {
		parts[i] = nonAlnum.ReplaceAllString(p, "_")
	}
	return strings.Join(parts, "-")
}

func newNatsStore(sessionID quickfix.SessionID, url, bucket string, opts []nats.Option, compactionInterval time.Duration, replicas int) (*natsStore, error) {
	memStore, err := quickfix.NewMemoryStoreFactory().Create(sessionID)
	if err != nil {
		return nil, pkgerrors.Wrap(err, "cache creation")
	}

	nc, err := nats.Connect(url, opts...)
	if err != nil {
		return nil, pkgerrors.Wrap(err, "nats connect")
	}

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, pkgerrors.Wrap(err, "jetstream new")
	}

	ctx, cancel := context.WithTimeout(context.Background(), defaultOpTimeout)
	defer cancel()
	obj, err := js.CreateOrUpdateObjectStore(ctx, jetstream.ObjectStoreConfig{Bucket: bucket, Replicas: replicas})
	if err != nil {
		nc.Close()
		return nil, pkgerrors.Wrap(err, "create/open object store")
	}

	prefix := sessionPrefix(sessionID)
	store := &natsStore{
		sessionID:          sessionID,
		cache:              memStore,
		nc:                 nc,
		js:                 js,
		obj:                obj,
		bucket:             bucket,
		sessionObject:      prefix + "." + sessionSuffix,
		bodyPrefix:         prefix + "." + bodySuffix,
		headerPrefix:       prefix + "." + headerSuffix,
		messagePrefix:      prefix + "." + messagePrefix + ".",
		opTimeout:          defaultOpTimeout,
		now:                time.Now,
		compactionInterval: compactionInterval,
	}

	if err := store.cache.Reset(); err != nil {
		nc.Close()
		return nil, pkgerrors.Wrap(err, "cache reset")
	}

	if err := store.populateCache(); err != nil {
		nc.Close()
		return nil, err
	}

	if store.compactionInterval > 0 {
		store.stopCompaction = make(chan struct{})
		store.compactionDone = make(chan struct{})
		go store.compactionLoop()
	}
	return store, nil
}

func (s *natsStore) ctx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), s.opTimeout)
}

func (s *natsStore) messageObjectName(seqNum int) string {
	// Zero-pad so lexicographic ordering matches numeric ordering.
	return fmt.Sprintf("%s%010d", s.messagePrefix, seqNum)
}

func (s *natsStore) parseSeqNum(objectName string) (int, bool) {
	if !strings.HasPrefix(objectName, s.messagePrefix) {
		return 0, false
	}
	n, err := strconv.Atoi(strings.TrimPrefix(objectName, s.messagePrefix))
	if err != nil {
		return 0, false
	}
	return n, true
}

func (s *natsStore) bodyObjectFor(date string) string {
	if date == "" {
		return s.bodyPrefix
	}
	return s.bodyPrefix + "." + date
}

func (s *natsStore) headerObjectFor(date string) string {
	if date == "" {
		return s.headerPrefix
	}
	return s.headerPrefix + "." + date
}

// parseBodyObject reports whether name is a body object for this session. The
// returned date is the YYYYMMDD suffix, or "" for the legacy un-dated object
// retained for backward compatibility with stores written before daily
// rotation was introduced.
func (s *natsStore) parseBodyObject(name string) (string, bool) {
	if name == s.bodyPrefix {
		return "", true
	}
	if rest, ok := strings.CutPrefix(name, s.bodyPrefix+"."); ok {
		return rest, true
	}
	return "", false
}

// parseHeaderObject is the header-object counterpart to parseBodyObject.
func (s *natsStore) parseHeaderObject(name string) (string, bool) {
	if name == s.headerPrefix {
		return "", true
	}
	if rest, ok := strings.CutPrefix(name, s.headerPrefix+"."); ok {
		return rest, true
	}
	return "", false
}

func (s *natsStore) today() string {
	return s.now().UTC().Format(dateLayout)
}

func (s *natsStore) loadSessionRecord() (*sessionRecord, error) {
	ctx, cancel := s.ctx()
	defer cancel()
	data, err := s.obj.GetBytes(ctx, s.sessionObject)
	if err != nil {
		if errors.Is(err, jetstream.ErrObjectNotFound) {
			return nil, nil
		}
		return nil, pkgerrors.Wrap(err, "get session record")
	}
	rec := &sessionRecord{}
	if err := json.Unmarshal(data, rec); err != nil {
		return nil, pkgerrors.Wrap(err, "decode session record")
	}
	return rec, nil
}

func (s *natsStore) saveSessionRecord(rec *sessionRecord) error {
	data, err := json.Marshal(rec)
	if err != nil {
		return pkgerrors.Wrap(err, "encode session record")
	}
	ctx, cancel := s.ctx()
	defer cancel()
	if _, err := s.obj.PutBytes(ctx, s.sessionObject, data); err != nil {
		return pkgerrors.Wrap(err, "put session record")
	}
	return nil
}

func (s *natsStore) populateCache() error {
	rec, err := s.loadSessionRecord()
	if err != nil {
		return err
	}
	if rec != nil {
		s.cache.SetCreationTime(rec.CreationTime)
		if err := s.cache.SetNextTargetMsgSeqNum(rec.IncomingSeqNum); err != nil {
			return pkgerrors.Wrap(err, "cache set next target")
		}
		if err := s.cache.SetNextSenderMsgSeqNum(rec.OutgoingSeqNum); err != nil {
			return pkgerrors.Wrap(err, "cache set next sender")
		}
		return nil
	}
	return s.saveSessionRecord(&sessionRecord{
		CreationTime:   s.cache.CreationTime(),
		IncomingSeqNum: s.cache.NextTargetMsgSeqNum(),
		OutgoingSeqNum: s.cache.NextSenderMsgSeqNum(),
	})
}

func (s *natsStore) currentRecord() *sessionRecord {
	return &sessionRecord{
		CreationTime:   s.cache.CreationTime(),
		IncomingSeqNum: s.cache.NextTargetMsgSeqNum(),
		OutgoingSeqNum: s.cache.NextSenderMsgSeqNum(),
	}
}

// Reset deletes all messages for the session and resets seqnums to 1.
func (s *natsStore) Reset() error {
	s.compactMu.Lock()
	defer s.compactMu.Unlock()

	if err := s.deleteAllMessageObjects(); err != nil {
		return err
	}
	if err := s.cache.Reset(); err != nil {
		return err
	}
	return s.saveSessionRecord(s.currentRecord())
}

// deleteAllMessageObjects removes any per-message objects plus the compacted
// body and header objects for this session.
func (s *natsStore) deleteAllMessageObjects() error {
	ctx, cancel := s.ctx()
	defer cancel()
	infos, err := s.obj.List(ctx)
	if err != nil {
		if errors.Is(err, jetstream.ErrNoObjectsFound) {
			return nil
		}
		return pkgerrors.Wrap(err, "list objects")
	}
	for _, info := range infos {
		if !s.isMessageStorageObject(info.Name) {
			continue
		}
		if err := s.deleteObject(info.Name); err != nil {
			return err
		}
	}
	return nil
}

func (s *natsStore) isMessageStorageObject(name string) bool {
	if _, ok := s.parseBodyObject(name); ok {
		return true
	}
	if _, ok := s.parseHeaderObject(name); ok {
		return true
	}
	_, ok := s.parseSeqNum(name)
	return ok
}

func (s *natsStore) deleteObject(name string) error {
	dctx, dcancel := s.ctx()
	defer dcancel()
	if err := s.obj.Delete(dctx, name); err != nil && !errors.Is(err, jetstream.ErrObjectNotFound) {
		return pkgerrors.Wrapf(err, "delete object %s", name)
	}
	return nil
}

// Refresh reloads the store state from NATS.
func (s *natsStore) Refresh() error {
	if err := s.cache.Reset(); err != nil {
		return err
	}
	return s.populateCache()
}

func (s *natsStore) NextSenderMsgSeqNum() int { return s.cache.NextSenderMsgSeqNum() }
func (s *natsStore) NextTargetMsgSeqNum() int { return s.cache.NextTargetMsgSeqNum() }

func (s *natsStore) SetNextSenderMsgSeqNum(next int) error {
	rec := s.currentRecord()
	rec.OutgoingSeqNum = next
	if err := s.saveSessionRecord(rec); err != nil {
		return err
	}
	return s.cache.SetNextSenderMsgSeqNum(next)
}

func (s *natsStore) SetNextTargetMsgSeqNum(next int) error {
	rec := s.currentRecord()
	rec.IncomingSeqNum = next
	if err := s.saveSessionRecord(rec); err != nil {
		return err
	}
	return s.cache.SetNextTargetMsgSeqNum(next)
}

func (s *natsStore) IncrNextSenderMsgSeqNum() error {
	return s.SetNextSenderMsgSeqNum(s.cache.NextSenderMsgSeqNum() + 1)
}

func (s *natsStore) IncrNextTargetMsgSeqNum() error {
	return s.SetNextTargetMsgSeqNum(s.cache.NextTargetMsgSeqNum() + 1)
}

func (s *natsStore) CreationTime() time.Time { return s.cache.CreationTime() }

// SetCreationTime is a no-op for natsStore; creation time is managed via Reset.
func (s *natsStore) SetCreationTime(_ time.Time) {}

func (s *natsStore) SaveMessage(seqNum int, msg []byte) error {
	ctx, cancel := s.ctx()
	defer cancel()
	if _, err := s.obj.PutBytes(ctx, s.messageObjectName(seqNum), msg); err != nil {
		return pkgerrors.Wrap(err, "save message")
	}
	return nil
}

func (s *natsStore) SaveMessageAndIncrNextSenderMsgSeqNum(seqNum int, msg []byte) error {
	if err := s.SaveMessage(seqNum, msg); err != nil {
		return err
	}
	return s.IncrNextSenderMsgSeqNum()
}

// headerEntry describes the location of one message in the compacted body object.
type headerEntry struct {
	SeqNum int
	Offset int64
	Size   int
}

func (s *natsStore) loadHeader(date string) ([]headerEntry, error) {
	ctx, cancel := s.ctx()
	defer cancel()
	data, err := s.obj.GetBytes(ctx, s.headerObjectFor(date))
	if err != nil {
		if errors.Is(err, jetstream.ErrObjectNotFound) {
			return nil, nil
		}
		return nil, pkgerrors.Wrap(err, "get header")
	}
	return parseHeader(data)
}

func parseHeader(data []byte) ([]headerEntry, error) {
	if len(data) == 0 {
		return nil, nil
	}
	var entries []headerEntry
	for line := range strings.SplitSeq(strings.TrimRight(string(data), "\n"), "\n") {
		if line == "" {
			continue
		}
		var e headerEntry
		if _, err := fmt.Sscanf(line, "%d,%d,%d", &e.SeqNum, &e.Offset, &e.Size); err != nil {
			return nil, pkgerrors.Wrapf(err, "parse header line %q", line)
		}
		entries = append(entries, e)
	}
	return entries, nil
}

func formatHeader(entries []headerEntry) []byte {
	var b strings.Builder
	for _, e := range entries {
		fmt.Fprintf(&b, "%d,%d,%d\n", e.SeqNum, e.Offset, e.Size)
	}
	return []byte(b.String())
}

func (s *natsStore) loadBody(date string) ([]byte, error) {
	ctx, cancel := s.ctx()
	defer cancel()
	data, err := s.obj.GetBytes(ctx, s.bodyObjectFor(date))
	if err != nil {
		if errors.Is(err, jetstream.ErrObjectNotFound) {
			return nil, nil
		}
		return nil, pkgerrors.Wrap(err, "get body")
	}
	return data, nil
}

func (s *natsStore) IterateMessages(beginSeqNum, endSeqNum int, cb func([]byte) error) error {
	// Collect every body/header pair (one per rotated day, plus any legacy
	// undated pair) and any uncompacted per-message objects. The compactor
	// takes the same lock when rewriting so each (body,header) read here is
	// internally consistent.
	s.compactMu.Lock()

	listCtx, cancel := s.ctx()
	infos, err := s.obj.List(listCtx)
	cancel()
	if err != nil && !errors.Is(err, jetstream.ErrNoObjectsFound) {
		s.compactMu.Unlock()
		return pkgerrors.Wrap(err, "list objects")
	}

	type pair struct{ hasBody, hasHeader bool }
	pairs := map[string]*pair{}
	var msgObjects []string
	for _, info := range infos {
		if d, ok := s.parseBodyObject(info.Name); ok {
			p := pairs[d]
			if p == nil {
				p = &pair{}
				pairs[d] = p
			}
			p.hasBody = true
			continue
		}
		if d, ok := s.parseHeaderObject(info.Name); ok {
			p := pairs[d]
			if p == nil {
				p = &pair{}
				pairs[d] = p
			}
			p.hasHeader = true
			continue
		}
		if _, ok := s.parseSeqNum(info.Name); ok {
			msgObjects = append(msgObjects, info.Name)
		}
	}

	type entry struct {
		seq    int
		data   []byte
		objKey string
	}
	var entries []entry
	covered := map[int]struct{}{}

	for date, p := range pairs {
		if !p.hasHeader {
			continue
		}
		header, err := s.loadHeader(date)
		if err != nil {
			s.compactMu.Unlock()
			return err
		}
		var body []byte
		if p.hasBody {
			body, err = s.loadBody(date)
			if err != nil {
				s.compactMu.Unlock()
				return err
			}
		}
		for _, h := range header {
			if h.SeqNum < beginSeqNum || h.SeqNum > endSeqNum {
				continue
			}
			end := h.Offset + int64(h.Size)
			if end > int64(len(body)) {
				s.compactMu.Unlock()
				return fmt.Errorf("header %s references body offset %d+%d beyond body length %d", s.headerObjectFor(date), h.Offset, h.Size, len(body))
			}
			entries = append(entries, entry{seq: h.SeqNum, data: body[h.Offset:end]})
			covered[h.SeqNum] = struct{}{}
		}
	}
	s.compactMu.Unlock()

	for _, name := range msgObjects {
		seq, _ := s.parseSeqNum(name)
		if seq < beginSeqNum || seq > endSeqNum {
			continue
		}
		if _, dup := covered[seq]; dup {
			continue
		}
		entries = append(entries, entry{seq: seq, objKey: name})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].seq < entries[j].seq })

	for _, e := range entries {
		data := e.data
		if data == nil {
			gctx, gcancel := s.ctx()
			d, err := s.obj.GetBytes(gctx, e.objKey)
			gcancel()
			if err != nil {
				if errors.Is(err, jetstream.ErrObjectNotFound) {
					continue
				}
				return pkgerrors.Wrapf(err, "get message %s", e.objKey)
			}
			data = d
		}
		if err := cb(data); err != nil {
			return err
		}
	}
	return nil
}

func (s *natsStore) GetMessages(beginSeqNum, endSeqNum int) ([][]byte, error) {
	var msgs [][]byte
	err := s.IterateMessages(beginSeqNum, endSeqNum, func(msg []byte) error {
		msgs = append(msgs, msg)
		return nil
	})
	return msgs, err
}

// compactionLoop runs in the background and merges per-message objects into
// the body+header pair on a fixed interval.
func (s *natsStore) compactionLoop() {
	defer close(s.compactionDone)
	ticker := time.NewTicker(s.compactionInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.stopCompaction:
			_ = s.compact()
			return
		case <-ticker.C:
			_ = s.compact()
		}
	}
}

// compact merges any per-message objects into the body+header objects for the
// current day and removes the now-redundant per-message objects. At 00:00
// UTC a fresh body+header pair is started automatically since the date
// suffix changes.
func (s *natsStore) compact() error {
	s.compactMu.Lock()
	defer s.compactMu.Unlock()

	listCtx, cancel := s.ctx()
	infos, err := s.obj.List(listCtx)
	cancel()
	if err != nil {
		if errors.Is(err, jetstream.ErrNoObjectsFound) {
			return nil
		}
		return pkgerrors.Wrap(err, "list objects")
	}

	type pending struct {
		seq  int
		name string
	}
	var todo []pending
	for _, info := range infos {
		seq, ok := s.parseSeqNum(info.Name)
		if !ok {
			continue
		}
		todo = append(todo, pending{seq: seq, name: info.Name})
	}
	if len(todo) == 0 {
		return nil
	}
	sort.Slice(todo, func(i, j int) bool { return todo[i].seq < todo[j].seq })

	// Build a covered set spanning every existing (dated and legacy) header
	// so we never double-store a message that's already been compacted into
	// a previous day's pair.
	covered := map[int]struct{}{}
	for _, info := range infos {
		date, ok := s.parseHeaderObject(info.Name)
		if !ok {
			continue
		}
		entries, err := s.loadHeader(date)
		if err != nil {
			return err
		}
		for _, e := range entries {
			covered[e.SeqNum] = struct{}{}
		}
	}

	// Today's pair is where new messages get appended.
	today := s.today()
	body, err := s.loadBody(today)
	if err != nil {
		return err
	}
	header, err := s.loadHeader(today)
	if err != nil {
		return err
	}

	bodyChanged := false
	for _, p := range todo {
		if _, exists := covered[p.seq]; exists {
			// Already stored in some day's body+header pair; the per-message
			// object is redundant.
			continue
		}
		gctx, gcancel := s.ctx()
		data, err := s.obj.GetBytes(gctx, p.name)
		gcancel()
		if err != nil {
			if errors.Is(err, jetstream.ErrObjectNotFound) {
				continue
			}
			return pkgerrors.Wrapf(err, "compact: read %s", p.name)
		}
		offset := int64(len(body))
		body = append(body, data...)
		// Append a newline separator so the concatenated body file is easier
		// to inspect with text tools. The separator is excluded from Size, so
		// IterateMessages still slices out exactly the original message bytes.
		body = append(body, '\n')
		header = append(header, headerEntry{SeqNum: p.seq, Offset: offset, Size: len(data)})
		covered[p.seq] = struct{}{}
		bodyChanged = true
	}

	if bodyChanged {
		// Write body before header so the header never references missing bytes.
		putCtx, pcancel := s.ctx()
		if _, err := s.obj.PutBytes(putCtx, s.bodyObjectFor(today), body); err != nil {
			pcancel()
			return pkgerrors.Wrap(err, "compact: put body")
		}
		pcancel()

		hdrCtx, hcancel := s.ctx()
		if _, err := s.obj.PutBytes(hdrCtx, s.headerObjectFor(today), formatHeader(header)); err != nil {
			hcancel()
			return pkgerrors.Wrap(err, "compact: put header")
		}
		hcancel()
	}

	// Remove per-message objects only after the merged content is durably stored.
	for _, p := range todo {
		if err := s.deleteObject(p.name); err != nil {
			return err
		}
	}
	return nil
}

// Close stops the compaction goroutine, runs a final compaction pass, and
// closes the underlying NATS connection.
func (s *natsStore) Close() error {
	if s.stopCompaction != nil {
		close(s.stopCompaction)
		<-s.compactionDone
		s.stopCompaction = nil
	}
	if s.nc != nil {
		s.nc.Close()
		s.nc = nil
	}
	return nil
}
