package nats

import (
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/quickfixgo/quickfix"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

// runJetStreamServer starts an in-process NATS server with JetStream enabled
// and returns its client URL.
func runJetStreamServer(t *testing.T) string {
	t.Helper()
	srv, err := server.NewServer(&server.Options{
		JetStream: true,
		Port:      -1,
		StoreDir:  t.TempDir(),
	})
	require.NoError(t, err)
	go srv.Start()
	require.True(t, srv.ReadyForConnections(10*time.Second), "embedded NATS server did not become ready")
	t.Cleanup(srv.Shutdown)
	return srv.ClientURL()
}

// NatsStoreTestSuite runs all tests in StoreTestSuite against the NATS Object
// Store implementation, backed by an in-process JetStream server.
type NatsStoreTestSuite struct {
	StoreTestSuite
	natsURL string
}

func (suite *NatsStoreTestSuite) SetupSuite() {
	suite.natsURL = runJetStreamServer(suite.T())
}

func (suite *NatsStoreTestSuite) SetupTest() {
	sessionID := quickfix.SessionID{BeginString: "FIX.4.4", SenderCompID: "SENDER", TargetCompID: "TARGET"}
	factory, err := NewStoreFactory(Config{URLs: suite.natsURL, Bucket: "quickfix_test"})
	require.NoError(suite.T(), err)

	suite.MsgStore, err = factory.Create(sessionID)
	require.NoError(suite.T(), err)
	require.NoError(suite.T(), suite.MsgStore.Reset())
}

func (suite *NatsStoreTestSuite) TearDownTest() {
	if suite.MsgStore != nil {
		require.NoError(suite.T(), suite.MsgStore.Close())
	}
}

func TestNatsStoreTestSuite(t *testing.T) {
	suite.Run(t, new(NatsStoreTestSuite))
}

// TestNewStoreFactoryValidation verifies that invalid configuration fails fast
// before any NATS connection is attempted. These cases do not require a
// running NATS server.
func TestNewStoreFactoryValidation(t *testing.T) {
	_, err := NewStoreFactory(Config{URLs: ""})
	require.Error(t, err, "expected empty URLs to be rejected")

	for _, replicas := range []int{-1, -100} {
		_, err = NewStoreFactory(Config{URLs: "nats://127.0.0.1:1", Replicas: replicas})
		require.Error(t, err, "expected replicas=%d to be rejected", replicas)
	}
}

// TestReplicasApplied verifies that the configured replicas value is applied
// to the underlying Object Store bucket. A single-node server can only satisfy
// 1 replica.
func TestReplicasApplied(t *testing.T) {
	natsURL := runJetStreamServer(t)

	sessionID := quickfix.SessionID{BeginString: "FIX.4.4", SenderCompID: "REPLICA_S", TargetCompID: "REPLICA_T"}
	factory, err := NewStoreFactory(Config{URLs: natsURL, Bucket: "quickfix_test", Replicas: 1})
	require.NoError(t, err)

	store, err := factory.Create(sessionID)
	require.NoError(t, err)
	defer func() { _ = store.Close() }()

	ns, ok := store.(*natsStore)
	require.True(t, ok)

	ctx, cancel := ns.ctx()
	defer cancel()
	status, err := ns.obj.Status(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, status.Replicas())
}

// TestCompactionMergesMessages verifies that the background compactor merges
// per-message objects into the shared body+header pair and removes the
// per-message objects, while messages remain readable throughout.
func TestCompactionMergesMessages(t *testing.T) {
	natsURL := runJetStreamServer(t)

	sessionID := quickfix.SessionID{BeginString: "FIX.4.4", SenderCompID: "COMPACT_S", TargetCompID: "COMPACT_T"}
	factory, err := NewStoreFactory(Config{
		URLs:               natsURL,
		Bucket:             "quickfix_test",
		CompactionInterval: 200 * time.Millisecond,
	})
	require.NoError(t, err)

	store, err := factory.Create(sessionID)
	require.NoError(t, err)
	defer func() { _ = store.Close() }()
	require.NoError(t, store.Reset())

	ns, ok := store.(*natsStore)
	require.True(t, ok)

	expected := map[int]string{
		1: "alpha",
		2: "bravo",
		3: "charlie",
		4: "delta",
		5: "echo",
	}
	for i := 1; i <= 5; i++ {
		require.NoError(t, store.SaveMessage(i, []byte(expected[i])))
	}

	// Wait for compaction to run.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if msgsCompacted(t, ns) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	require.True(t, msgsCompacted(t, ns), "expected per-message objects to be merged into body+header")

	msgs, err := store.GetMessages(1, 5)
	require.NoError(t, err)
	require.Len(t, msgs, 5)
	for i, m := range msgs {
		require.Equal(t, expected[i+1], string(m))
	}

	// Saving more messages after compaction should still be readable, and they
	// should be merged on a subsequent compaction pass.
	require.NoError(t, store.SaveMessage(6, []byte("foxtrot")))
	msgs, err = store.GetMessages(1, 6)
	require.NoError(t, err)
	require.Len(t, msgs, 6)
	require.Equal(t, "foxtrot", string(msgs[5]))
}

func msgsCompacted(t *testing.T, ns *natsStore) bool {
	t.Helper()
	ctx, cancel := ns.ctx()
	defer cancel()
	infos, err := ns.obj.List(ctx)
	if err != nil {
		return false
	}
	hasBody, hasHeader, hasMsg := false, false, false
	for _, i := range infos {
		if _, ok := ns.parseBodyObject(i.Name); ok {
			hasBody = true
		}
		if _, ok := ns.parseHeaderObject(i.Name); ok {
			hasHeader = true
		}
		if _, ok := ns.parseSeqNum(i.Name); ok {
			hasMsg = true
		}
	}
	return hasBody && hasHeader && !hasMsg
}

// TestCompactionRotatesDaily verifies that when the wall clock advances past
// midnight, subsequent compactions write to a fresh body+header pair for the
// new day while previously rotated pairs remain readable.
func TestCompactionRotatesDaily(t *testing.T) {
	natsURL := runJetStreamServer(t)

	sessionID := quickfix.SessionID{BeginString: "FIX.4.4", SenderCompID: "ROTATE_S", TargetCompID: "ROTATE_T"}
	factory, err := NewStoreFactory(Config{URLs: natsURL, Bucket: "quickfix_test"})
	require.NoError(t, err)

	store, err := factory.Create(sessionID)
	require.NoError(t, err)
	defer func() { _ = store.Close() }()
	require.NoError(t, store.Reset())

	ns, ok := store.(*natsStore)
	require.True(t, ok)

	day1 := time.Date(2026, 5, 21, 23, 59, 30, 0, time.UTC)
	day2 := day1.Add(2 * time.Minute) // crosses midnight UTC

	// Day 1 messages.
	ns.now = func() time.Time { return day1 }
	require.NoError(t, store.SaveMessage(1, []byte("day1-msg1")))
	require.NoError(t, store.SaveMessage(2, []byte("day1-msg2")))
	require.NoError(t, ns.compact())

	// Day 2 messages.
	ns.now = func() time.Time { return day2 }
	require.NoError(t, store.SaveMessage(3, []byte("day2-msg3")))
	require.NoError(t, ns.compact())

	ctx, cancel := ns.ctx()
	defer cancel()
	infos, err := ns.obj.List(ctx)
	require.NoError(t, err)
	dates := map[string]struct{}{}
	for _, i := range infos {
		if d, ok := ns.parseBodyObject(i.Name); ok {
			dates[d] = struct{}{}
		}
	}
	require.Contains(t, dates, "20260521")
	require.Contains(t, dates, "20260522")

	msgs, err := store.GetMessages(1, 3)
	require.NoError(t, err)
	require.Equal(t, []string{"day1-msg1", "day1-msg2", "day2-msg3"},
		[]string{string(msgs[0]), string(msgs[1]), string(msgs[2])})
}
