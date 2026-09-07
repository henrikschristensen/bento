package quickfix

import (
	"strings"
	"testing"
	"time"

	goquickfix "github.com/quickfixgo/quickfix"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/service"
)

func TestStoreDefaultsToMemory(t *testing.T) {
	conf, err := quickfixInputSpec().ParseYAML(`
connection_type: acceptor
settings_file: `+writeTempCfg(t, testEngineSettings)+`
`, nil)
	require.NoError(t, err)

	r, err := newQuickfixInputFromParsed(conf, service.MockResources())
	require.NoError(t, err)
	assert.Nil(t, r.cfg.store)
}

func TestStoreNatsParsed(t *testing.T) {
	conf, err := quickfixInputSpec().ParseYAML(`
connection_type: acceptor
settings_file: `+writeTempCfg(t, testEngineSettings)+`
store:
  nats:
    urls:
      - nats://localhost:4222
      - nats://localhost:4223
    bucket: fixstate
    token: s3cret
    replicas: 3
    compaction_interval: 1m
`, nil)
	require.NoError(t, err)

	r, err := newQuickfixInputFromParsed(conf, service.MockResources())
	require.NoError(t, err)

	store, ok := r.cfg.store.(*natsStoreConfig)
	require.True(t, ok, "expected *natsStoreConfig, got %T", r.cfg.store)
	assert.Equal(t, "nats://localhost:4222,nats://localhost:4223", store.urls)
	assert.Equal(t, "fixstate", store.bucket)
	assert.Equal(t, "s3cret", store.token)
	assert.Equal(t, 3, store.replicas)
	assert.Equal(t, time.Minute, store.compactionInterval)
	assert.False(t, store.tlsEnabled)
}

func TestStoreNatsRejectsInvalidReplicas(t *testing.T) {
	conf, err := quickfixInputSpec().ParseYAML(`
connection_type: acceptor
settings_file: `+writeTempCfg(t, testEngineSettings)+`
store:
  nats:
    urls: [nats://localhost:4222]
    replicas: 0
`, nil)
	require.NoError(t, err)

	_, err = newQuickfixInputFromParsed(conf, service.MockResources())
	require.ErrorContains(t, err, "replicas")
}

func TestStoreFileParsed(t *testing.T) {
	conf, err := quickfixInputSpec().ParseYAML(`
connection_type: acceptor
settings_file: `+writeTempCfg(t, testEngineSettings)+`
store:
  file:
    path: /var/lib/bento/fix
`, nil)
	require.NoError(t, err)

	r, err := newQuickfixInputFromParsed(conf, service.MockResources())
	require.NoError(t, err)

	store, ok := r.cfg.store.(*fileStoreConfig)
	require.True(t, ok, "expected *fileStoreConfig, got %T", r.cfg.store)
	assert.Equal(t, "/var/lib/bento/fix", store.path)
	assert.True(t, store.sync, "sync must default to true")
}

func TestStoreFileSyncDisabled(t *testing.T) {
	conf, err := quickfixInputSpec().ParseYAML(`
connection_type: acceptor
settings_file: `+writeTempCfg(t, testEngineSettings)+`
store:
  file:
    path: /var/lib/bento/fix
    sync: false
`, nil)
	require.NoError(t, err)

	r, err := newQuickfixInputFromParsed(conf, service.MockResources())
	require.NoError(t, err)

	store, ok := r.cfg.store.(*fileStoreConfig)
	require.True(t, ok, "expected *fileStoreConfig, got %T", r.cfg.store)
	assert.False(t, store.sync)
}

func TestStoreSQLParsed(t *testing.T) {
	conf, err := quickfixInputSpec().ParseYAML(`
connection_type: acceptor
settings_file: `+writeTempCfg(t, testEngineSettings)+`
store:
  sql:
    driver: postgres
    dsn: postgres://user:pass@localhost:5432/fix?sslmode=disable
    sessions_table: fix_sessions
    messages_table: fix_messages
`, nil)
	require.NoError(t, err)

	r, err := newQuickfixInputFromParsed(conf, service.MockResources())
	require.NoError(t, err)

	store, ok := r.cfg.store.(*sqlStoreConfig)
	require.True(t, ok, "expected *sqlStoreConfig, got %T", r.cfg.store)
	assert.Equal(t, "postgres", store.driver)
	assert.Equal(t, "postgres://user:pass@localhost:5432/fix?sslmode=disable", store.dsn)
	assert.Equal(t, "fix_sessions", store.sessionsTable)
	assert.Equal(t, "fix_messages", store.messagesTable)
}

func TestStoreSQLTableNamesDefault(t *testing.T) {
	conf, err := quickfixInputSpec().ParseYAML(`
connection_type: acceptor
settings_file: `+writeTempCfg(t, testEngineSettings)+`
store:
  sql:
    driver: mysql
    dsn: user:pass@tcp(localhost:3306)/fix
`, nil)
	require.NoError(t, err)

	r, err := newQuickfixInputFromParsed(conf, service.MockResources())
	require.NoError(t, err)

	store, ok := r.cfg.store.(*sqlStoreConfig)
	require.True(t, ok, "expected *sqlStoreConfig, got %T", r.cfg.store)
	assert.Empty(t, store.sessionsTable, "unset table names defer to quickfixgo defaults")
	assert.Empty(t, store.messagesTable, "unset table names defer to quickfixgo defaults")
}

func TestStoreSQLRejectsMissingDSN(t *testing.T) {
	// dsn is required by the spec whenever store.sql is present, so the
	// rejection happens at parse time.
	_, err := quickfixInputSpec().ParseYAML(`
connection_type: acceptor
settings_file: `+writeTempCfg(t, testEngineSettings)+`
store:
  sql:
    driver: postgres
`, nil)
	require.ErrorContains(t, err, "dsn")
}

func TestStoreMongoParsed(t *testing.T) {
	conf, err := quickfixInputSpec().ParseYAML(`
connection_type: acceptor
settings_file: `+writeTempCfg(t, testEngineSettings)+`
store:
  mongo:
    url: mongodb://user:pass@localhost:27017
    database: fixstore
    replica_set: rs0
`, nil)
	require.NoError(t, err)

	r, err := newQuickfixInputFromParsed(conf, service.MockResources())
	require.NoError(t, err)

	store, ok := r.cfg.store.(*mongoStoreConfig)
	require.True(t, ok, "expected *mongoStoreConfig, got %T", r.cfg.store)
	assert.Equal(t, "mongodb://user:pass@localhost:27017", store.url)
	assert.Equal(t, "fixstore", store.database)
	assert.Equal(t, "rs0", store.replicaSet)
}

func TestStoreRejectsMultipleBackends(t *testing.T) {
	conf, err := quickfixInputSpec().ParseYAML(`
connection_type: acceptor
settings_file: `+writeTempCfg(t, testEngineSettings)+`
store:
  file:
    path: /var/lib/bento/fix
  mongo:
    url: mongodb://localhost:27017
    database: fixstore
`, nil)
	require.NoError(t, err)

	_, err = newQuickfixInputFromParsed(conf, service.MockResources())
	require.ErrorContains(t, err, "only one")
}

func TestStoreSettingsKeysRejected(t *testing.T) {
	for _, tc := range []struct {
		key   string
		value string
		field string
	}{
		{"NatsStoreURL", "nats://localhost:4222", "store.nats"},
		{"NatsStoreBucket", "fixstate", "store.nats"},
		{"FileStorePath", "/tmp/fix", "store.file"},
		{"FileStoreSync", "N", "store.file"},
		{"SQLStoreDriver", "postgres", "store.sql"},
		{"SQLStoreDataSourceName", "postgres://localhost/db", "store.sql"},
		{"SQLStoreConnMaxLifetime", "1m", "store.sql"},
		{"SQLStoreMessagesTableName", "messages", "store.sql"},
		{"SQLStoreSessionsTableName", "sessions", "store.sql"},
		{"MongoStoreConnection", "mongodb://localhost:27017", "store.mongo"},
		{"MongoStoreDatabase", "fixstore", "store.mongo"},
		{"MongoStoreReplicaSet", "rs0", "store.mongo"},
	} {
		t.Run(tc.key, func(t *testing.T) {
			settings := testEngineSettings + tc.key + "=" + tc.value + "\n"
			conf, err := quickfixInputSpec().ParseYAML(`
connection_type: acceptor
settings_file: `+writeTempCfg(t, settings)+`
`, nil)
			require.NoError(t, err)

			_, err = newQuickfixInputFromParsed(conf, service.MockResources())
			require.ErrorContains(t, err, tc.key)
			require.ErrorContains(t, err, tc.field)
		})
	}
}

func TestAcquireEngineStoreTypeConflict(t *testing.T) {
	installFakeBackend(t, &fakeBackend{})

	cfg := testConnConfig("typeconf")
	cfg.store = &natsStoreConfig{urls: "nats://localhost:4222", bucket: "a", replicas: 1}
	h1, err := acquireEngine(cfg, testLogger(), &recordingSubscriber{})
	require.NoError(t, err)
	t.Cleanup(h1.release)

	// A different backend type for the same name must fail, even if its
	// settings happen to be valid.
	for _, other := range []fixStore{
		&fileStoreConfig{path: "/tmp/fix", sync: true},
		&sqlStoreConfig{driver: "postgres", dsn: "postgres://localhost/db"},
		&mongoStoreConfig{url: "mongodb://localhost:27017", database: "fixstore"},
	} {
		different := testConnConfig("typeconf")
		different.store = other
		_, err = acquireEngine(different, testLogger(), &recordingSubscriber{})
		require.ErrorContains(t, err, "store", "backend type %T must conflict with the registered nats store", other)
	}
}

func TestFileStoreRoundTrip(t *testing.T) {
	qf, err := goquickfix.ParseSettings(strings.NewReader(testEngineSettings))
	require.NoError(t, err)

	c := &fileStoreConfig{path: t.TempDir(), sync: true}
	factory, err := c.storeFactory(qf)
	require.NoError(t, err)

	store, err := factory.Create(testEngineSessionID)
	require.NoError(t, err)
	require.NoError(t, store.SetNextSenderMsgSeqNum(42))
	require.NoError(t, store.SetNextTargetMsgSeqNum(43))
	require.NoError(t, store.Close())

	// A new store over the same path recovers the persisted sequence numbers.
	restored, err := factory.Create(testEngineSessionID)
	require.NoError(t, err)
	t.Cleanup(func() { _ = restored.Close() })
	assert.Equal(t, 42, restored.NextSenderMsgSeqNum())
	assert.Equal(t, 43, restored.NextTargetMsgSeqNum())
}

func TestStoreFactoriesInjectSettings(t *testing.T) {
	newSettings := func(t *testing.T) *goquickfix.Settings {
		qf, err := goquickfix.ParseSettings(strings.NewReader(testEngineSettings))
		require.NoError(t, err)
		return qf
	}

	t.Run("file", func(t *testing.T) {
		qf := newSettings(t)
		c := &fileStoreConfig{path: "/tmp/fix", sync: false}
		factory, err := c.storeFactory(qf)
		require.NoError(t, err)
		require.NotNil(t, factory)

		v, err := qf.GlobalSettings().Setting("FileStorePath")
		require.NoError(t, err)
		assert.Equal(t, "/tmp/fix", v)
		v, err = qf.GlobalSettings().Setting("FileStoreSync")
		require.NoError(t, err)
		assert.Equal(t, "N", v)
	})

	t.Run("sql", func(t *testing.T) {
		qf := newSettings(t)
		c := &sqlStoreConfig{driver: "postgres", dsn: "postgres://localhost/db", sessionsTable: "s", messagesTable: "m"}
		factory, err := c.storeFactory(qf)
		require.NoError(t, err)
		require.NotNil(t, factory)

		for key, want := range map[string]string{
			"SQLStoreDriver":            "postgres",
			"SQLStoreDataSourceName":    "postgres://localhost/db",
			"SQLStoreSessionsTableName": "s",
			"SQLStoreMessagesTableName": "m",
		} {
			v, err := qf.GlobalSettings().Setting(key)
			require.NoError(t, err)
			assert.Equal(t, want, v)
		}
	})

	t.Run("mongo", func(t *testing.T) {
		qf := newSettings(t)
		c := &mongoStoreConfig{url: "mongodb://localhost:27017", database: "fixstore", replicaSet: "rs0"}
		factory, err := c.storeFactory(qf)
		require.NoError(t, err)
		require.NotNil(t, factory)

		for key, want := range map[string]string{
			"MongoStoreConnection": "mongodb://localhost:27017",
			"MongoStoreDatabase":   "fixstore",
			"MongoStoreReplicaSet": "rs0",
		} {
			v, err := qf.GlobalSettings().Setting(key)
			require.NoError(t, err)
			assert.Equal(t, want, v)
		}
	})
}

func TestAcquireEngineStoreConflict(t *testing.T) {
	installFakeBackend(t, &fakeBackend{})

	storeA := &natsStoreConfig{urls: "nats://localhost:4222", bucket: "a", replicas: 1}

	cfg := testConnConfig("storeconf")
	cfg.store = storeA
	h1, err := acquireEngine(cfg, testLogger(), &recordingSubscriber{})
	require.NoError(t, err)
	t.Cleanup(h1.release)

	// A different store config for the same name must fail.
	different := testConnConfig("storeconf")
	different.store = &natsStoreConfig{urls: "nats://localhost:4222", bucket: "b", replicas: 1}
	_, err = acquireEngine(different, testLogger(), &recordingSubscriber{})
	require.ErrorContains(t, err, "store")

	// Omitting the store inherits the first registrant's config.
	inherit := testConnConfig("storeconf")
	h2, err := acquireEngine(inherit, testLogger(), &recordingSubscriber{})
	require.NoError(t, err)
	h2.release()

	// Supplying a store where the engine has none must fail.
	cfgNoStore := testConnConfig("nostore")
	h3, err := acquireEngine(cfgNoStore, testLogger(), &recordingSubscriber{})
	require.NoError(t, err)
	t.Cleanup(h3.release)

	withStore := testConnConfig("nostore")
	withStore.store = storeA
	_, err = acquireEngine(withStore, testLogger(), &recordingSubscriber{})
	require.ErrorContains(t, err, "store")
}
