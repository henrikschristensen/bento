package quickfix

import (
	"crypto/tls"
	"fmt"
	"strings"
	"time"

	goquickfix "github.com/quickfixgo/quickfix"
	"github.com/quickfixgo/quickfix/config"
	filestore "github.com/quickfixgo/quickfix/store/file"
	mongostore "github.com/quickfixgo/quickfix/store/mongo"
	sqlstore "github.com/quickfixgo/quickfix/store/sql"

	natsstore "github.com/warpstreamlabs/bento/internal/impl/quickfix/store/nats"
	"github.com/warpstreamlabs/bento/public/service"
)

const fieldStore = "store"

// fixStore is the seam between a parsed `store` config and a shared FIX
// engine. equal reports whether two configs describe the same backend with
// identical settings, for shared-engine conflict checks. storeFactory injects
// the backend's FIX settings keys into qfSettings (global scope, which
// quickfixgo overlays onto every session) and returns the corresponding
// quickfixgo store factory.
type fixStore interface {
	equal(fixStore) bool
	storeFactory(qfSettings *goquickfix.Settings) (goquickfix.MessageStoreFactory, error)
}

// sqlStoreDrivers mirrors the driver enum of bento's sql components, so a FIX
// session store and a sql input/output in the same pipeline name their
// database the same way. The value is passed straight to sql.Open by
// quickfixgo's sql store.
var sqlStoreDrivers = []string{"mysql", "postgres", "clickhouse", "mssql", "sqlite", "oracle", "snowflake", "trino", "gocosmos", "spanner", "duckdb"}

// storeSpecField returns the optional `store` field shared by the quickfix
// input and output specs. Exactly one backend may be configured; when none is
// the FIX message store is kept in memory only.
func storeSpecField() *service.ConfigField {
	return service.NewObjectField(fieldStore,
		service.NewObjectField("file",
			service.NewStringField("path").
				Description("The directory path in which session state and message files are written. quickfixgo creates per-session subdirectories under this path, so several engines may share it.").
				Example("/var/lib/bento/fix"),
			service.NewBoolField("sync").
				Description("Whether file writes are fsynced before returning. Disable only where losing the last write on crash is acceptable.").
				Default(true),
		).
			Description("Stores FIX session state and messages on the local filesystem so they survive process restarts.").
			Optional(),
		service.NewObjectField("sql",
			service.NewStringEnumField("driver", sqlStoreDrivers...).
				Description("A database driver to use, named the same way as bento's `sql` components."),
			service.NewStringField("dsn").
				Description("The data source name passed to the driver.").
				Example("postgres://user:pass@localhost:5432/fix?sslmode=disable"),
			service.NewStringField("sessions_table").
				Description("An optional override for the name of the table holding session state.").
				Default(""),
			service.NewStringField("messages_table").
				Description("An optional override for the name of the table holding sent messages.").
				Default(""),
		).
			Description("Stores FIX session state and messages in a SQL database so they survive process restarts. The tables must already exist; create them with the DDL scripts shipped with quickfixgo (`_sql/<dialect>` in the module source). Note that quickfixgo's sql store uses `?` bind placeholders for every driver except `postgres`, so drivers requiring other placeholder styles (e.g. `mssql`, `oracle`) will fail at runtime.").
			Optional(),
		service.NewObjectField("mongo",
			service.NewStringField("url").
				Description("The MongoDB connection URL, including any credentials.").
				Example("mongodb://user:pass@localhost:27017"),
			service.NewStringField("database").
				Description("The name of the target MongoDB database.").
				Default(""),
			service.NewStringField("replica_set").
				Description("An optional replica set name to connect to.").
				Default(""),
		).
			Description("Stores FIX session state and messages in MongoDB so they survive process restarts.").
			Optional(),
		service.NewObjectField("nats",
			service.NewStringListField("urls").
				Description("A list of NATS server URLs to connect to. If an item of the list contains commas it will be expanded into multiple URLs.").
				Example([]string{"nats://127.0.0.1:4222"}),
			service.NewStringField("bucket").
				Description("The JetStream Object Store bucket used for FIX message storage.").
				Default(natsstore.DefaultBucket),
			service.NewStringField("credentials_file").
				Description("An optional path to a NATS credentials (.creds) file used to authenticate.").
				Default(""),
			service.NewStringField("token").
				Description("An optional auth token used to authenticate.").
				Default(""),
			service.NewIntField("replicas").
				Description("The number of JetStream replicas kept for the Object Store bucket.").
				Default(1),
			service.NewDurationField("compaction_interval").
				Description("The interval at which Object Store objects are compacted. Set to `0s` to disable background compaction.").
				Default("10s"),
			service.NewTLSToggledField("tls"),
		).
			Description("Stores FIX session state and messages in a NATS JetStream Object Store so they survive process restarts.").
			Optional(),
	).
		Description("Configures the FIX message store. Exactly one backend may be configured; when omitted, session state is kept in memory only.").
		Optional()
}

// parseStoreConfig reads the optional `store` field. It returns nil when no
// backend is configured. Because spec defaults materialise nested objects even
// when the user omitted them, each backend's presence is judged by a field
// that has no default: nats by its urls list, file by path, sql by dsn, mongo
// by url.
func parseStoreConfig(pConf *service.ParsedConfig) (fixStore, error) {
	var names []string
	var stores []fixStore

	ns := pConf.Namespace(fieldStore)

	nats, err := parseNatsStoreConfig(pConf)
	if err != nil {
		return nil, err
	}
	if nats != nil {
		names = append(names, "nats")
		stores = append(stores, nats)
	}

	if ns.Contains("file") {
		f, err := parseFileStoreConfig(ns.Namespace("file"))
		if err != nil {
			return nil, err
		}
		if f != nil {
			names = append(names, "file")
			stores = append(stores, f)
		}
	}

	if ns.Contains("sql") {
		s, err := parseSQLStoreConfig(ns.Namespace("sql"))
		if err != nil {
			return nil, err
		}
		if s != nil {
			names = append(names, "sql")
			stores = append(stores, s)
		}
	}

	if ns.Contains("mongo") {
		m, err := parseMongoStoreConfig(ns.Namespace("mongo"))
		if err != nil {
			return nil, err
		}
		if m != nil {
			names = append(names, "mongo")
			stores = append(stores, m)
		}
	}

	if len(stores) > 1 {
		return nil, fmt.Errorf("only one `%s` backend may be configured, found: %s", fieldStore, strings.Join(names, ", "))
	}
	if len(stores) == 0 {
		return nil, nil
	}
	return stores[0], nil
}

//------------------------------------------------------------------------------
// file

// fileStoreConfig holds the parsed `store.file` configuration.
type fileStoreConfig struct {
	path string
	sync bool
}

func parseFileStoreConfig(ns *service.ParsedConfig) (*fileStoreConfig, error) {
	c := &fileStoreConfig{}
	var err error
	if c.path, err = ns.FieldString("path"); err != nil {
		return nil, err
	}
	if c.path == "" {
		return nil, nil
	}
	if c.sync, err = ns.FieldBool("sync"); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *fileStoreConfig) equal(o fixStore) bool {
	oc, ok := o.(*fileStoreConfig)
	return ok && *c == *oc
}

func (c *fileStoreConfig) storeFactory(qfSettings *goquickfix.Settings) (goquickfix.MessageStoreFactory, error) {
	g := qfSettings.GlobalSettings()
	g.Set(config.FileStorePath, c.path)
	g.Set(config.FileStoreSync, fixBool(c.sync))
	return filestore.NewStoreFactory(qfSettings), nil
}

// fixBool renders a bool the way FIX settings files express them.
func fixBool(b bool) string {
	if b {
		return "Y"
	}
	return "N"
}

//------------------------------------------------------------------------------
// sql

// sqlStoreConfig holds the parsed `store.sql` configuration. Empty table
// names defer to quickfixgo's own defaults (sessions, messages).
type sqlStoreConfig struct {
	driver        string
	dsn           string
	sessionsTable string
	messagesTable string
}

func parseSQLStoreConfig(ns *service.ParsedConfig) (*sqlStoreConfig, error) {
	c := &sqlStoreConfig{}
	var err error
	if c.driver, err = ns.FieldString("driver"); err != nil {
		return nil, err
	}
	if c.dsn, err = ns.FieldString("dsn"); err != nil {
		return nil, err
	}
	if c.dsn == "" {
		return nil, nil
	}
	if c.sessionsTable, err = ns.FieldString("sessions_table"); err != nil {
		return nil, err
	}
	if c.messagesTable, err = ns.FieldString("messages_table"); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *sqlStoreConfig) equal(o fixStore) bool {
	oc, ok := o.(*sqlStoreConfig)
	return ok && *c == *oc
}

func (c *sqlStoreConfig) storeFactory(qfSettings *goquickfix.Settings) (goquickfix.MessageStoreFactory, error) {
	g := qfSettings.GlobalSettings()
	g.Set(config.SQLStoreDriver, c.driver)
	g.Set(config.SQLStoreDataSourceName, c.dsn)
	if c.sessionsTable != "" {
		g.Set(config.SQLStoreSessionsTableName, c.sessionsTable)
	}
	if c.messagesTable != "" {
		g.Set(config.SQLStoreMessagesTableName, c.messagesTable)
	}
	return sqlstore.NewStoreFactory(qfSettings), nil
}

//------------------------------------------------------------------------------
// mongo

// mongoStoreConfig holds the parsed `store.mongo` configuration.
type mongoStoreConfig struct {
	url        string
	database   string
	replicaSet string
}

func parseMongoStoreConfig(ns *service.ParsedConfig) (*mongoStoreConfig, error) {
	c := &mongoStoreConfig{}
	var err error
	if c.url, err = ns.FieldString("url"); err != nil {
		return nil, err
	}
	if c.url == "" {
		return nil, nil
	}
	if c.database, err = ns.FieldString("database"); err != nil {
		return nil, err
	}
	if c.database == "" {
		return nil, fmt.Errorf("`%s.mongo.database` must be set when `%s.mongo.url` is", fieldStore, fieldStore)
	}
	if c.replicaSet, err = ns.FieldString("replica_set"); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *mongoStoreConfig) equal(o fixStore) bool {
	oc, ok := o.(*mongoStoreConfig)
	return ok && *c == *oc
}

func (c *mongoStoreConfig) storeFactory(qfSettings *goquickfix.Settings) (goquickfix.MessageStoreFactory, error) {
	g := qfSettings.GlobalSettings()
	g.Set(config.MongoStoreConnection, c.url)
	g.Set(config.MongoStoreDatabase, c.database)
	if c.replicaSet != "" {
		g.Set(config.MongoStoreReplicaSet, c.replicaSet)
	}
	return mongostore.NewStoreFactory(qfSettings), nil
}

//------------------------------------------------------------------------------
// nats

// natsStoreConfig holds the parsed `store.nats` configuration. tlsConf is the
// built TLS config used when connecting; the remaining fields double as the
// identity of the config for shared-engine conflict checks.
type natsStoreConfig struct {
	urls               string
	bucket             string
	credentialsFile    string
	token              string
	replicas           int
	compactionInterval time.Duration
	tlsEnabled         bool
	tlsServerName      string
	tlsInsecureSkip    bool
	tlsConf            *tls.Config
}

// parseNatsStoreConfig reads the optional `store.nats` field. It returns nil
// when no NATS store is configured, judged by the urls list: a NATS store
// without URLs is not configured.
func parseNatsStoreConfig(pConf *service.ParsedConfig) (*natsStoreConfig, error) {
	if !pConf.Contains(fieldStore, "nats") {
		return nil, nil
	}
	ns := pConf.Namespace(fieldStore).Namespace("nats")

	c := &natsStoreConfig{}
	var err error
	var urlList []string
	if urlList, err = ns.FieldStringList("urls"); err != nil {
		return nil, err
	}
	if len(urlList) == 0 {
		return nil, nil
	}
	c.urls = strings.Join(urlList, ",")
	if c.bucket, err = ns.FieldString("bucket"); err != nil {
		return nil, err
	}
	if c.credentialsFile, err = ns.FieldString("credentials_file"); err != nil {
		return nil, err
	}
	if c.token, err = ns.FieldString("token"); err != nil {
		return nil, err
	}
	if c.replicas, err = ns.FieldInt("replicas"); err != nil {
		return nil, err
	}
	if c.replicas < 1 {
		return nil, fmt.Errorf("`%s.nats.replicas` must be a positive integer, got %d", fieldStore, c.replicas)
	}
	if c.compactionInterval, err = ns.FieldDuration("compaction_interval"); err != nil {
		return nil, err
	}
	if c.tlsConf, c.tlsEnabled, err = ns.FieldTLSToggled("tls"); err != nil {
		return nil, err
	}
	if c.tlsEnabled {
		c.tlsServerName = c.tlsConf.ServerName
		c.tlsInsecureSkip = c.tlsConf.InsecureSkipVerify
	}
	return c, nil
}

// equal reports whether two store configs are identical, for shared-engine
// conflict checks.
func (c *natsStoreConfig) equal(o fixStore) bool {
	on, ok := o.(*natsStoreConfig)
	if !ok {
		return false
	}
	return c.urls == on.urls &&
		c.bucket == on.bucket &&
		c.credentialsFile == on.credentialsFile &&
		c.token == on.token &&
		c.replicas == on.replicas &&
		c.compactionInterval == on.compactionInterval &&
		c.tlsEnabled == on.tlsEnabled &&
		c.tlsServerName == on.tlsServerName &&
		c.tlsInsecureSkip == on.tlsInsecureSkip
}

// storeFactory builds the NATS-backed FIX message store factory.
func (c *natsStoreConfig) storeFactory(*goquickfix.Settings) (goquickfix.MessageStoreFactory, error) {
	return natsstore.NewStoreFactory(natsstore.Config{
		URLs:               c.urls,
		CredentialsFile:    c.credentialsFile,
		Token:              c.token,
		TLSConfig:          c.tlsConf,
		Bucket:             c.bucket,
		CompactionInterval: c.compactionInterval,
		Replicas:           c.replicas,
	})
}

//------------------------------------------------------------------------------
// settings-key rejection

// rejectedStoreKeys maps FIX settings keys that configure quickfixgo's own
// message stores (or the legacy NatsStore* keys from before the `store.nats`
// field existed) to the bento `store` field that replaces them. They are
// rejected outright: the engine always chooses its store from bento config,
// so these keys would otherwise be silently ignored.
var rejectedStoreKeys = []struct {
	key   string
	field string
}{
	{"NatsStoreURL", "store.nats"},
	{"NatsStoreBucket", "store.nats"},
	{"NatsStoreCredentials", "store.nats"},
	{"NatsStoreToken", "store.nats"},
	{"NatsStoreCompactionInterval", "store.nats"},
	{"NatsStoreReplicas", "store.nats"},
	{"FileStorePath", "store.file"},
	{"FileStoreSync", "store.file"},
	{"SQLStoreDriver", "store.sql"},
	{"SQLStoreDataSourceName", "store.sql"},
	{"SQLStoreConnMaxLifetime", "store.sql"},
	{"SQLStoreMessagesTableName", "store.sql"},
	{"SQLStoreSessionsTableName", "store.sql"},
	{"MongoStoreConnection", "store.mongo"},
	{"MongoStoreDatabase", "store.mongo"},
	{"MongoStoreReplicaSet", "store.mongo"},
}

// rejectStoreSettingsKeys fails when any store-related FIX settings key is
// present in the parsed FIX settings, directing the user at the `store` field.
func rejectStoreSettingsKeys(qfSettings *goquickfix.Settings) error {
	for _, r := range rejectedStoreKeys {
		if hasSetting(qfSettings, r.key) {
			return fmt.Errorf("FIX settings key `%s` is not supported; configure the FIX message store with the `%s` field instead", r.key, r.field)
		}
	}
	return nil
}

// hasSetting reports whether key is set (non-empty) in the global defaults or
// any session of the parsed FIX settings.
func hasSetting(qfSettings *goquickfix.Settings, key string) bool {
	if v, err := qfSettings.GlobalSettings().Setting(key); err == nil && v != "" {
		return true
	}
	for _, s := range qfSettings.SessionSettings() {
		if v, err := s.Setting(key); err == nil && v != "" {
			return true
		}
	}
	return false
}
