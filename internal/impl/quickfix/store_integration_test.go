package quickfix

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/ory/dockertest/v3"
	goquickfix "github.com/quickfixgo/quickfix"
	qfddl "github.com/quickfixgo/quickfix/_sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/warpstreamlabs/bento/public/service/integration"
)

// testStoreRoundTrip exercises a persistent store factory: sequence numbers
// and a saved message must survive the store being closed and recreated.
func testStoreRoundTrip(t *testing.T, factory goquickfix.MessageStoreFactory) {
	t.Helper()

	msg := []byte("8=FIX.4.4\x019=5\x0135=0\x0110=163\x01")

	store, err := factory.Create(testEngineSessionID)
	require.NoError(t, err)
	require.NoError(t, store.Reset())
	require.NoError(t, store.SetNextSenderMsgSeqNum(42))
	require.NoError(t, store.SetNextTargetMsgSeqNum(43))
	require.NoError(t, store.SaveMessage(1, msg))
	require.NoError(t, store.Close())

	restored, err := factory.Create(testEngineSessionID)
	require.NoError(t, err)
	t.Cleanup(func() { _ = restored.Close() })

	assert.Equal(t, 42, restored.NextSenderMsgSeqNum())
	assert.Equal(t, 43, restored.NextTargetMsgSeqNum())

	msgs, err := restored.GetMessages(1, 1)
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	assert.Equal(t, msg, msgs[0])
}

func TestIntegrationSQLStorePostgres(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Skipf("Could not connect to docker: %s", err)
	}
	pool.MaxWait = 3 * time.Minute

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository:   "postgres",
		Tag:          "16-alpine",
		Env:          []string{"POSTGRES_PASSWORD=password", "POSTGRES_DB=fix"},
		ExposedPorts: []string{"5432/tcp"},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	dsn := fmt.Sprintf("postgres://postgres:password@localhost:%s/fix?sslmode=disable", resource.GetPort("5432/tcp"))

	var db *sql.DB
	require.NoError(t, pool.Retry(func() error {
		var err error
		if db, err = sql.Open("postgres", dsn); err != nil {
			return err
		}
		return db.Ping()
	}))
	t.Cleanup(func() { _ = db.Close() })

	// The user owns the schema; the test applies the DDL shipped with
	// quickfixgo the same way an operator would.
	for _, ddl := range []string{"postgresql/sessions_table.sql", "postgresql/messages_table.sql"} {
		b, err := qfddl.FS.ReadFile(ddl)
		require.NoError(t, err)
		_, err = db.Exec(string(b))
		require.NoError(t, err)
	}

	qf, err := goquickfix.ParseSettings(strings.NewReader(testEngineSettings))
	require.NoError(t, err)
	factory, err := (&sqlStoreConfig{driver: "postgres", dsn: dsn}).storeFactory(qf)
	require.NoError(t, err)

	testStoreRoundTrip(t, factory)
}

func TestIntegrationMongoStore(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Skipf("Could not connect to docker: %s", err)
	}
	pool.MaxWait = 3 * time.Minute

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository:   "mongo",
		Tag:          "7",
		ExposedPorts: []string{"27017/tcp"},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	url := fmt.Sprintf("mongodb://localhost:%s", resource.GetPort("27017/tcp"))

	require.NoError(t, pool.Retry(func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		client, err := mongo.Connect(ctx, options.Client().ApplyURI(url))
		if err != nil {
			return err
		}
		defer func() { _ = client.Disconnect(ctx) }()
		return client.Ping(ctx, nil)
	}))

	qf, err := goquickfix.ParseSettings(strings.NewReader(testEngineSettings))
	require.NoError(t, err)
	factory, err := (&mongoStoreConfig{url: url, database: "fixstore"}).storeFactory(qf)
	require.NoError(t, err)

	testStoreRoundTrip(t, factory)
}
