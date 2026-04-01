package teams

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/service"
)

// newTestServer starts an httptest.Server that records the last received
// MessageCard payload and always returns HTTP 200.
func newTestServer(t *testing.T) (*httptest.Server, *map[string]any) {
	t.Helper()

	received := &map[string]any{}

	svr := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			http.Error(rw, "bad json", http.StatusBadRequest)
			return
		}
		*received = payload
		rw.WriteHeader(http.StatusOK)
		_, _ = rw.Write([]byte("1"))
	}))

	t.Cleanup(svr.Close)
	return svr, received
}

func buildWriter(t *testing.T, svr *httptest.Server, extraYAML string) *teamsWebhookWriter {
	t.Helper()

	conf := fmt.Sprintf("webhook_url: %s\n%s", svr.URL, extraYAML)
	pconf, err := teamsWebhookOutputSpec().ParseYAML(conf, nil)
	require.NoError(t, err)

	w, err := newTeamsWebhookWriter(pconf, service.MockResources())
	require.NoError(t, err)

	// Skip Microsoft-specific URL validation so tests can use httptest servers.
	w.skipURLValidation = true
	return w
}

func TestTeamsWebhookLifecycle(t *testing.T) {
	svr, _ := newTestServer(t)
	w := buildWriter(t, svr, "")

	// Write before Connect should return ErrNotConnected.
	msg := service.NewMessage([]byte("hello"))
	require.ErrorIs(t, w.Write(context.Background(), msg), service.ErrNotConnected)

	// Close before Connect should be a no-op.
	require.NoError(t, w.Close(context.Background()))

	// Connect is idempotent.
	require.NoError(t, w.Connect(context.Background()))
	require.NoError(t, w.Connect(context.Background()))

	// Write after Close should error.
	require.NoError(t, w.Close(context.Background()))
	require.ErrorIs(t, w.Write(context.Background(), msg), service.ErrNotConnected)

	// Reconnect and happy path.
	require.NoError(t, w.Connect(context.Background()))
	require.NoError(t, w.Write(context.Background(), msg))
	require.NoError(t, w.Close(context.Background()))

	// Close is idempotent.
	require.NoError(t, w.Close(context.Background()))
}

func TestTeamsWebhookPlainText(t *testing.T) {
	svr, received := newTestServer(t)
	w := buildWriter(t, svr, "")

	require.NoError(t, w.Connect(context.Background()))

	msg := service.NewMessage([]byte("plain text message"))
	require.NoError(t, w.Write(context.Background(), msg))

	assert.Equal(t, "plain text message", (*received)["text"])
}

func TestTeamsWebhookJSONPassthrough(t *testing.T) {
	svr, received := newTestServer(t)
	w := buildWriter(t, svr, "")

	require.NoError(t, w.Connect(context.Background()))

	payload := `{"title":"Alert","text":"Something happened","themeColor":"FF0000"}`
	msg := service.NewMessage([]byte(payload))
	require.NoError(t, w.Write(context.Background(), msg))

	assert.Equal(t, "Alert", (*received)["title"])
	assert.Equal(t, "Something happened", (*received)["text"])
	assert.Equal(t, "FF0000", (*received)["themeColor"])
}

func TestTeamsWebhookTitleOverride(t *testing.T) {
	svr, received := newTestServer(t)
	w := buildWriter(t, svr, "title: My Override Title\n")

	require.NoError(t, w.Connect(context.Background()))

	payload := `{"title":"Original Title","text":"body"}`
	msg := service.NewMessage([]byte(payload))
	require.NoError(t, w.Write(context.Background(), msg))

	assert.Equal(t, "My Override Title", (*received)["title"])
	assert.Equal(t, "body", (*received)["text"])
}

func TestTeamsWebhookThemeColorOverride(t *testing.T) {
	svr, received := newTestServer(t)
	w := buildWriter(t, svr, "theme_color: \"00FF00\"\n")

	require.NoError(t, w.Connect(context.Background()))

	msg := service.NewMessage([]byte("green message"))
	require.NoError(t, w.Write(context.Background(), msg))

	assert.Equal(t, "00FF00", (*received)["themeColor"])
}
