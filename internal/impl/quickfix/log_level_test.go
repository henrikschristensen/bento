package quickfix

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	goquickfix "github.com/quickfixgo/quickfix"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/service"
)

// syncBuffer is a goroutine-safe writer; the QuickFIX engine logs from multiple
// internal goroutines, so the capture sink must be synchronised.
type syncBuffer struct {
	mu  sync.Mutex
	buf strings.Builder
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// resourcesAtLevel builds a bento Resources whose logger writes to buf at the
// given slog level, mirroring how BENTO_LOG_LEVEL controls the real deployment.
func resourcesAtLevel(buf *syncBuffer, level slog.Level) *service.Resources {
	slogger := slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: level}))
	return service.MockResources(service.MockResourcesOptUseSlogger(slogger))
}

// runFIXExchange stands up an acceptor input and initiator output using the
// provided resources, waits for logon, sends one application message and reads
// it back. It returns after the round trip so the caller can inspect the logs.
func runFIXExchange(t *testing.T, res *service.Resources) {
	t.Helper()

	port := freePort(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	inputConf, err := quickfixInputSpec().ParseYAML(fmt.Sprintf(`
connection_type: acceptor
settings_file: %q
`, writeTempCfg(t, acceptorCfg(port))), nil)
	require.NoError(t, err)

	inp, err := newQuickfixInputFromParsed(inputConf, res)
	require.NoError(t, err)
	require.NoError(t, inp.Connect(ctx))
	t.Cleanup(func() { _ = inp.Close(ctx) })

	outputConf, err := quickfixOutputSpec().ParseYAML(fmt.Sprintf(`
connection_type: initiator
settings_file: %q
`, writeTempCfg(t, initiatorCfg(port))), nil)
	require.NoError(t, err)

	out, err := newQuickfixOutputFromParsed(outputConf, res)
	require.NoError(t, err)
	require.NoError(t, out.Connect(ctx))
	t.Cleanup(func() { _ = out.Close(ctx) })

	require.Eventually(t, func() bool {
		return hasLoggedOn(out)
	}, 15*time.Second, 100*time.Millisecond, "timed out waiting for FIX session logon")

	rawFIX := buildRawFIX("FIX.4.4", "D", "CLIENT", "SERVER", map[goquickfix.Tag]string{
		goquickfix.Tag(11): "ORDER001",
	})
	require.NoError(t, out.Write(ctx, service.NewMessage(rawFIX)))

	readCtx, readCancel := context.WithTimeout(ctx, 10*time.Second)
	defer readCancel()

	msg, ackFn, err := inp.Read(readCtx)
	require.NoError(t, err)
	require.NoError(t, ackFn(ctx, nil))
	_, err = msg.AsBytes()
	require.NoError(t, err)
}

// TestQuickfixDebugLoggingSurfacesMessages proves that per-message FIX traffic
// is emitted at DEBUG level (the behaviour BENTO_LOG_LEVEL=DEBUG unlocks) and is
// suppressed at INFO, while session events remain visible at INFO.
func TestQuickfixDebugLoggingSurfacesMessages(t *testing.T) {
	t.Run("debug surfaces incoming and outgoing FIX", func(t *testing.T) {
		buf := &syncBuffer{}
		runFIXExchange(t, resourcesAtLevel(buf, slog.LevelDebug))

		logs := buf.String()
		assert.Contains(t, logs, "FIX outgoing:", "expected outgoing FIX message log at DEBUG")
		assert.Contains(t, logs, "FIX incoming:", "expected incoming FIX message log at DEBUG")
		// The application New Order Single should appear on the wire log.
		assert.Contains(t, logs, "11=ORDER001", "expected application message body in DEBUG log")
	})

	t.Run("info suppresses per-message FIX but keeps events", func(t *testing.T) {
		buf := &syncBuffer{}
		runFIXExchange(t, resourcesAtLevel(buf, slog.LevelInfo))

		logs := buf.String()
		assert.NotContains(t, logs, "FIX outgoing:", "per-message FIX log must be suppressed at INFO")
		assert.NotContains(t, logs, "FIX incoming:", "per-message FIX log must be suppressed at INFO")
		// Session lifecycle events are logged at INFO, so logon should show.
		assert.Contains(t, strings.ToLower(logs), "logon", "expected session logon event at INFO")
	})
}
