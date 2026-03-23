package custominputs

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/service"
)

func fseventInput(t testing.TB, confPattern string, args ...any) service.Input {
	confSpec := fsEventInputSpec()
	conf, err := confSpec.ParseYAML(fmt.Sprintf(confPattern, args...), nil)
	require.NoError(t, err)

	batchInput, err := fsEventWatcherFromParsed(conf, service.MockResources())
	require.NoError(t, err)
	return batchInput
}

func TestFSEventWriteDedup(t *testing.T) {
	tmpDir := t.TempDir()

	// Pre-create the file before connecting so the CREATE event is not captured.
	testFile := filepath.Join(tmpDir, "test.txt")
	require.NoError(t, os.WriteFile(testFile, []byte("initial"), 0644))

	const dedupTimeout = 200 * time.Millisecond
	input := fseventInput(t, `
paths:
  - %s
write_dedup_timeout: 200ms
`, tmpDir)

	ctx := context.Background()
	require.NoError(t, input.Connect(ctx))
	t.Cleanup(func() { _ = input.Close(context.Background()) })

	// Perform rapid writes, each well within the dedup window, so the timer
	// keeps getting reset and fires only once after the last write.
	const numWrites = 5
	for i := 0; i < numWrites; i++ {
		require.NoError(t, os.WriteFile(testFile, []byte(fmt.Sprintf("write %d", i)), 0644))
		time.Sleep(10 * time.Millisecond)
	}
	lastWriteAt := time.Now()

	// Collect all events: numWrites immediate events + 1 dedup event.
	readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	var msgs []*service.Message
	for len(msgs) < numWrites+1 {
		msg, ack, err := input.Read(readCtx)
		require.NoError(t, err)
		require.NoError(t, ack(ctx, nil))
		msgs = append(msgs, msg)
	}

	// The dedup timer fires once after write_dedup_timeout has elapsed since the
	// last write, so total elapsed time must be at least dedupTimeout.
	require.GreaterOrEqual(t, time.Since(lastWriteAt), dedupTimeout,
		"dedup event should not fire before write_dedup_timeout expires")

	// Verify no additional (spurious) events arrive within a short grace period,
	// confirming the dedup timer fired exactly once.
	noMoreCtx, noMoreCancel := context.WithTimeout(ctx, 150*time.Millisecond)
	defer noMoreCancel()
	_, _, err := input.Read(noMoreCtx)
	require.ErrorIs(t, err, context.DeadlineExceeded,
		"no extra events should be emitted after the single dedup event")
}
