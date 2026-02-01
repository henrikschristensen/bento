package io_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/internal/component/input"
	"github.com/warpstreamlabs/bento/internal/component/testutil"
	"github.com/warpstreamlabs/bento/internal/manager/mock"

	_ "github.com/warpstreamlabs/bento/internal/impl/io"
)

func fseventInput(t testing.TB, confPattern string, args ...any) input.Streamed {
	iConf, err := testutil.InputFromYAML(fmt.Sprintf(confPattern, args...))
	require.NoError(t, err)

	i, err := mock.NewManager().NewInput(iConf)
	require.NoError(t, err)

	return i
}

func TestFSEventBasic(t *testing.T) {
	dir := t.TempDir()
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	testFile := filepath.Join(dir, "test.txt")
	require.NoError(t, os.WriteFile(testFile, []byte("initial content"), 0o644))

	i := fseventInput(t, `
fsevent:
  paths: [ "%v" ]
`, testFile)

	// Wait for the input to connect and start watching
	time.Sleep(time.Second)

	err := os.WriteFile(testFile, []byte("modified content"), 0o644)
	require.NoError(t, err)

	select {
	case tran := <-i.TransactionChan():
		require.NoError(t, tran.Ack(ctx, nil))
		msg := tran.Payload
		assert.Equal(t, 1, msg.Len())

		// Check metadata fields
		part := msg.Get(0)
		assert.Equal(t, testFile, part.MetaGetStr("fsevent_path"))
		assert.NotEmpty(t, part.MetaGetStr("fsevent_operation"))
		assert.NotEmpty(t, part.MetaGetStr("fsevent_mod_time_unix"))
		assert.NotEmpty(t, part.MetaGetStr("fsevent_mod_time"))

		operation := part.MetaGetStr("fsevent_operation")
		assert.Contains(t, operation, "WRITE")

	case <-time.After(time.Second * 5):
		t.Fatal("timed out waiting for filesystem event")
	}
}

func TestFSEventCreateFile(t *testing.T) {
	dir := t.TempDir()
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	i := fseventInput(t, `
fsevent:
  paths: [ "%v" ]
`, dir)

	// Wait for the input to connect and start watching
	time.Sleep(time.Second)

	newFile := filepath.Join(dir, "newfile.txt")
	err := os.WriteFile(newFile, []byte("new file content"), 0o644)
	require.NoError(t, err)

	select {
	case tran := <-i.TransactionChan():
		require.NoError(t, tran.Ack(ctx, nil))
		msg := tran.Payload
		assert.Equal(t, 1, msg.Len())

		part := msg.Get(0)
		assert.Equal(t, newFile, part.MetaGetStr("fsevent_path"))

		operation := part.MetaGetStr("fsevent_operation")
		assert.Contains(t, operation, "CREATE")

	case <-time.After(time.Second * 5):
		t.Fatal("timed out waiting for create event")
	}
}

func TestFSEventDeleteFile(t *testing.T) {
	dir := t.TempDir()
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	testFile := filepath.Join(dir, "test.txt")
	require.NoError(t, os.WriteFile(testFile, []byte("content"), 0o644))

	i := fseventInput(t, `
fsevent:
  paths: [ "%v" ]
`, testFile)

	// Wait for the input to connect and start watching
	time.Sleep(time.Second)

	err := os.Remove(testFile)
	require.NoError(t, err)

	select {
	case tran := <-i.TransactionChan():
		require.NoError(t, tran.Ack(ctx, nil))
		msg := tran.Payload
		assert.Equal(t, 1, msg.Len())

		part := msg.Get(0)
		assert.Equal(t, testFile, part.MetaGetStr("fsevent_path"))

		// The operation should be REMOVE or CHMOD (some filesystems send CHMOD before REMOVE)
		operation := part.MetaGetStr("fsevent_operation")
		assert.Contains(t, operation, "CHMOD", "Expected CHMOD operation, got: %s", operation)

	case <-time.After(time.Second * 5):
		t.Fatal("timed out waiting for delete event")
	}
}

func TestFSEventMultipleDirs(t *testing.T) {
	dir1 := t.TempDir()
	dir2 := t.TempDir()
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	// Watch both directories
	i := fseventInput(t, `
fsevent:
  paths: [ "%v", "%v" ]
`, dir1, dir2)

	// Give the watcher a moment to start up
	time.Sleep(time.Second)

	file1 := filepath.Join(dir1, "file1.txt")
	file2 := filepath.Join(dir2, "file2.txt")

	require.NoError(t, os.WriteFile(file1, []byte("content1"), 0o644))

	var dir1Event, dir2Event bool
	for j := 0; j < 10; j++ {
		select {
		case tran := <-i.TransactionChan():
			require.NoError(t, tran.Ack(ctx, nil))
			msg := tran.Payload
			assert.Equal(t, 1, msg.Len())

			part := msg.Get(0)
			eventPath := part.MetaGetStr("fsevent_path")
			operation := part.MetaGetStr("fsevent_operation")
			assert.True(t, operation == "WRITE" || operation == "CREATE", "Expected WRITE or CREATE operation, got: %s", operation)

			switch eventPath {
			case file1:
				dir1Event = true
			case file2:
				dir2Event = true
			}

			if dir1Event && dir2Event {
				break
			}

		case <-time.After(time.Second * 1):
			continue
		}
	}

	require.NoError(t, os.WriteFile(file2, []byte("content2"), 0o644))

	for j := 0; j < 10; j++ {
		select {
		case tran := <-i.TransactionChan():
			require.NoError(t, tran.Ack(ctx, nil))
			msg := tran.Payload
			assert.Equal(t, 1, msg.Len())

			part := msg.Get(0)
			eventPath := part.MetaGetStr("fsevent_path")
			operation := part.MetaGetStr("fsevent_operation")
			assert.True(t, operation == "WRITE" || operation == "CREATE", "Expected WRITE or CREATE operation, got: %s", operation)

			switch eventPath {
			case file1:
				dir1Event = true
			case file2:
				dir2Event = true
			}

			if dir1Event && dir2Event {
				break
			}

		case <-time.After(time.Second * 1):
			continue
		}
	}

	assert.True(t, dir1Event, "Should have received event from dir1")
	assert.True(t, dir2Event, "Should have received event from dir2")
}
