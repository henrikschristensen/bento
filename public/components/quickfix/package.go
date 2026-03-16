// Package quickfix contains components for sending and receiving FIX protocol
// messages using the QuickFIX/Go engine. Both acceptor (server) and initiator
// (client) connection modes are supported. Messages are treated as raw FIX
// strings without any version-specific processing.
package quickfix

import (
	// Import the quickfix implementations.
	_ "github.com/warpstreamlabs/bento/internal/impl/quickfix"
)
