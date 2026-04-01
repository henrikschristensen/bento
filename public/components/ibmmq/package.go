//go:build x_bento_extra || x_ibmmq

package ibmmq

import (
	// Bring in the internal plugin definitions.
	_ "github.com/warpstreamlabs/bento/internal/impl/ibmmq"
)
