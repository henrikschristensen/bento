//go:build !(x_bento_extra || x_ibmmq)

package bloblang

// RegisterNewNats2MxMsg is a no-op when built without x_bento_extra or x_ibmmq tags.
func RegisterNewNats2MxMsg() {}

func RegisterFromNats2MxMsg() {}
