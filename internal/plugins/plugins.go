package plugins

import (
	"github.com/warpstreamlabs/bento/internal/plugins/bloblang"
)

func RegisterPlugins() {
	bloblang.RegisterEBCDICTOUTF8()
	bloblang.RegisterNewNats2MxMsg()
	bloblang.RegisterFromNats2MxMsg()
	bloblang.RegisterCenterSliceSameMonth()
	bloblang.RegisterFindNearestDateFromSlice()
	bloblang.RegisterGetNumDaysInMonth()
	bloblang.RegisterTimeDiffDaysAbsolute()
	bloblang.RegisterRounding()
}
