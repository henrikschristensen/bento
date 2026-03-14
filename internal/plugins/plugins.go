package plugins

import (
	"github.com/warpstreamlabs/bento/internal/plugins/bloblang"
	"github.com/warpstreamlabs/bento/internal/plugins/inputs"
	"github.com/warpstreamlabs/bento/internal/plugins/processors"
)

func RegisterPlugins() {
	bloblang.RegisterEBCDICTOUTF8()
	bloblang.RegisterNewNats2MxMsg()
	bloblang.RegisterCenterSliceSameMonth()
	bloblang.RegisterFindNearestDateFromSlice()
	bloblang.RegisterGetNumDaysInMonth()
	bloblang.RegisterTimeDiffDaysAbsolute()
	inputs.RegisterFsEventInput()
	processors.RegisterFileProcessor()
}
