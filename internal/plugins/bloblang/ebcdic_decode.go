package bloblang

import (
	"github.com/warpstreamlabs/bento/public/bloblang"
	"golang.org/x/text/encoding/charmap"
)

func ebcdic_to_utf8(s []byte) ([]byte, error) {
	decoder := charmap.CodePage1047.NewDecoder()
	utf8Bytes, err := decoder.Bytes(s)
	if err != nil {
		return nil, err
	}
	return utf8Bytes, nil
}

func RegisterEBCDICTOUTF8() {
	pspec := bloblang.NewPluginSpec().
		Description("Convert from EBCDIC (IBM-785/CodePage1047) to UTF-8")

	bloblang.RegisterMethodV2("ebcdic_to_utf8", pspec, func(args *bloblang.ParsedParams) (bloblang.Method, error) {
		return bloblang.BytesMethod(func(s []byte) (any, error) {
			utf8bytes, err := ebcdic_to_utf8(s)
			if err != nil {
				return nil, err
			}
			return utf8bytes, nil
		}), nil
	})
}
