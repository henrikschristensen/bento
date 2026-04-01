//go:build x_bento_extra || x_ibmmq

package bloblang

import (
	"github.com/ibm-messaging/mq-golang/v5/ibmmq"
	"github.com/nats-io/nats-mq/message"
	"github.com/warpstreamlabs/bento/public/bloblang"
)

func newNats2MxMsg(s string) ([]byte, error) {
	msg := message.NewBridgeMessage([]byte(s))
	msg.Header.MsgType = ibmmq.MQMT_DATAGRAM
	msg.Header.Format = ibmmq.MQFMT_STRING
	return msg.Encode()
}

func RegisterNewNats2MxMsg() {
	pspec := bloblang.NewPluginSpec().
		Description("Create message for the nats_mq bridge")

	bloblang.RegisterMethodV2("new_nats2mx_msg", pspec, func(args *bloblang.ParsedParams) (bloblang.Method, error) {
		return bloblang.StringMethod(func(s string) (any, error) {
			msg, err := newNats2MxMsg(s)
			if err != nil {
				return nil, err
			}
			return msg, nil
		}), nil
	})
}

func RegisterFromNats2MxMsg() {
	pspec := bloblang.NewPluginSpec().
		Description("Decode a nats_mq bridge message and return the raw body as a string")

	bloblang.RegisterFunctionV2("from_nats2mx_msg", pspec, func(args *bloblang.ParsedParams) (bloblang.Function, error) {
		return nil, nil
	})

	bloblang.RegisterMethodV2("from_nats2mx_msg", pspec, func(args *bloblang.ParsedParams) (bloblang.Method, error) {
		return bloblang.BytesMethod(func(b []byte) (any, error) {
			msg, err := message.DecodeBridgeMessage(b)
			if err != nil {
				return nil, err
			}
			return string(msg.Body), nil
		}), nil
	})
}
