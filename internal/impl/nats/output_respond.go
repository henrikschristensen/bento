package nats

import (
	"context"
	"errors"
	"fmt"

	"github.com/nats-io/nats.go"

	"github.com/warpstreamlabs/bento/public/service"
)

// natsRespondMsgMetaKey is the metadata key under which the nats input stores
// the original *nats.Msg so that nats_respond can call Respond on it.
const natsRespondMsgMetaKey = "nats_msg"

func natsRespondOutputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Categories("Services").
		Summary("Reply to an incoming NATS request using the original message's reply subject.").
		Description(`
Sends the processed message as a direct reply to the NATS request that was
received by a ` + "`nats`" + ` input in the same pipeline.

The ` + "`nats`" + ` input stores a reference to the original ` + "`*nats.Msg`" + ` in the
` + "`nats_msg`" + ` metadata field. This output reads that reference, builds a reply
message from the current content and (optionally) headers, and calls
` + "`msg.RespondMsg()`" + `. After sending the reply it clears the reply subject on
the original message so that the input's acknowledgement function does not also
publish a ` + "`+ACK`" + ` to the same inbox, which would otherwise race with the
real reply.

If the incoming message had no reply subject (i.e. it was a fire-and-forget
publish rather than a request), this output is a no-op.
`).
		Field(service.NewInterpolatedStringMapField("headers").
			Description("Explicit message headers to add to the reply.").
			Default(map[string]any{}).
			Example(map[string]any{
				"Content-Type": "application/json",
			})).
		Field(service.NewMetadataFilterField("metadata").
			Description("Determine which (if any) metadata values should be forwarded as headers in the reply.").
			Optional())
}

func init() {
	err := service.RegisterOutput(
		"nats_respond", natsRespondOutputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Output, int, error) {
			w, err := newNATSRespondWriter(conf, mgr)
			if err != nil {
				return nil, 0, err
			}
			return w, 1, nil
		},
	)
	if err != nil {
		panic(err)
	}
}

type natsRespondWriter struct {
	headers    map[string]*service.InterpolatedString
	metaFilter *service.MetadataFilter
	log        *service.Logger
}

func newNATSRespondWriter(conf *service.ParsedConfig, mgr *service.Resources) (*natsRespondWriter, error) {
	w := &natsRespondWriter{log: mgr.Logger()}

	var err error
	if w.headers, err = conf.FieldInterpolatedStringMap("headers"); err != nil {
		return nil, err
	}
	if conf.Contains("metadata") {
		if w.metaFilter, err = conf.FieldMetadataFilter("metadata"); err != nil {
			return nil, err
		}
	}
	return w, nil
}

func (w *natsRespondWriter) Connect(_ context.Context) error { return nil }

func (w *natsRespondWriter) Write(_ context.Context, msg *service.Message) error {
	rawMsg, ok := msg.MetaGetMut(natsRespondMsgMetaKey)
	if !ok {
		w.log.Warnf("nats_respond: no %q metadata found; message was not received via nats input", natsRespondMsgMetaKey)
		return nil
	}

	natsMsg, ok := rawMsg.(*nats.Msg)
	if !ok {
		w.log.Errorf("nats_respond: %q metadata is not *nats.Msg", natsRespondMsgMetaKey)
		return nil
	}

	if natsMsg.Reply == "" {
		// Fire-and-forget publish — nothing to reply to.
		return nil
	}

	data, err := msg.AsBytes()
	if err != nil {
		return err
	}

	reply := nats.NewMsg("")
	reply.Data = data

	for k, v := range w.headers {
		headerStr, err := v.TryString(msg)
		if err != nil {
			return fmt.Errorf("header %v interpolation error: %w", k, err)
		}
		reply.Header.Add(k, headerStr)
	}
	_ = w.metaFilter.Walk(msg, func(key, value string) error {
		reply.Header.Add(key, value)
		return nil
	})

	if err := natsMsg.RespondMsg(reply); errors.Is(err, nats.ErrMsgNoReply) {
		return nil
	} else if err != nil {
		return err
	}

	// Clear the reply subject so that the nats input's ack function will not
	// also publish +ACK to the same inbox and race with our reply.
	natsMsg.Reply = ""
	return nil
}

func (w *natsRespondWriter) Close(_ context.Context) error { return nil }
