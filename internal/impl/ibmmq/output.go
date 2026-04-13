//go:build x_bento_extra || x_ibmmq

package ibmmq

import (
	"context"
	"fmt"
	"sync"

	"github.com/ibm-messaging/mq-golang/v5/ibmmq"

	"github.com/warpstreamlabs/bento/public/service"
)

const (
	ioFieldQueueManager = "queue_manager"
	ioFieldQueue        = "queue"
	ioFieldChannel      = "channel"
	ioFieldConnName     = "connection_name"
	ioFieldUsername     = "username"
	ioFieldPassword     = "password"
	ioFieldPutOptions   = "put_options"
	ioFieldOpenOptions  = "open_options"
	ioFieldConnOptions  = "connection_options"
)

func ibmMQOutputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("Services").
		Stable().
		Summary("Sends messages to an IBM MQ queue.").
		Description(`Connects to an IBM MQ queue manager and publishes messages to the specified queue.

The `+"`queue`"+` field can be dynamically set using function interpolations described [here](/docs/configuration/interpolation#bloblang-queries).

### Metadata

MQMD fields can be set per-message by adding `+"`ibmmq_*`"+` metadata to each
message before it reaches this output. When a metadata key is present it
overrides the corresponding config default. Writable fields are:

`+"```text"+`
- ibmmq_report
- ibmmq_feedback
- ibmmq_encoding
- ibmmq_coded_char_set_id
- ibmmq_format
- ibmmq_priority
- ibmmq_msg_id        (hex; only effective without MQPMO_NEW_MSG_ID in put_options)
- ibmmq_correl_id     (hex; only effective without MQPMO_NEW_CORREL_ID in put_options)
- ibmmq_reply_to_q
- ibmmq_reply_to_qmgr
- ibmmq_user_identifier
- ibmmq_accounting_token  (hex)
- ibmmq_appl_identity_data
- ibmmq_put_appl_type
- ibmmq_put_appl_name
- ibmmq_appl_origin_data
- ibmmq_group_id      (hex)
- ibmmq_msg_seq_number
- ibmmq_offset
- ibmmq_msg_flags
- ibmmq_original_length
`+"```"+`
`+service.OutputPerformanceDocs(true, false)).
		Fields(
			service.NewStringField(ioFieldQueueManager).
				Description("The name of the IBM MQ queue manager to connect to.").
				Example("QM1"),
			service.NewInterpolatedStringField(ioFieldQueue).
				Description("The name of the target IBM MQ queue.").
				Example("DEV.QUEUE.1"),
			service.NewStringField(ioFieldChannel).
				Description("The name of the server-connection channel.").
				Default("DEV.APP.SVRCONN").
				Example("DEV.APP.SVRCONN"),
			service.NewStringField(ioFieldConnName).
				Description("The connection name in the format `host(port)`.").
				Example("localhost(1414)"),
			service.NewStringField(ioFieldUsername).
				Description("Optional username for authentication.").
				Optional(),
			service.NewStringField(ioFieldPassword).
				Description("Optional password for authentication.").
				Secret().
				Optional(),
			service.NewStringListField(ioFieldPutOptions).
				Description("A list of PMO (Put Message Options) flags to apply when publishing messages. These are OR'd together. Valid values: " + optionKeysDoc(pmoOptionNames) + ".").
				Default([]any{MQPmoNoSyncpoint, MQPmoNewMsgID, MQPmoNewCorrelID}).
				Advanced(),
			service.NewStringListField(ioFieldOpenOptions).
				Description("A list of MQOO (Open Options) flags used when opening the queue for writing. These are OR'd together. Valid values: " + optionKeysDoc(mqooOptionNames) + ".").
				Default([]any{MQOoOutput, MQOoFailIfQuiescing}).
				Advanced(),
			service.NewStringListField(ioFieldConnOptions).
				Description("A list of MQCNO (Connection Options) flags used when connecting to the queue manager. These are OR'd together. Valid values: " + optionKeysDoc(mqcnoOptionNames) + ".").
				Default([]any{MQCnoClientBinding}).
				Advanced(),
			service.NewOutputMaxInFlightField(),
		)
}

func init() {
	err := service.RegisterOutput("ibm_mq", ibmMQOutputSpec(), func(conf *service.ParsedConfig, mgr *service.Resources) (service.Output, int, error) {
		w, err := newIBMMQWriterFromParsed(conf, mgr)
		if err != nil {
			return nil, 0, err
		}
		maxInFlight, err := conf.FieldMaxInFlight()
		if err != nil {
			return nil, 0, err
		}
		return w, maxInFlight, nil
	})
	if err != nil {
		panic(err)
	}
}

type ibmMQWriter struct {
	log *service.Logger

	queueManager string
	queue        *service.InterpolatedString
	channel      string
	connName     string
	username     string
	password     string
	putOptions   int32
	openOptions  int32
	connOptions  int32

	connMut sync.Mutex
	qMgr    *ibmmq.MQQueueManager
}

func newIBMMQWriterFromParsed(conf *service.ParsedConfig, mgr *service.Resources) (*ibmMQWriter, error) {
	w := &ibmMQWriter{
		log: mgr.Logger(),
	}
	var err error

	if w.queueManager, err = conf.FieldString(ioFieldQueueManager); err != nil {
		return nil, err
	}
	if w.queue, err = conf.FieldInterpolatedString(ioFieldQueue); err != nil {
		return nil, err
	}
	if w.channel, err = conf.FieldString(ioFieldChannel); err != nil {
		return nil, err
	}
	if w.connName, err = conf.FieldString(ioFieldConnName); err != nil {
		return nil, err
	}
	w.username, _ = conf.FieldString(ioFieldUsername)
	w.password, _ = conf.FieldString(ioFieldPassword)
	putOptionNames, err := conf.FieldStringList(ioFieldPutOptions)
	if err != nil {
		return nil, err
	}
	if w.putOptions, err = parsePMOOptions(putOptionNames); err != nil {
		return nil, err
	}
	openOptionNames, err := conf.FieldStringList(ioFieldOpenOptions)
	if err != nil {
		return nil, err
	}
	if w.openOptions, err = parseMQOOOptions(openOptionNames); err != nil {
		return nil, err
	}
	connOptionNames, err := conf.FieldStringList(ioFieldConnOptions)
	if err != nil {
		return nil, err
	}
	if w.connOptions, err = parseMQCNOOptions(connOptionNames); err != nil {
		return nil, err
	}
	return w, nil
}

func (w *ibmMQWriter) Connect(ctx context.Context) error {
	w.connMut.Lock()
	defer w.connMut.Unlock()

	if w.qMgr != nil {
		return nil
	}

	cno := ibmmq.NewMQCNO()
	cno.Options = w.connOptions

	cd := ibmmq.NewMQCD()
	cd.ChannelName = w.channel
	cd.ConnectionName = w.connName
	cno.ClientConn = cd

	if w.username != "" {
		csp := ibmmq.NewMQCSP()
		csp.AuthenticationType = ibmmq.MQCSP_AUTH_USER_ID_AND_PWD
		csp.UserId = w.username
		csp.Password = w.password
		cno.SecurityParms = csp
	}

	qMgr, err := ibmmq.Connx(w.queueManager, cno)
	if err != nil {
		return fmt.Errorf("failed to connect to IBM MQ queue manager %q: %w", w.queueManager, err)
	}

	w.qMgr = &qMgr
	w.log.Infof("Connected to IBM MQ queue manager %q via %q", w.queueManager, w.connName)
	return nil
}

func (w *ibmMQWriter) Write(ctx context.Context, msg *service.Message) error {
	w.connMut.Lock()
	qMgr := w.qMgr
	w.connMut.Unlock()

	if qMgr == nil {
		return service.ErrNotConnected
	}

	queueName, err := w.queue.TryString(msg)
	if err != nil {
		return fmt.Errorf("queue interpolation error: %w", err)
	}

	mqod := ibmmq.NewMQOD()
	mqod.ObjectName = queueName
	mqod.ObjectType = ibmmq.MQOT_Q

	openOptions := w.openOptions
	qObject, err := qMgr.Open(mqod, openOptions)
	if err != nil {
		return fmt.Errorf("failed to open IBM MQ queue %q: %w", queueName, err)
	}
	defer func() {
		if cerr := qObject.Close(0); cerr != nil {
			w.log.Errorf("Failed to close IBM MQ queue object: %v", cerr)
		}
	}()

	msgBytes, err := msg.AsBytes()
	if err != nil {
		return err
	}

	mqmd := ibmmq.NewMQMD()
	applyMetaToMQMD(msg, mqmd)
	mqpmo := ibmmq.NewMQPMO()
	mqpmo.Options = w.putOptions

	if err := qObject.Put(mqmd, mqpmo, msgBytes); err != nil {
		return fmt.Errorf("failed to put message to IBM MQ queue %q: %w", queueName, err)
	}
	return nil
}

func (w *ibmMQWriter) Close(context.Context) error {
	w.connMut.Lock()
	defer w.connMut.Unlock()

	if w.qMgr != nil {
		if err := w.qMgr.Disc(); err != nil {
			w.log.Errorf("Failed to disconnect from IBM MQ: %v", err)
		}
		w.qMgr = nil
	}
	return nil
}
