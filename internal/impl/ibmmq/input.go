//go:build x_bento_extra || x_ibmmq

package ibmmq

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ibm-messaging/mq-golang/v5/ibmmq"

	"github.com/warpstreamlabs/bento/public/service"
)

const (
	iiFieldPollInterval = "poll_interval"
	iiFieldSyncPoint    = "use_sync_point"
	iiFieldGetOptions   = "get_options"
	iiFieldMsgFormat    = "message_format"
	iiFieldOpenOptions  = "open_options"
	iiFieldConnOptions  = "connection_options"
)

func ibmMQInputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("Services").
		Stable().
		Summary("Consumes messages from an IBM MQ queue.").
		Description(`Connects to an IBM MQ queue manager and reads messages from the specified queue.

### Metadata

This input adds the following metadata fields to each message:

`+"```text"+`
- ibmmq_version
- ibmmq_report
- ibmmq_msg_type
- ibmmq_expiry
- ibmmq_feedback
- ibmmq_encoding
- ibmmq_coded_char_set_id
- ibmmq_format
- ibmmq_priority
- ibmmq_persistence
- ibmmq_msg_id
- ibmmq_correl_id
- ibmmq_backout_count
- ibmmq_reply_to_q
- ibmmq_reply_to_qmgr
- ibmmq_user_identifier
- ibmmq_accounting_token
- ibmmq_appl_identity_data
- ibmmq_put_appl_type
- ibmmq_put_appl_name
- ibmmq_put_date
- ibmmq_put_time
- ibmmq_appl_origin_data
- ibmmq_group_id
- ibmmq_msg_seq_number
- ibmmq_offset
- ibmmq_msg_flags
- ibmmq_original_length
`+"```"+`

Integer fields are formatted as decimal strings. Byte-array fields
(`+"`ibmmq_msg_id`"+`, `+"`ibmmq_correl_id`"+`, `+"`ibmmq_accounting_token`"+`,
`+"`ibmmq_group_id`"+`) are upper-case hex-encoded. String fields have trailing
spaces trimmed.

You can access these metadata fields using [function interpolation](/docs/configuration/interpolation#bloblang-queries).`).
		Fields(
			service.NewStringField(ioFieldQueueManager).
				Description("The name of the IBM MQ queue manager to connect to.").
				Example("QM1"),
			service.NewStringField(ioFieldQueue).
				Description("The name of the IBM MQ queue to consume from.").
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
			service.NewDurationField(iiFieldPollInterval).
				Description("The duration to wait between polling the queue when no messages are available.").
				Default("500ms").
				Advanced(),
			service.NewBoolField(iiFieldSyncPoint).
				Description("When true, messages are retrieved under a syncpoint (transaction). The message is only removed from the queue once the ack function is called successfully; a nack triggers a backout.").
				Default(false).
				Advanced(),
			service.NewStringListField(iiFieldGetOptions).
				Description("A list of GMO (Get Message Options) flags to apply when retrieving messages. These are OR'd together. Valid values: "+optionKeysDoc(gmoOptionNames)+". The syncpoint flags are always overridden by `use_sync_point`.").
				Default([]any{MQGmoWait, MQGmoFailIfQuiescing}).
				Advanced(),
			service.NewStringField(iiFieldMsgFormat).
				Description("The MQMD message format to request when retrieving messages. When set alongside `"+MQGmoConvert+"` in `get_options`, the queue manager will convert the message data to this format before returning it. Accepts well-known aliases ("+mqfmtKeysDoc()+") or a raw string up to 8 characters. Defaults to `"+MQFmtNone+"` (no conversion requested).").
				Default(MQFmtNone).
				Advanced(),
			service.NewStringListField(iiFieldOpenOptions).
				Description("A list of MQOO (Open Options) flags used when opening the queue for reading. These are OR'd together. Valid values: "+optionKeysDoc(mqooOptionNames)+".").
				Default([]any{MQOoInputShared, MQOoFailIfQuiescing}).
				Advanced(),
			service.NewStringListField(iiFieldConnOptions).
				Description("A list of MQCNO (Connection Options) flags used when connecting to the queue manager. These are OR'd together. Valid values: "+optionKeysDoc(mqcnoOptionNames)+".").
				Default([]any{MQCnoClientBinding}).
				Advanced(),
		)
}

func init() {
	err := service.RegisterInput("ibm_mq", ibmMQInputSpec(), func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
		return newIBMMQReaderFromParsed(conf, mgr)
	})
	if err != nil {
		panic(err)
	}
}

type ibmMQReader struct {
	log *service.Logger

	queueManager string
	queue        string
	channel      string
	connName     string
	username     string
	password     string
	pollInterval time.Duration
	useSyncPoint bool
	getOptions   int32
	msgFormat    string
	openOptions  int32
	connOptions  int32

	connMut sync.Mutex
	qMgr    *ibmmq.MQQueueManager
	qObject *ibmmq.MQObject
}

func newIBMMQReaderFromParsed(conf *service.ParsedConfig, mgr *service.Resources) (*ibmMQReader, error) {
	r := &ibmMQReader{
		log: mgr.Logger(),
	}
	var err error

	if r.queueManager, err = conf.FieldString(ioFieldQueueManager); err != nil {
		return nil, err
	}
	if r.queue, err = conf.FieldString(ioFieldQueue); err != nil {
		return nil, err
	}
	if r.channel, err = conf.FieldString(ioFieldChannel); err != nil {
		return nil, err
	}
	if r.connName, err = conf.FieldString(ioFieldConnName); err != nil {
		return nil, err
	}
	r.username, _ = conf.FieldString(ioFieldUsername)
	r.password, _ = conf.FieldString(ioFieldPassword)
	if r.pollInterval, err = conf.FieldDuration(iiFieldPollInterval); err != nil {
		return nil, err
	}
	if r.useSyncPoint, err = conf.FieldBool(iiFieldSyncPoint); err != nil {
		return nil, err
	}
	getOptionNames, err := conf.FieldStringList(iiFieldGetOptions)
	if err != nil {
		return nil, err
	}
	if r.getOptions, err = parseGMOOptions(getOptionNames); err != nil {
		return nil, err
	}
	msgFormatRaw, err := conf.FieldString(iiFieldMsgFormat)
	if err != nil {
		return nil, err
	}
	if r.msgFormat, err = resolveMQMDFormat(msgFormatRaw); err != nil {
		return nil, err
	}
	openOptionNames, err := conf.FieldStringList(iiFieldOpenOptions)
	if err != nil {
		return nil, err
	}
	if r.openOptions, err = parseMQOOOptions(openOptionNames); err != nil {
		return nil, err
	}
	connOptionNames, err := conf.FieldStringList(iiFieldConnOptions)
	if err != nil {
		return nil, err
	}
	if r.connOptions, err = parseMQCNOOptions(connOptionNames); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *ibmMQReader) Connect(ctx context.Context) error {
	r.connMut.Lock()
	defer r.connMut.Unlock()

	if r.qMgr != nil {
		return nil
	}

	cno := ibmmq.NewMQCNO()
	cno.Options = r.connOptions

	cd := ibmmq.NewMQCD()
	cd.ChannelName = r.channel
	cd.ConnectionName = r.connName
	cno.ClientConn = cd

	if r.username != "" {
		csp := ibmmq.NewMQCSP()
		csp.AuthenticationType = ibmmq.MQCSP_AUTH_USER_ID_AND_PWD
		csp.UserId = r.username
		csp.Password = r.password
		cno.SecurityParms = csp
	}

	qMgr, err := ibmmq.Connx(r.queueManager, cno)
	if err != nil {
		return fmt.Errorf("failed to connect to IBM MQ queue manager %q: %w", r.queueManager, err)
	}

	mqod := ibmmq.NewMQOD()
	mqod.ObjectName = r.queue
	mqod.ObjectType = ibmmq.MQOT_Q

	openOptions := r.openOptions
	qObject, err := qMgr.Open(mqod, openOptions)
	if err != nil {
		_ = qMgr.Disc()
		return fmt.Errorf("failed to open IBM MQ queue %q: %w", r.queue, err)
	}

	r.qMgr = &qMgr
	r.qObject = &qObject
	r.log.Infof("Connected to IBM MQ queue manager %q, reading from queue %q", r.queueManager, r.queue)
	return nil
}

func (r *ibmMQReader) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	r.connMut.Lock()
	qObject := r.qObject
	r.connMut.Unlock()

	if qObject == nil {
		return nil, nil, service.ErrNotConnected
	}

	// Build GMO options; WaitInterval causes Get to block up to pollInterval
	// before returning MQRC_NO_MSG_AVAILABLE, so we loop until a message arrives,
	// a real error occurs, or the context is cancelled.
	baseOptions := r.getOptions
	if r.useSyncPoint {
		baseOptions = (baseOptions &^ ibmmq.MQGMO_NO_SYNCPOINT) | ibmmq.MQGMO_SYNCPOINT
	} else {
		baseOptions = (baseOptions &^ ibmmq.MQGMO_SYNCPOINT) | ibmmq.MQGMO_NO_SYNCPOINT
	}
	waitMs := int32(r.pollInterval.Milliseconds())

	var (
		mqmd    *ibmmq.MQMD
		buf     []byte
		datalen int
	)
	for {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}

		mqmd = ibmmq.NewMQMD()
		mqmd.Format = r.msgFormat
		gmo := ibmmq.NewMQGMO()
		gmo.Options = baseOptions
		gmo.WaitInterval = waitMs

		buf = make([]byte, 32*1024)
		var err error
		datalen, err = qObject.Get(mqmd, gmo, buf)
		if err != nil {
			mqret := err.(*ibmmq.MQReturn)
			if mqret.MQRC == ibmmq.MQRC_NO_MSG_AVAILABLE {
				// Timed out waiting — check context and retry.
				continue
			}
			return nil, nil, fmt.Errorf("IBM MQ get error: %w", err)
		}
		break
	}

	msg := service.NewMessage(buf[:datalen])
	mqmdToMeta(mqmd, msg)

	return msg, func(_ context.Context, aErr error) error {
		if !r.useSyncPoint {
			// MQGMO_NO_SYNCPOINT removes messages immediately on Get; nothing to ack/nack.
			return nil
		}
		r.connMut.Lock()
		qMgr := r.qMgr
		r.connMut.Unlock()
		if qMgr == nil {
			return nil
		}
		if aErr != nil {
			return qMgr.Back()
		}
		return qMgr.Cmit()
	}, nil
}

func (r *ibmMQReader) Close(ctx context.Context) error {
	r.connMut.Lock()
	defer r.connMut.Unlock()

	if r.qObject != nil {
		if err := r.qObject.Close(0); err != nil {
			r.log.Errorf("Failed to close IBM MQ queue object: %v", err)
		}
		r.qObject = nil
	}
	if r.qMgr != nil {
		if err := r.qMgr.Disc(); err != nil {
			r.log.Errorf("Failed to disconnect from IBM MQ: %v", err)
		}
		r.qMgr = nil
	}
	return nil
}
