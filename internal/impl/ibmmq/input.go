//go:build x_bento_extra || x_ibmmq

package ibmmq

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/ibm-messaging/mq-golang/v5/ibmmq"

	"github.com/warpstreamlabs/bento/public/service"
)

const (
	iiFieldPollInterval = "poll_interval"
	iiFieldSyncPoint    = "use_sync_point"
)

func ibmMQInputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("Services").
		Stable().
		Summary("Consumes messages from an IBM MQ queue.").
		Description(`Connects to an IBM MQ queue manager and reads messages from the specified queue.

### Metadata

This input adds the following metadata fields to each message:

` + "```text" + `
- ibmmq_msg_id
- ibmmq_correl_id
- ibmmq_format
- ibmmq_reply_to_q
- ibmmq_reply_to_qmgr
` + "```" + `

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
	return r, nil
}

func (r *ibmMQReader) Connect(ctx context.Context) error {
	r.connMut.Lock()
	defer r.connMut.Unlock()

	if r.qMgr != nil {
		return nil
	}

	cno := ibmmq.NewMQCNO()
	cno.Options = ibmmq.MQCNO_CLIENT_BINDING

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

	openOptions := ibmmq.MQOO_INPUT_SHARED | ibmmq.MQOO_FAIL_IF_QUIESCING
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

	mqmd := ibmmq.NewMQMD()
	gmo := ibmmq.NewMQGMO()
	if r.useSyncPoint {
		gmo.Options = ibmmq.MQGMO_SYNCPOINT | ibmmq.MQGMO_FAIL_IF_QUIESCING
	} else {
		gmo.Options = ibmmq.MQGMO_NO_SYNCPOINT | ibmmq.MQGMO_FAIL_IF_QUIESCING
	}

	// Use a wait interval so we don't busy-loop; respect context cancellation.
	waitMs := int32(r.pollInterval.Milliseconds())
	gmo.Options |= ibmmq.MQGMO_WAIT
	gmo.WaitInterval = waitMs

	buf := make([]byte, 32*1024)
	datalen, err := qObject.Get(mqmd, gmo, buf)
	if err != nil {
		mqret := err.(*ibmmq.MQReturn)
		if mqret.MQRC == ibmmq.MQRC_NO_MSG_AVAILABLE {
			// No message yet — signal caller to retry.
			select {
			case <-ctx.Done():
				return nil, nil, ctx.Err()
			default:
			}
			return nil, nil, nil
		}
		return nil, nil, fmt.Errorf("IBM MQ get error: %w", err)
	}

	msg := service.NewMessage(buf[:datalen])
	msg.MetaSetMut("ibmmq_msg_id", fmt.Sprintf("%X", mqmd.MsgId))
	msg.MetaSetMut("ibmmq_correl_id", fmt.Sprintf("%X", mqmd.CorrelId))
	msg.MetaSetMut("ibmmq_format", strings.TrimSpace(mqmd.Format))
	msg.MetaSetMut("ibmmq_reply_to_q", strings.TrimSpace(mqmd.ReplyToQ))
	msg.MetaSetMut("ibmmq_reply_to_qmgr", strings.TrimSpace(mqmd.ReplyToQMgr))

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
