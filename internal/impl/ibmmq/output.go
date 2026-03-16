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
)

func ibmMQOutputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("Services").
		Stable().
		Summary("Sends messages to an IBM MQ queue.").
		Description(`Connects to an IBM MQ queue manager and publishes messages to the specified queue.

The `+"`queue`"+` field can be dynamically set using function interpolations described [here](/docs/configuration/interpolation#bloblang-queries).`+service.OutputPerformanceDocs(true, false)).
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
	return w, nil
}

func (w *ibmMQWriter) Connect(ctx context.Context) error {
	w.connMut.Lock()
	defer w.connMut.Unlock()

	if w.qMgr != nil {
		return nil
	}

	cno := ibmmq.NewMQCNO()
	cno.Options = ibmmq.MQCNO_CLIENT_BINDING

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

	openOptions := ibmmq.MQOO_OUTPUT | ibmmq.MQOO_FAIL_IF_QUIESCING
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
	mqpmo := ibmmq.NewMQPMO()
	mqpmo.Options = ibmmq.MQPMO_NO_SYNCPOINT | ibmmq.MQPMO_NEW_MSG_ID | ibmmq.MQPMO_NEW_CORREL_ID

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
