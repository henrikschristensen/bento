package quickfix

import (
	"fmt"
	"strings"

	goquickfix "github.com/quickfixgo/quickfix"

	"github.com/warpstreamlabs/bento/public/service"
)

// bentoLog implements goquickfix.Log, routing FIX engine messages to a bento
// logger at the appropriate level.
//
// Level mapping derived from every OnEvent/OnEventf call site in the QuickFIX/Go
// source:
//
//   - OnIncoming / OnOutgoing  → Debug  (high-volume wire messages)
//   - OnEvent / OnEventf:
//     contains "panic"                                                  → Error
//     contains "failed","invalid","error","rejected","missing",         → Warn
//     "not found","not in session","unable","too high","timeout"
//     everything else (normal session flow: logon, logout, connect …)  → Info
//
// An optional session prefix is prepended to session-scoped log lines.
type bentoLog struct {
	log    *service.Logger
	prefix string // empty for the global log
}

func (l *bentoLog) OnIncoming(msg []byte) {
	l.log.Debugf("%sFIX incoming: %s", l.prefix, msg)
}

func (l *bentoLog) OnOutgoing(msg []byte) {
	l.log.Debugf("%sFIX outgoing: %s", l.prefix, msg)
}

func (l *bentoLog) OnEvent(s string) {
	l.logEvent(s)
}

func (l *bentoLog) OnEventf(format string, a ...any) {
	l.logEvent(fmt.Sprintf(format, a...))
}

// logEvent routes an event message to the correct bento log level.
func (l *bentoLog) logEvent(msg string) {
	lower := strings.ToLower(msg)
	switch {
	case strings.Contains(lower, "panic"):
		l.log.Errorf("%s%s", l.prefix, msg)
	case strings.Contains(lower, "failed") ||
		strings.Contains(lower, "invalid") ||
		strings.Contains(lower, "error") ||
		strings.Contains(lower, "rejected") ||
		strings.Contains(lower, "missing") ||
		strings.Contains(lower, "not found") ||
		strings.Contains(lower, "not in session") ||
		strings.Contains(lower, "unable") ||
		strings.Contains(lower, "too high") ||
		strings.Contains(lower, "timeout"):
		l.log.Warnf("%s%s", l.prefix, msg)
	default:
		l.log.Infof("%s%s", l.prefix, msg)
	}
}

// bentoLogFactory implements goquickfix.LogFactory.
type bentoLogFactory struct {
	log *service.Logger
}

func newBentoLogFactory(log *service.Logger) goquickfix.LogFactory {
	return &bentoLogFactory{log: log}
}

func (f *bentoLogFactory) Create() (goquickfix.Log, error) {
	return &bentoLog{log: f.log}, nil
}

func (f *bentoLogFactory) CreateSessionLog(sessionID goquickfix.SessionID) (goquickfix.Log, error) {
	return &bentoLog{
		log:    f.log,
		prefix: "[" + sessionID.String() + "] ",
	}, nil
}
