//go:build x_bento_extra || x_ibmmq

package ibmmq

import (
	"fmt"
	"sort"
	"strings"

	"github.com/ibm-messaging/mq-golang/v5/ibmmq"
)

// ---------------------------------------------------------------------------
// MQFMT – message format aliases
// ---------------------------------------------------------------------------

// Standard IBM MQ MQMD format strings (8 chars, space-padded).
const (
	MQFmtNone             = "MQFMT_NONE"
	MQFmtString           = "MQFMT_STRING"
	MQFmtRFHeader         = "MQFMT_RF_HEADER"
	MQFmtRFHeader2        = "MQFMT_RF_HEADER_2"
	MQFmtDeadLetterHeader = "MQFMT_DEAD_LETTER_HEADER"
	MQFmtXmitQHeader      = "MQFMT_XMIT_Q_HEADER"
	MQFmtTrigger          = "MQFMT_TRIGGER"
	MQFmtIMS              = "MQFMT_IMS"
	MQFmtIMSVarString     = "MQFMT_IMS_VAR_STRING"
	MQFmtPCF              = "MQFMT_PCF"
	MQFmtEvent            = "MQFMT_EVENT"
	MQFmtEmbeddedPCF      = "MQFMT_EMBEDDED_PCF"
	MQFmtMDExtension      = "MQFMT_MD_EXTENSION"
)

var mqmdFormatAliases = map[string]string{
	MQFmtNone:             "        ",
	MQFmtString:           "MQSTR   ",
	MQFmtRFHeader:         "MQHRF   ",
	MQFmtRFHeader2:        "MQHRF2  ",
	MQFmtDeadLetterHeader: "MQDEAD  ",
	MQFmtXmitQHeader:      "MQXMIT  ",
	MQFmtTrigger:          "MQTRIG  ",
	MQFmtIMS:              "MQIMS   ",
	MQFmtIMSVarString:     "MQIMSVS ",
	MQFmtPCF:              "MQADMIN ",
	MQFmtEvent:            "MQEVENT ",
	MQFmtEmbeddedPCF:      "MQHEPCF ",
	MQFmtMDExtension:      "MQHMDE  ",
}

// resolveMQMDFormat resolves a user-supplied format value to the 8-char MQMD
// Format string. Accepts a MQFMT_* alias or a raw string ≤8 chars (right-padded).
func resolveMQMDFormat(s string) (string, error) {
	if v, ok := mqmdFormatAliases[s]; ok {
		return v, nil
	}
	if len(s) > 8 {
		return "", fmt.Errorf("message format %q exceeds the maximum length of 8 characters", s)
	}
	return fmt.Sprintf("%-8s", s), nil
}

// ---------------------------------------------------------------------------
// MQCNO – connection options
// ---------------------------------------------------------------------------

const (
	MQCnoNone                 = "MQCNO_NONE"
	MQCnoStandardBinding      = "MQCNO_STANDARD_BINDING"
	MQCnoFastpathBinding      = "MQCNO_FASTPATH_BINDING"
	MQCnoSerializeConnTagQMgr = "MQCNO_SERIALIZE_CONN_TAG_Q_MGR"
	MQCnoSerializeConnTagQSG  = "MQCNO_SERIALIZE_CONN_TAG_QSG"
	MQCnoRestrictConnTagQMgr  = "MQCNO_RESTRICT_CONN_TAG_Q_MGR"
	MQCnoRestrictConnTagQSG   = "MQCNO_RESTRICT_CONN_TAG_QSG"
	MQCnoHandleShareNone      = "MQCNO_HANDLE_SHARE_NONE"
	MQCnoHandleShareBlock     = "MQCNO_HANDLE_SHARE_BLOCK"
	MQCnoHandleShareNoBlock   = "MQCNO_HANDLE_SHARE_NO_BLOCK"
	MQCnoSharedBinding        = "MQCNO_SHARED_BINDING"
	MQCnoIsolatedBinding      = "MQCNO_ISOLATED_BINDING"
	MQCnoLocalBinding         = "MQCNO_LOCAL_BINDING"
	MQCnoClientBinding        = "MQCNO_CLIENT_BINDING"
	MQCnoAccountingMQIEnabled = "MQCNO_ACCOUNTING_MQI_ENABLED"
	MQCnoAccountingMQIDisabled = "MQCNO_ACCOUNTING_MQI_DISABLED"
	MQCnoAccountingQEnabled   = "MQCNO_ACCOUNTING_Q_ENABLED"
	MQCnoAccountingQDisabled  = "MQCNO_ACCOUNTING_Q_DISABLED"
	MQCnoNoConvSharing        = "MQCNO_NO_CONV_SHARING"
	MQCnoAllConvsShare        = "MQCNO_ALL_CONVS_SHARE"
	MQCnoCDForOutputOnly      = "MQCNO_CD_FOR_OUTPUT_ONLY"
	MQCnoUseCDSelection       = "MQCNO_USE_CD_SELECTION"
	MQCnoReconnectAsDef       = "MQCNO_RECONNECT_AS_DEF"
	MQCnoReconnect            = "MQCNO_RECONNECT"
	MQCnoReconnectDisabled    = "MQCNO_RECONNECT_DISABLED"
	MQCnoReconnectQMgr        = "MQCNO_RECONNECT_Q_MGR"
	MQCnoActivityTraceEnabled = "MQCNO_ACTIVITY_TRACE_ENABLED"
	MQCnoActivityTraceDisabled = "MQCNO_ACTIVITY_TRACE_DISABLED"
)

var mqcnoOptionNames = map[string]int32{
	MQCnoNone:                  ibmmq.MQCNO_NONE,
	MQCnoStandardBinding:       ibmmq.MQCNO_STANDARD_BINDING,
	MQCnoFastpathBinding:       ibmmq.MQCNO_FASTPATH_BINDING,
	MQCnoSerializeConnTagQMgr:  ibmmq.MQCNO_SERIALIZE_CONN_TAG_Q_MGR,
	MQCnoSerializeConnTagQSG:   ibmmq.MQCNO_SERIALIZE_CONN_TAG_QSG,
	MQCnoRestrictConnTagQMgr:   ibmmq.MQCNO_RESTRICT_CONN_TAG_Q_MGR,
	MQCnoRestrictConnTagQSG:    ibmmq.MQCNO_RESTRICT_CONN_TAG_QSG,
	MQCnoHandleShareNone:       ibmmq.MQCNO_HANDLE_SHARE_NONE,
	MQCnoHandleShareBlock:      ibmmq.MQCNO_HANDLE_SHARE_BLOCK,
	MQCnoHandleShareNoBlock:    ibmmq.MQCNO_HANDLE_SHARE_NO_BLOCK,
	MQCnoSharedBinding:         ibmmq.MQCNO_SHARED_BINDING,
	MQCnoIsolatedBinding:       ibmmq.MQCNO_ISOLATED_BINDING,
	MQCnoLocalBinding:          ibmmq.MQCNO_LOCAL_BINDING,
	MQCnoClientBinding:         ibmmq.MQCNO_CLIENT_BINDING,
	MQCnoAccountingMQIEnabled:  ibmmq.MQCNO_ACCOUNTING_MQI_ENABLED,
	MQCnoAccountingMQIDisabled: ibmmq.MQCNO_ACCOUNTING_MQI_DISABLED,
	MQCnoAccountingQEnabled:    ibmmq.MQCNO_ACCOUNTING_Q_ENABLED,
	MQCnoAccountingQDisabled:   ibmmq.MQCNO_ACCOUNTING_Q_DISABLED,
	MQCnoNoConvSharing:         ibmmq.MQCNO_NO_CONV_SHARING,
	MQCnoAllConvsShare:         ibmmq.MQCNO_ALL_CONVS_SHARE,
	MQCnoCDForOutputOnly:       ibmmq.MQCNO_CD_FOR_OUTPUT_ONLY,
	MQCnoUseCDSelection:        ibmmq.MQCNO_USE_CD_SELECTION,
	MQCnoReconnectAsDef:        ibmmq.MQCNO_RECONNECT_AS_DEF,
	MQCnoReconnect:             ibmmq.MQCNO_RECONNECT,
	MQCnoReconnectDisabled:     ibmmq.MQCNO_RECONNECT_DISABLED,
	MQCnoReconnectQMgr:         ibmmq.MQCNO_RECONNECT_Q_MGR,
	MQCnoActivityTraceEnabled:  ibmmq.MQCNO_ACTIVITY_TRACE_ENABLED,
	MQCnoActivityTraceDisabled: ibmmq.MQCNO_ACTIVITY_TRACE_DISABLED,
}

func parseMQCNOOptions(names []string) (int32, error) {
	return parseOptions("connection", mqcnoOptionNames, names)
}

// ---------------------------------------------------------------------------
// MQOO – open options
// ---------------------------------------------------------------------------

const (
	MQOoInputAsQDef         = "MQOO_INPUT_AS_Q_DEF"
	MQOoInputShared         = "MQOO_INPUT_SHARED"
	MQOoInputExclusive      = "MQOO_INPUT_EXCLUSIVE"
	MQOoBrowse              = "MQOO_BROWSE"
	MQOoOutput              = "MQOO_OUTPUT"
	MQOoInquire             = "MQOO_INQUIRE"
	MQOoSet                 = "MQOO_SET"
	MQOoSaveAllContext      = "MQOO_SAVE_ALL_CONTEXT"
	MQOoPassIdentityContext = "MQOO_PASS_IDENTITY_CONTEXT"
	MQOoPassAllContext      = "MQOO_PASS_ALL_CONTEXT"
	MQOoSetIdentityContext  = "MQOO_SET_IDENTITY_CONTEXT"
	MQOoSetAllContext       = "MQOO_SET_ALL_CONTEXT"
	MQOoAlternateUserAuth   = "MQOO_ALTERNATE_USER_AUTHORITY"
	MQOoFailIfQuiescing     = "MQOO_FAIL_IF_QUIESCING"
	MQOoBindOnOpen          = "MQOO_BIND_ON_OPEN"
	MQOoBindNotFixed        = "MQOO_BIND_NOT_FIXED"
	MQOoResolveNames        = "MQOO_RESOLVE_NAMES"
	MQOoCoOp                = "MQOO_CO_OP"
	MQOoResolveLocalQ       = "MQOO_RESOLVE_LOCAL_Q"
	MQOoNoReadAhead         = "MQOO_NO_READ_AHEAD"
	MQOoReadAhead           = "MQOO_READ_AHEAD"
	MQOoNoMulticast         = "MQOO_NO_MULTICAST"
	MQOoBindOnGroup         = "MQOO_BIND_ON_GROUP"
)

var mqooOptionNames = map[string]int32{
	MQOoInputAsQDef:         ibmmq.MQOO_INPUT_AS_Q_DEF,
	MQOoInputShared:         ibmmq.MQOO_INPUT_SHARED,
	MQOoInputExclusive:      ibmmq.MQOO_INPUT_EXCLUSIVE,
	MQOoBrowse:              ibmmq.MQOO_BROWSE,
	MQOoOutput:              ibmmq.MQOO_OUTPUT,
	MQOoInquire:             ibmmq.MQOO_INQUIRE,
	MQOoSet:                 ibmmq.MQOO_SET,
	MQOoSaveAllContext:      ibmmq.MQOO_SAVE_ALL_CONTEXT,
	MQOoPassIdentityContext: ibmmq.MQOO_PASS_IDENTITY_CONTEXT,
	MQOoPassAllContext:      ibmmq.MQOO_PASS_ALL_CONTEXT,
	MQOoSetIdentityContext:  ibmmq.MQOO_SET_IDENTITY_CONTEXT,
	MQOoSetAllContext:       ibmmq.MQOO_SET_ALL_CONTEXT,
	MQOoAlternateUserAuth:   ibmmq.MQOO_ALTERNATE_USER_AUTHORITY,
	MQOoFailIfQuiescing:     ibmmq.MQOO_FAIL_IF_QUIESCING,
	MQOoBindOnOpen:          ibmmq.MQOO_BIND_ON_OPEN,
	MQOoBindNotFixed:        ibmmq.MQOO_BIND_NOT_FIXED,
	MQOoResolveNames:        ibmmq.MQOO_RESOLVE_NAMES,
	MQOoCoOp:                ibmmq.MQOO_CO_OP,
	MQOoResolveLocalQ:       ibmmq.MQOO_RESOLVE_LOCAL_Q,
	MQOoNoReadAhead:         ibmmq.MQOO_NO_READ_AHEAD,
	MQOoReadAhead:           ibmmq.MQOO_READ_AHEAD,
	MQOoNoMulticast:         ibmmq.MQOO_NO_MULTICAST,
	MQOoBindOnGroup:         ibmmq.MQOO_BIND_ON_GROUP,
}

func parseMQOOOptions(names []string) (int32, error) {
	return parseOptions("open", mqooOptionNames, names)
}

// ---------------------------------------------------------------------------
// MQGMO – get message options
// ---------------------------------------------------------------------------

const (
	MQGmoWait                 = "MQGMO_WAIT"
	MQGmoNoWait               = "MQGMO_NO_WAIT"
	MQGmoSyncpoint            = "MQGMO_SYNCPOINT"
	MQGmoNoSyncpoint          = "MQGMO_NO_SYNCPOINT"
	MQGmoFailIfQuiescing      = "MQGMO_FAIL_IF_QUIESCING"
	MQGmoConvert              = "MQGMO_CONVERT"
	MQGmoAcceptTruncatedMsg   = "MQGMO_ACCEPT_TRUNCATED_MSG"
	MQGmoBrowseFirst          = "MQGMO_BROWSE_FIRST"
	MQGmoBrowseNext           = "MQGMO_BROWSE_NEXT"
	MQGmoBrowseMsgUnderCursor = "MQGMO_BROWSE_MSG_UNDER_CURSOR"
	MQGmoLock                 = "MQGMO_LOCK"
	MQGmoUnlock               = "MQGMO_UNLOCK"
	MQGmoMarkSkipBackout      = "MQGMO_MARK_SKIP_BACKOUT"
	MQGmoCompleteMsg          = "MQGMO_COMPLETE_MSG"
	MQGmoLogicalOrder         = "MQGMO_LOGICAL_ORDER"
)

var gmoOptionNames = map[string]int32{
	MQGmoWait:                 ibmmq.MQGMO_WAIT,
	MQGmoNoWait:               ibmmq.MQGMO_NO_WAIT,
	MQGmoSyncpoint:            ibmmq.MQGMO_SYNCPOINT,
	MQGmoNoSyncpoint:          ibmmq.MQGMO_NO_SYNCPOINT,
	MQGmoFailIfQuiescing:      ibmmq.MQGMO_FAIL_IF_QUIESCING,
	MQGmoConvert:              ibmmq.MQGMO_CONVERT,
	MQGmoAcceptTruncatedMsg:   ibmmq.MQGMO_ACCEPT_TRUNCATED_MSG,
	MQGmoBrowseFirst:          ibmmq.MQGMO_BROWSE_FIRST,
	MQGmoBrowseNext:           ibmmq.MQGMO_BROWSE_NEXT,
	MQGmoBrowseMsgUnderCursor: ibmmq.MQGMO_BROWSE_MSG_UNDER_CURSOR,
	MQGmoLock:                 ibmmq.MQGMO_LOCK,
	MQGmoUnlock:               ibmmq.MQGMO_UNLOCK,
	MQGmoMarkSkipBackout:      ibmmq.MQGMO_MARK_SKIP_BACKOUT,
	MQGmoCompleteMsg:          ibmmq.MQGMO_COMPLETE_MSG,
	MQGmoLogicalOrder:         ibmmq.MQGMO_LOGICAL_ORDER,
}

func parseGMOOptions(names []string) (int32, error) {
	return parseOptions("get message", gmoOptionNames, names)
}

// ---------------------------------------------------------------------------
// MQPMO – put message options
// ---------------------------------------------------------------------------

const (
	MQPmoSyncpoint           = "MQPMO_SYNCPOINT"
	MQPmoNoSyncpoint         = "MQPMO_NO_SYNCPOINT"
	MQPmoNewMsgID            = "MQPMO_NEW_MSG_ID"
	MQPmoNewCorrelID         = "MQPMO_NEW_CORREL_ID"
	MQPmoFailIfQuiescing     = "MQPMO_FAIL_IF_QUIESCING"
	MQPmoPassIdentityContext = "MQPMO_PASS_IDENTITY_CONTEXT"
	MQPmoPassAllContext      = "MQPMO_PASS_ALL_CONTEXT"
	MQPmoSetIdentityContext  = "MQPMO_SET_IDENTITY_CONTEXT"
	MQPmoSetAllContext       = "MQPMO_SET_ALL_CONTEXT"
	MQPmoDefaultContext      = "MQPMO_DEFAULT_CONTEXT"
	MQPmoNoContext           = "MQPMO_NO_CONTEXT"
	MQPmoLogicalOrder        = "MQPMO_LOGICAL_ORDER"
	MQPmoAsyncResponse       = "MQPMO_ASYNC_RESPONSE"
	MQPmoSyncResponse        = "MQPMO_SYNC_RESPONSE"
)

var pmoOptionNames = map[string]int32{
	MQPmoSyncpoint:           ibmmq.MQPMO_SYNCPOINT,
	MQPmoNoSyncpoint:         ibmmq.MQPMO_NO_SYNCPOINT,
	MQPmoNewMsgID:            ibmmq.MQPMO_NEW_MSG_ID,
	MQPmoNewCorrelID:         ibmmq.MQPMO_NEW_CORREL_ID,
	MQPmoFailIfQuiescing:     ibmmq.MQPMO_FAIL_IF_QUIESCING,
	MQPmoPassIdentityContext: ibmmq.MQPMO_PASS_IDENTITY_CONTEXT,
	MQPmoPassAllContext:      ibmmq.MQPMO_PASS_ALL_CONTEXT,
	MQPmoSetIdentityContext:  ibmmq.MQPMO_SET_IDENTITY_CONTEXT,
	MQPmoSetAllContext:       ibmmq.MQPMO_SET_ALL_CONTEXT,
	MQPmoDefaultContext:      ibmmq.MQPMO_DEFAULT_CONTEXT,
	MQPmoNoContext:           ibmmq.MQPMO_NO_CONTEXT,
	MQPmoLogicalOrder:        ibmmq.MQPMO_LOGICAL_ORDER,
	MQPmoAsyncResponse:       ibmmq.MQPMO_ASYNC_RESPONSE,
	MQPmoSyncResponse:        ibmmq.MQPMO_SYNC_RESPONSE,
}

func parsePMOOptions(names []string) (int32, error) {
	return parseOptions("put message", pmoOptionNames, names)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// parseOptions is the generic OR-combiner used by all option parsers.
func parseOptions(kind string, table map[string]int32, names []string) (int32, error) {
	var opts int32
	for _, name := range names {
		v, ok := table[name]
		if !ok {
			return 0, fmt.Errorf("unknown %s option %q", kind, name)
		}
		opts |= v
	}
	return opts, nil
}

// optionKeysDoc returns a sorted, backtick-quoted, comma-separated list of all
// keys in the given map for use in field description strings.
func optionKeysDoc(table map[string]int32) string {
	keys := make([]string, 0, len(table))
	for k := range table {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	quoted := make([]string, len(keys))
	for i, k := range keys {
		quoted[i] = "`" + k + "`"
	}
	return strings.Join(quoted, ", ")
}

// mqfmtKeysDoc returns a sorted, backtick-quoted, comma-separated list of all
// MQFMT alias names for use in field description strings.
func mqfmtKeysDoc() string {
	keys := make([]string, 0, len(mqmdFormatAliases))
	for k := range mqmdFormatAliases {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	quoted := make([]string, len(keys))
	for i, k := range keys {
		quoted[i] = "`" + k + "`"
	}
	return strings.Join(quoted, ", ")
}
