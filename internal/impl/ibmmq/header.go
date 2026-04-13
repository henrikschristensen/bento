//go:build x_bento_extra || x_ibmmq

package ibmmq

import (
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"

	"github.com/ibm-messaging/mq-golang/v5/ibmmq"

	"github.com/warpstreamlabs/bento/public/service"
)

// mqmdToMeta populates msg metadata from all MQMD fields. Integer fields are
// formatted as decimal strings; byte-array fields (MsgId, CorrelId,
// AccountingToken, GroupId) are upper-case hex-encoded; string fields are
// trimmed of trailing spaces.
func mqmdToMeta(mqmd *ibmmq.MQMD, msg *service.Message) {
	msg.MetaSetMut("ibmmq_version", fmt.Sprintf("%d", mqmd.Version))
	msg.MetaSetMut("ibmmq_report", fmt.Sprintf("%d", mqmd.Report))
	msg.MetaSetMut("ibmmq_msg_type", fmt.Sprintf("%d", mqmd.MsgType))
	msg.MetaSetMut("ibmmq_expiry", fmt.Sprintf("%d", mqmd.Expiry))
	msg.MetaSetMut("ibmmq_feedback", fmt.Sprintf("%d", mqmd.Feedback))
	msg.MetaSetMut("ibmmq_encoding", fmt.Sprintf("%d", mqmd.Encoding))
	msg.MetaSetMut("ibmmq_coded_char_set_id", fmt.Sprintf("%d", mqmd.CodedCharSetId))
	msg.MetaSetMut("ibmmq_format", strings.TrimSpace(mqmd.Format))
	msg.MetaSetMut("ibmmq_priority", fmt.Sprintf("%d", mqmd.Priority))
	msg.MetaSetMut("ibmmq_persistence", fmt.Sprintf("%d", mqmd.Persistence))
	msg.MetaSetMut("ibmmq_msg_id", fmt.Sprintf("%X", mqmd.MsgId))
	msg.MetaSetMut("ibmmq_correl_id", fmt.Sprintf("%X", mqmd.CorrelId))
	msg.MetaSetMut("ibmmq_backout_count", fmt.Sprintf("%d", mqmd.BackoutCount))
	msg.MetaSetMut("ibmmq_reply_to_q", strings.TrimSpace(mqmd.ReplyToQ))
	msg.MetaSetMut("ibmmq_reply_to_qmgr", strings.TrimSpace(mqmd.ReplyToQMgr))
	msg.MetaSetMut("ibmmq_user_identifier", strings.TrimSpace(mqmd.UserIdentifier))
	msg.MetaSetMut("ibmmq_accounting_token", fmt.Sprintf("%X", mqmd.AccountingToken))
	msg.MetaSetMut("ibmmq_appl_identity_data", strings.TrimSpace(mqmd.ApplIdentityData))
	msg.MetaSetMut("ibmmq_put_appl_type", fmt.Sprintf("%d", mqmd.PutApplType))
	msg.MetaSetMut("ibmmq_put_appl_name", strings.TrimSpace(mqmd.PutApplName))
	msg.MetaSetMut("ibmmq_put_date", strings.TrimSpace(mqmd.PutDate))
	msg.MetaSetMut("ibmmq_put_time", strings.TrimSpace(mqmd.PutTime))
	msg.MetaSetMut("ibmmq_appl_origin_data", strings.TrimSpace(mqmd.ApplOriginData))
	msg.MetaSetMut("ibmmq_group_id", fmt.Sprintf("%X", mqmd.GroupId))
	msg.MetaSetMut("ibmmq_msg_seq_number", fmt.Sprintf("%d", mqmd.MsgSeqNumber))
	msg.MetaSetMut("ibmmq_offset", fmt.Sprintf("%d", mqmd.Offset))
	msg.MetaSetMut("ibmmq_msg_flags", fmt.Sprintf("%d", mqmd.MsgFlags))
	msg.MetaSetMut("ibmmq_original_length", fmt.Sprintf("%d", mqmd.OriginalLength))
}

// applyMetaToMQMD reads ibmmq_* metadata from msg and applies the values to
// mqmd. Only user-writable MQMD fields are updated; fields managed by the
// queue manager (Version, MsgType, Expiry, BackoutCount, Persistence,
// PutDate, PutTime) are intentionally skipped. Fields absent from the message
// metadata are left unchanged, so config-derived defaults remain in effect.
func applyMetaToMQMD(msg *service.Message, mqmd *ibmmq.MQMD) {
	metaInt32 := func(key string) (int32, bool) {
		v, ok := msg.MetaGet(key)
		if !ok || v == "" {
			return 0, false
		}
		n, err := strconv.ParseInt(v, 10, 32)
		if err != nil {
			return 0, false
		}
		return int32(n), true
	}

	metaHex := func(key string, size int) ([]byte, bool) {
		v, ok := msg.MetaGet(key)
		if !ok || v == "" {
			return nil, false
		}
		b, err := hex.DecodeString(v)
		if err != nil {
			return nil, false
		}
		buf := make([]byte, size)
		copy(buf, b)
		return buf, true
	}

	metaStr := func(key string) (string, bool) {
		return msg.MetaGet(key)
	}

	if v, ok := metaInt32("ibmmq_report"); ok {
		mqmd.Report = v
	}
	if v, ok := metaInt32("ibmmq_feedback"); ok {
		mqmd.Feedback = v
	}
	if v, ok := metaInt32("ibmmq_encoding"); ok {
		mqmd.Encoding = v
	}
	if v, ok := metaInt32("ibmmq_coded_char_set_id"); ok {
		mqmd.CodedCharSetId = v
	}
	if v, ok := metaStr("ibmmq_format"); ok {
		if resolved, err := resolveMQMDFormat(v); err == nil {
			mqmd.Format = resolved
		}
	}
	if v, ok := metaInt32("ibmmq_priority"); ok {
		mqmd.Priority = v
	}
	if v, ok := metaHex("ibmmq_msg_id", int(ibmmq.MQ_MSG_ID_LENGTH)); ok {
		mqmd.MsgId = v
	}
	if v, ok := metaHex("ibmmq_correl_id", int(ibmmq.MQ_CORREL_ID_LENGTH)); ok {
		mqmd.CorrelId = v
	}
	if v, ok := metaStr("ibmmq_reply_to_q"); ok {
		mqmd.ReplyToQ = v
	}
	if v, ok := metaStr("ibmmq_reply_to_qmgr"); ok {
		mqmd.ReplyToQMgr = v
	}
	if v, ok := metaStr("ibmmq_user_identifier"); ok {
		mqmd.UserIdentifier = v
	}
	if v, ok := metaHex("ibmmq_accounting_token", int(ibmmq.MQ_ACCOUNTING_TOKEN_LENGTH)); ok {
		mqmd.AccountingToken = v
	}
	if v, ok := metaStr("ibmmq_appl_identity_data"); ok {
		mqmd.ApplIdentityData = v
	}
	if v, ok := metaInt32("ibmmq_put_appl_type"); ok {
		mqmd.PutApplType = v
	}
	if v, ok := metaStr("ibmmq_put_appl_name"); ok {
		mqmd.PutApplName = v
	}
	if v, ok := metaStr("ibmmq_appl_origin_data"); ok {
		mqmd.ApplOriginData = v
	}
	if v, ok := metaHex("ibmmq_group_id", int(ibmmq.MQ_GROUP_ID_LENGTH)); ok {
		mqmd.GroupId = v
	}
	if v, ok := metaInt32("ibmmq_msg_seq_number"); ok {
		mqmd.MsgSeqNumber = v
	}
	if v, ok := metaInt32("ibmmq_offset"); ok {
		mqmd.Offset = v
	}
	if v, ok := metaInt32("ibmmq_msg_flags"); ok {
		mqmd.MsgFlags = v
	}
	if v, ok := metaInt32("ibmmq_original_length"); ok {
		mqmd.OriginalLength = v
	}
}
