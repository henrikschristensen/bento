package quickfix

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	goquickfix "github.com/quickfixgo/quickfix"
	"github.com/quickfixgo/quickfix/datadictionary"
)

// buildNewOrderSingle builds a minimal NewOrderSingle (35=D) message programmatically.
func buildNewOrderSingle() *goquickfix.Message {
	msg := goquickfix.NewMessage()
	msg.Header.SetString(tagBeginString, "FIX.4.2")
	msg.Header.SetString(tagMsgType, "D")
	msg.Header.SetString(tagSenderCompID, "TW")
	msg.Header.SetString(tagTargetCompID, "ISLD")
	msg.Header.SetInt(goquickfix.Tag(34), 2)
	msg.Header.SetString(goquickfix.Tag(52), "20140515-19:49:56.659") // SendingTime
	msg.Body.SetString(goquickfix.Tag(11), "100")                     // ClOrdID
	msg.Body.SetString(goquickfix.Tag(21), "1")                       // HandlInst
	msg.Body.SetString(goquickfix.Tag(40), "1")                       // OrdType
	msg.Body.SetString(goquickfix.Tag(54), "1")                       // Side
	msg.Body.SetString(goquickfix.Tag(55), "TSLA")                    // Symbol
	msg.Body.SetString(goquickfix.Tag(60), "00010101-00:00:00.000")   // TransactTime
	return msg
}

// mustLoadDD loads a data dictionary from the testdata directory.
func mustLoadDD(t *testing.T, path string) *datadictionary.DataDictionary {
	t.Helper()
	dd, err := datadictionary.Parse(path)
	require.NoError(t, err)
	return dd
}

// ----------------------------------------------------------------------------
// messageToJSON – no data dictionary (numeric keys)
// ----------------------------------------------------------------------------

func TestToJSONNoDictionary(t *testing.T) {
	msg := buildNewOrderSingle()

	jsonBytes, err := messageToJSON(msg, nil)
	require.NoError(t, err)

	var parsed map[string]map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(jsonBytes, &parsed))

	// Top-level sections must be present.
	assert.Contains(t, parsed, "Header")
	assert.Contains(t, parsed, "Body")
	assert.Contains(t, parsed, "Trailer")

	// Field values are stored as numeric keys when no DD is available.
	headerStr := func(key string) string {
		var s string
		require.NoError(t, json.Unmarshal(parsed["Header"][key], &s))
		return s
	}
	assert.Equal(t, "FIX.4.2", headerStr("8"))
	assert.Equal(t, "D", headerStr("35"))
	assert.Equal(t, "TW", headerStr("49"))
	assert.Equal(t, "ISLD", headerStr("56"))

	bodyStr := func(key string) string {
		var s string
		require.NoError(t, json.Unmarshal(parsed["Body"][key], &s))
		return s
	}
	assert.Equal(t, "TSLA", bodyStr("55"))
	assert.Equal(t, "1", bodyStr("54"))

	// CheckSum must NOT appear in the Trailer section.
	_, hasCheckSum := parsed["Trailer"]["10"]
	assert.False(t, hasCheckSum, "CheckSum should be excluded from JSON output")
}

// ----------------------------------------------------------------------------
// messageToJSON – with data dictionary (named keys)
// ----------------------------------------------------------------------------

func TestToJSONWithDictionary(t *testing.T) {
	dd := mustLoadDD(t, "testdata/FIX42.xml")

	msg := buildNewOrderSingle()

	jsonBytes, err := messageToJSON(msg, dd)
	require.NoError(t, err)

	var parsed map[string]map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(jsonBytes, &parsed))

	strVal := func(section, key string) string {
		t.Helper()
		v, ok := parsed[section][key]
		require.True(t, ok, "section %q key %q not found in JSON", section, key)
		var s string
		require.NoError(t, json.Unmarshal(v, &s))
		return s
	}

	// Named keys from the data dictionary.
	assert.Equal(t, "FIX.4.2", strVal("Header", "BeginString"))
	assert.Equal(t, "D", strVal("Header", "MsgType"))
	assert.Equal(t, "TW", strVal("Header", "SenderCompID"))
	assert.Equal(t, "ISLD", strVal("Header", "TargetCompID"))
	assert.Equal(t, "TSLA", strVal("Body", "Symbol"))
	assert.Equal(t, "1", strVal("Body", "Side"))

	// CheckSum must NOT appear even with a DD.
	_, hasCheckSum := parsed["Trailer"]["CheckSum"]
	assert.False(t, hasCheckSum, "CheckSum should be excluded from JSON output")
}

// ----------------------------------------------------------------------------
// Round-trip: messageToJSON → messageFromJSON (no data dictionary)
// ----------------------------------------------------------------------------

func TestFromJSONNoDictionaryRoundTrip(t *testing.T) {
	orig := buildNewOrderSingle()

	jsonBytes, err := messageToJSON(orig, nil)
	require.NoError(t, err)

	restored := goquickfix.NewMessage()
	require.NoError(t, messageFromJSON(restored, jsonBytes, nil))

	// Header fields.
	bs, err := restored.Header.GetString(tagBeginString)
	require.NoError(t, err)
	assert.Equal(t, "FIX.4.2", bs)

	mt, err := restored.Header.GetString(tagMsgType)
	require.NoError(t, err)
	assert.Equal(t, "D", mt)

	// Body fields.
	sym, err := restored.Body.GetString(goquickfix.Tag(55))
	require.NoError(t, err)
	assert.Equal(t, "TSLA", sym)

	side, err := restored.Body.GetString(goquickfix.Tag(54))
	require.NoError(t, err)
	assert.Equal(t, "1", side)
}

// ----------------------------------------------------------------------------
// Round-trip: messageToJSON → messageFromJSON (with data dictionary, named keys)
// ----------------------------------------------------------------------------

func TestFromJSONWithDictionaryRoundTrip(t *testing.T) {
	dd := mustLoadDD(t, "testdata/FIX42.xml")

	orig := buildNewOrderSingle()

	jsonBytes, err := messageToJSON(orig, dd)
	require.NoError(t, err)

	// The JSON now has named keys; messageFromJSON should resolve them back to tags.
	restored := goquickfix.NewMessage()
	require.NoError(t, messageFromJSON(restored, jsonBytes, dd))

	sym, err := restored.Body.GetString(goquickfix.Tag(55))
	require.NoError(t, err)
	assert.Equal(t, "TSLA", sym)

	senderID, err := restored.Header.GetString(tagSenderCompID)
	require.NoError(t, err)
	assert.Equal(t, "TW", senderID)
}

// ----------------------------------------------------------------------------
// Repeating groups
// ----------------------------------------------------------------------------

func TestToJSONRepeatingGroups(t *testing.T) {
	dd := mustLoadDD(t, "testdata/FIX44.xml")

	// ExecutionReport (35=8) with two NoContraBrokers (tag 382) instances.
	msg := goquickfix.NewMessage()
	msg.Header.SetString(tagBeginString, "FIX.4.4")
	msg.Header.SetString(tagMsgType, "8")
	msg.Header.SetString(tagSenderCompID, "SENDER")
	msg.Header.SetString(tagTargetCompID, "TARGET")
	msg.Header.SetInt(goquickfix.Tag(34), 1)
	msg.Body.SetString(goquickfix.Tag(37), "ORD001")  // OrderID
	msg.Body.SetString(goquickfix.Tag(17), "EXEC001") // ExecID
	msg.Body.SetString(goquickfix.Tag(150), "0")      // ExecType
	msg.Body.SetString(goquickfix.Tag(39), "0")       // OrdStatus
	msg.Body.SetString(goquickfix.Tag(55), "AAPL")    // Symbol
	msg.Body.SetString(goquickfix.Tag(54), "1")       // Side
	msg.Body.SetString(goquickfix.Tag(151), "100")    // LeavesQty
	msg.Body.SetString(goquickfix.Tag(14), "0")       // CumQty
	msg.Body.SetString(goquickfix.Tag(6), "0")        // AvgPx

	// NoContraBrokers (tag 382) group – use FIX44 template from DD.
	msgDef, ok := dd.Messages["8"]
	require.True(t, ok, "ExecutionReport not found in FIX44 DD")
	grpFieldDef, ok := msgDef.Fields[382]
	require.True(t, ok, "NoContraBrokers (382) not in ExecutionReport")
	require.True(t, grpFieldDef.IsGroup(), "tag 382 should be a group")

	template := buildGroupTemplate(grpFieldDef)
	rg := goquickfix.NewRepeatingGroup(382, template)

	g1 := rg.Add()
	g1.SetString(goquickfix.Tag(375), "BRKR1") // ContraBroker
	g1.SetString(goquickfix.Tag(337), "ACC1")  // ContraTrader

	g2 := rg.Add()
	g2.SetString(goquickfix.Tag(375), "BRKR2")
	g2.SetString(goquickfix.Tag(337), "ACC2")

	msg.Body.SetGroup(rg)

	jsonBytes, err := messageToJSON(msg, dd)
	require.NoError(t, err)

	var parsed map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(jsonBytes, &parsed))

	var body map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(parsed["Body"], &body))

	// NoContraBrokers should be serialized as a JSON array.
	rawGrp, ok := body["NoContraBrokers"]
	require.True(t, ok, "NoContraBrokers not found in Body JSON")

	var grpArray []map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(rawGrp, &grpArray))
	require.Len(t, grpArray, 2)

	strFromRaw := func(raw json.RawMessage) string {
		var s string
		require.NoError(t, json.Unmarshal(raw, &s))
		return s
	}

	assert.Equal(t, "BRKR1", strFromRaw(grpArray[0]["ContraBroker"]))
	assert.Equal(t, "ACC1", strFromRaw(grpArray[0]["ContraTrader"]))
	assert.Equal(t, "BRKR2", strFromRaw(grpArray[1]["ContraBroker"]))
	assert.Equal(t, "ACC2", strFromRaw(grpArray[1]["ContraTrader"]))
}

func TestFromJSONRepeatingGroupsRoundTrip(t *testing.T) {
	dd := mustLoadDD(t, "testdata/FIX44.xml")

	// Build the source JSON directly.
	srcJSON := []byte(`{
		"Header": {
			"BeginString": "FIX.4.4",
			"MsgType":     "8",
			"SenderCompID":"SENDER",
			"TargetCompID":"TARGET",
			"MsgSeqNum":   "1"
		},
		"Body": {
			"OrderID":   "ORD001",
			"ExecID":    "EXEC001",
			"ExecType":  "0",
			"OrdStatus": "0",
			"Symbol":    "AAPL",
			"Side":      "1",
			"LeavesQty": "100",
			"CumQty":    "0",
			"AvgPx":     "0",
			"NoContraBrokers": [
				{"ContraBroker": "BRKR1", "ContraTrader": "ACC1"},
				{"ContraBroker": "BRKR2", "ContraTrader": "ACC2"}
			]
		},
		"Trailer": {}
	}`)

	msg := goquickfix.NewMessage()
	require.NoError(t, messageFromJSON(msg, srcJSON, dd))

	// Verify scalar fields.
	sym, err := msg.Body.GetString(goquickfix.Tag(55))
	require.NoError(t, err)
	assert.Equal(t, "AAPL", sym)

	// Verify repeating group.
	msgDef := dd.Messages["8"]
	grpFieldDef := msgDef.Fields[382]
	template := buildGroupTemplate(grpFieldDef)
	rg := goquickfix.NewRepeatingGroup(382, template)

	require.NoError(t, msg.Body.GetGroup(rg))
	require.Equal(t, 2, rg.Len())

	broker1, err := rg.Get(0).GetString(goquickfix.Tag(375))
	require.NoError(t, err)
	assert.Equal(t, "BRKR1", broker1)

	broker2, err := rg.Get(1).GetString(goquickfix.Tag(375))
	require.NoError(t, err)
	assert.Equal(t, "BRKR2", broker2)
}

// ----------------------------------------------------------------------------
// messageFromJSON error handling
// ----------------------------------------------------------------------------

func TestFromJSONInvalidJSON(t *testing.T) {
	msg := goquickfix.NewMessage()
	err := messageFromJSON(msg, []byte(`not json`), nil)
	assert.Error(t, err)
}
