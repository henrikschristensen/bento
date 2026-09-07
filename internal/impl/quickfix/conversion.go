package quickfix

import (
	"bytes"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"

	goquickfix "github.com/quickfixgo/quickfix"
	"github.com/quickfixgo/quickfix/datadictionary"
)

const tagCheckSum goquickfix.Tag = 10

// fixToBentoPayload converts a received FIX message into the bento payload for
// the given message_format: a JSON object in "json" mode, the wire-format FIX
// string (SOH delimiters) in "raw" mode.
func fixToBentoPayload(m *goquickfix.Message, format string, dd *datadictionary.DataDictionary) ([]byte, error) {
	if format == "json" {
		return messageToJSON(m, dd)
	}
	return m.Bytes(), nil
}

// bentoToFIXMessage parses a bento payload in the given message_format into
// fixMsg: a JSON object with Header/Body/Trailer sections in "json" mode, a
// wire-format FIX string (SOH or pipe delimited) in "raw" mode.
func bentoToFIXMessage(fixMsg *goquickfix.Message, data []byte, format string, dd *datadictionary.DataDictionary) error {
	if format == "json" {
		return messageFromJSON(fixMsg, data, dd)
	}
	return parseRawFIX(fixMsg, data, dd)
}

// normaliseSOH converts a pipe-delimited (human-readable) FIX wire string to
// SOH delimiters. Payloads already containing SOH delimiters are returned
// unchanged.
func normaliseSOH(raw []byte) []byte {
	if bytes.ContainsRune(raw, '|') && !bytes.ContainsRune(raw, '\x01') {
		return bytes.ReplaceAll(raw, []byte("|"), []byte("\x01"))
	}
	return raw
}

// parseRawFIX parses a raw FIX wire string (SOH or pipe delimited) into
// fixMsg. When a data dictionary is available the message is parsed with it so
// repeating groups (e.g. NoSides in a TradeCaptureReport) are recognized and
// preserved; without it, group members would be treated as flat body fields
// and reordered by tag on send, destroying the group structure for the
// receiver.
func parseRawFIX(fixMsg *goquickfix.Message, raw []byte, dd *datadictionary.DataDictionary) error {
	raw = normaliseSOH(raw)
	if dd != nil {
		return goquickfix.ParseMessageWithDataDictionary(fixMsg, bytes.NewBuffer(raw), nil, dd)
	}
	return goquickfix.ParseMessage(fixMsg, bytes.NewBuffer(raw))
}

// messageToJSON serializes the Message to the FIX JSON Encoding format.
// See: https://github.com/FIXTradingCommunity/fix-json-encoding-spec
//
// If dd is nil, numeric tag numbers are used as field names and repeating groups
// are serialized as a flat string rather than as JSON arrays.
func messageToJSON(m *goquickfix.Message, dd *datadictionary.DataDictionary) ([]byte, error) {
	msgType, _ := m.MsgType()

	var headerFields, bodyFields, trailerFields map[int]*datadictionary.FieldDef
	if dd != nil {
		if dd.Header != nil {
			headerFields = dd.Header.Fields
		}
		if dd.Trailer != nil {
			trailerFields = dd.Trailer.Fields
		}
		if msgType != "" {
			if msgDef, ok := dd.Messages[msgType]; ok {
				bodyFields = msgDef.Fields
			}
		}
	}

	var buf bytes.Buffer
	buf.WriteString(`{"Header":`)
	writeFieldMapJSON(&buf, &m.Header.FieldMap, headerFields, dd)
	buf.WriteString(`,"Body":`)
	writeFieldMapJSON(&buf, &m.Body.FieldMap, bodyFields, dd)
	buf.WriteString(`,"Trailer":`)
	writeFieldMapJSON(&buf, &m.Trailer.FieldMap, trailerFields, dd)
	buf.WriteByte('}')

	return buf.Bytes(), nil
}

// messageFromJSON populates the Message from a FIX JSON Encoding byte slice.
// See: https://github.com/FIXTradingCommunity/fix-json-encoding-spec
//
// If dd is nil, field names must be numeric tag numbers and repeating groups
// are not supported.
func messageFromJSON(m *goquickfix.Message, data []byte, dd *datadictionary.DataDictionary) error {
	var raw struct {
		Header  json.RawMessage `json:"Header"`
		Body    json.RawMessage `json:"Body"`
		Trailer json.RawMessage `json:"Trailer"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return fmt.Errorf("error parsing FIX JSON message: %w", err)
	}

	var headerFields map[int]*datadictionary.FieldDef
	if dd != nil && dd.Header != nil {
		headerFields = dd.Header.Fields
	}

	if err := populateFieldMapFromJSON(&m.Header.FieldMap, raw.Header, headerFields, dd); err != nil {
		return fmt.Errorf("error parsing FIX JSON Header: %w", err)
	}

	// Parse MsgType from the header to determine the body field context.
	msgType, _ := m.MsgType()

	var trailerFields, bodyFields map[int]*datadictionary.FieldDef
	if dd != nil {
		if dd.Trailer != nil {
			trailerFields = dd.Trailer.Fields
		}
		if msgType != "" {
			if msgDef, ok := dd.Messages[msgType]; ok {
				bodyFields = msgDef.Fields
			}
		}
	}

	if err := populateFieldMapFromJSON(&m.Body.FieldMap, raw.Body, bodyFields, dd); err != nil {
		return fmt.Errorf("error parsing FIX JSON Body: %w", err)
	}
	if err := populateFieldMapFromJSON(&m.Trailer.FieldMap, raw.Trailer, trailerFields, dd); err != nil {
		return fmt.Errorf("error parsing FIX JSON Trailer: %w", err)
	}

	return nil
}

// writeFieldMapJSON writes a FieldMap's fields as a JSON object into buf.
// contextFields (if non-nil) identifies which tags represent repeating groups in this message context.
func writeFieldMapJSON(buf *bytes.Buffer, fm *goquickfix.FieldMap, contextFields map[int]*datadictionary.FieldDef, dd *datadictionary.DataDictionary) {
	tags := fm.Tags()
	slices.Sort(tags)

	buf.WriteByte('{')
	first := true

	for _, tag := range tags {
		if tag == tagCheckSum {
			continue
		}

		name := fieldNameOrTag(tag, dd)

		var groupDef *datadictionary.FieldDef
		if contextFields != nil {
			if fd, ok := contextFields[int(tag)]; ok && fd.IsGroup() {
				groupDef = fd
			}
		}

		var value []byte
		if groupDef == nil {
			var err error
			if value, err = fm.GetBytes(tag); err != nil {
				continue
			}
		}

		if !first {
			buf.WriteByte(',')
		}
		first = false

		// Write JSON key.
		nameBytes, _ := json.Marshal(name)
		buf.Write(nameBytes)
		buf.WriteByte(':')

		if groupDef != nil {
			writeGroupJSON(buf, fm, tag, groupDef, dd)
		} else {
			valBytes, _ := json.Marshal(string(value))
			buf.Write(valBytes)
		}
	}

	buf.WriteByte('}')
}

// writeGroupJSON serializes a repeating-group field as a JSON array of objects.
// The group is read out of fm using the template derived from the data dictionary.
func writeGroupJSON(buf *bytes.Buffer, fm *goquickfix.FieldMap, tag goquickfix.Tag, groupDef *datadictionary.FieldDef, dd *datadictionary.DataDictionary) {
	rg := goquickfix.NewRepeatingGroup(tag, buildGroupTemplate(groupDef))
	if err := fm.GetGroup(rg); err != nil {
		buf.WriteString("[]")
		return
	}

	subFields := groupSubFields(groupDef)

	buf.WriteByte('[')
	for i := 0; i < rg.Len(); i++ {
		if i > 0 {
			buf.WriteByte(',')
		}
		writeFieldMapJSON(buf, &rg.Get(i).FieldMap, subFields, dd)
	}
	buf.WriteByte(']')
}

// populateFieldMapFromJSON reads fields from a JSON object and sets them on fm.
// contextFields (if non-nil) identifies which tags are repeating groups in this context.
func populateFieldMapFromJSON(fm *goquickfix.FieldMap, data json.RawMessage, contextFields map[int]*datadictionary.FieldDef, dd *datadictionary.DataDictionary) error {
	if len(data) == 0 {
		return nil
	}

	var rawFields map[string]json.RawMessage
	if err := json.Unmarshal(data, &rawFields); err != nil {
		return err
	}

	for name, rawValue := range rawFields {
		tag, err := resolveTag(name, dd)
		if err != nil {
			continue // skip unknown fields
		}

		// JSON arrays represent repeating groups.
		if len(rawValue) > 0 && rawValue[0] == '[' {
			var groupDef *datadictionary.FieldDef
			if contextFields != nil {
				if fd, ok := contextFields[int(tag)]; ok && fd.IsGroup() {
					groupDef = fd
				}
			}
			if groupDef == nil {
				continue // skip groups we cannot parse without a definition
			}

			var items []json.RawMessage
			if err := json.Unmarshal(rawValue, &items); err != nil {
				return fmt.Errorf("error parsing group %q: %w", name, err)
			}

			subFields := groupSubFields(groupDef)
			rg := goquickfix.NewRepeatingGroup(tag, buildGroupTemplate(groupDef))
			for _, item := range items {
				grp := rg.Add()
				if err := populateFieldMapFromJSON(&grp.FieldMap, item, subFields, dd); err != nil {
					return err
				}
			}
			fm.SetGroup(rg)
		} else {
			var value string
			if err := json.Unmarshal(rawValue, &value); err != nil {
				return fmt.Errorf("error parsing field %q value: %w", name, err)
			}
			fm.SetBytes(tag, []byte(value))
		}
	}

	return nil
}

// buildGroupTemplate constructs a GroupTemplate from a group FieldDef, handling nested groups recursively.
func buildGroupTemplate(groupDef *datadictionary.FieldDef) goquickfix.GroupTemplate {
	template := make(goquickfix.GroupTemplate, 0, len(groupDef.Fields))
	for _, childDef := range groupDef.Fields {
		if childDef.IsGroup() {
			template = append(template, goquickfix.NewRepeatingGroup(goquickfix.Tag(childDef.Tag()), buildGroupTemplate(childDef)))
		} else {
			template = append(template, goquickfix.GroupElement(goquickfix.Tag(childDef.Tag())))
		}
	}
	return template
}

// groupSubFields builds a tag→FieldDef map for the direct children of a group FieldDef.
func groupSubFields(groupDef *datadictionary.FieldDef) map[int]*datadictionary.FieldDef {
	sub := make(map[int]*datadictionary.FieldDef, len(groupDef.Fields))
	for _, fd := range groupDef.Fields {
		sub[fd.Tag()] = fd
	}
	return sub
}

// fieldNameOrTag returns the human-readable field name from the data dictionary,
// or the numeric tag string when the dictionary is absent or the tag is unknown.
func fieldNameOrTag(tag goquickfix.Tag, dd *datadictionary.DataDictionary) string {
	if dd != nil {
		if ft, ok := dd.FieldTypeByTag[int(tag)]; ok {
			return ft.Name()
		}
	}
	return strconv.Itoa(int(tag))
}

// resolveTag maps a JSON field name to a FIX Tag.
// It first checks the data dictionary by name, then falls back to parsing a numeric string.
func resolveTag(name string, dd *datadictionary.DataDictionary) (goquickfix.Tag, error) {
	if dd != nil {
		if ft, ok := dd.FieldTypeByName[name]; ok {
			return goquickfix.Tag(ft.Tag()), nil
		}
	}
	n, err := strconv.Atoi(name)
	if err != nil {
		return 0, fmt.Errorf("unknown field %q", name)
	}
	return goquickfix.Tag(n), nil
}
