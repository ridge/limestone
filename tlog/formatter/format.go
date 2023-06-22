package formatter

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"go.uber.org/zap/buffer"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
)

// This code tries hard to avoid failing and thus obscuring a part of log message.
// It will warn, scream, misformat, but it will always return all the data that
// was present in original message.

// Extracts the key by removing it from the parsedMsg. If the key is not
// of string type, it is converted to a string forcibly with a warning.
func maybeExtractSpecialKey(parsedMsg map[string]any, key string) (string, bool) {
	val, ok := parsedMsg[key]
	if !ok {
		return "", false
	}

	delete(parsedMsg, key)
	strVal, ok := val.(string)
	if !ok {
		return fmt.Sprintf("<MALFORMED %v OF TYPE %T>", val, val), true
	}

	return strVal, true
}

// Extracts the key by removing it from the parsedMsg. If the key is not
// of string type, it is converted to a string forcibly with a warning.
// If the key is missing, the warning message is returned too.
func extractSpecialKey(parsedMsg map[string]any, key string) string {
	val, ok := maybeExtractSpecialKey(parsedMsg, key)
	if !ok {
		return "<MISSING " + key + ">"
	}
	return val
}

var bufferPool = buffer.NewPool()

var errNotOurs = errors.New("JSON log message in a foreign format")

// JSONLogMessage formats a single JSON-formatted log message
//
// prevTimestamp is passed to selectively deemphasize the parts of
// the current timestamp that haven't changed since the last log message
func JSONLogMessage(logMessage []byte, prevTimestamp string, color bool) (retOutput *buffer.Buffer, retTimestamp string, retError error) {
	var parsedMsg map[string]any
	if err := json.Unmarshal(logMessage, &parsedMsg); err != nil {
		return nil, "", err
	}

	// Some logs may be JSON-formatted but coming from other services
	// that do not use zap conventions. Treat them as non-JSON.
	if _, ok := parsedMsg["ts"]; !ok {
		return nil, "", errNotOurs
	}

	// Remove old pre-formatted text field. Do not remove this line
	// until all logs with this field are purged from log storage.
	delete(parsedMsg, "asText")

	// Remove auxiliary fields added by the log pipeline.
	// FIXME (misha): Fix the pipeline not to add these fields in the first place.
	delete(parsedMsg, "@timestamp")
	delete(parsedMsg, "@version")
	delete(parsedMsg, "agent")
	delete(parsedMsg, "input")
	delete(parsedMsg, "log")
	delete(parsedMsg, "tags")
	delete(parsedMsg, "ecs")
	delete(parsedMsg, "host")

	// special keys
	level := extractSpecialKey(parsedMsg, "level")
	ts := extractSpecialKey(parsedMsg, "ts")
	caller := extractSpecialKey(parsedMsg, "caller")
	msg := extractSpecialKey(parsedMsg, "msg")

	logger, _ := maybeExtractSpecialKey(parsedMsg, "logger")
	errorStr, haveError := maybeExtractSpecialKey(parsedMsg, "error")
	multilineError := haveError && strings.Contains(errorStr, "\n")

	buf := bufferPool.Get()
	style := consoleStyles[color]

	if dateTimeRx.MatchString(ts) {
		formatTimestamp(buf, ts, style, prevTimestamp)
	} else {
		formatString(buf, ts)
	}
	buf.AppendByte(' ')
	formatLevel(buf, level, style)

	buf.AppendByte(' ')
	style(buf, messageColor, msg)

	var multilineFields []string

	parsedMsgKeys := maps.Keys(parsedMsg)
	slices.Sort(parsedMsgKeys)

	for _, field := range parsedMsgKeys {
		if v, ok := parsedMsg[field].(string); ok && strings.Contains(v, "\n") {
			multilineFields = append(multilineFields, field)
			continue
		}

		buf.AppendByte(' ')
		style(buf, fieldColor, field+"=")
		formatValue(buf, parsedMsg[field], style)
	}

	// Error is put last, unless it is multiline
	if haveError && !multilineError {
		buf.AppendByte(' ')
		style(buf, errorColor, "error=")
		formatString(buf, errorStr)
	}

	buf.AppendString(" [")
	buf.AppendString(logger)
	buf.AppendString("] (")
	style(buf, callerColor, caller)
	buf.AppendString(")\n")

	if multilineError {
		formatMultilineString(buf, "error", errorStr, style, errorColor)
	}

	for _, field := range multilineFields {
		formatMultilineString(buf, field, parsedMsg[field].(string), style, fieldColor)
	}

	if multilineError || len(multilineFields) > 0 {
		style(buf, fieldColor, "----------")
		buf.AppendByte('\n')
	}

	return buf, ts, nil
}

func formatLevel(buf *buffer.Buffer, level string, style styleFn) {
	switch level {
	case "debug":
		style(buf, debugColor, "DBG")
	case "info":
		style(buf, infoColor, "INF")
	case "warn":
		style(buf, warnColor, "WRN")
	default:
		style(buf, errorColor, strings.ToUpper(level)[:3])
	}
}

func formatValue(buf *buffer.Buffer, value any, style styleFn) {
	switch v := value.(type) {
	case float64:
		fmt.Fprintf(buf, "%.22g", v) // Print integers as integers
	case bool:
		fmt.Fprintf(buf, "%#v", v)
	case nil:
		fmt.Fprint(buf, "null")
	case string:
		formatTimestampOrString(buf, v, style)
	case map[string]any:
		formatMap(buf, v, style)
	case []any:
		formatArray(buf, v, style)
	default:
		panic("unreachable")
	}
}

func formatMap(buf *buffer.Buffer, value map[string]any, style styleFn) {
	style(buf, objectPunctuationColor, "{")
	valueKeys := maps.Keys(value)
	slices.Sort(valueKeys)
	for i, field := range valueKeys {
		if i > 0 {
			style(buf, objectPunctuationColor, ", ")
		}
		style(buf, subFieldColor, field)
		style(buf, objectPunctuationColor, ":")
		buf.AppendByte(' ')
		formatValue(buf, value[field], style)
	}
	style(buf, objectPunctuationColor, "}")
}

func formatArray(buf *buffer.Buffer, value []any, style styleFn) {
	style(buf, arrayPunctuationColor, "[")
	for i, val := range value {
		if i > 0 {
			style(buf, arrayPunctuationColor, ", ")
		}
		formatValue(buf, val, style)
	}
	style(buf, arrayPunctuationColor, "]")
}

var dateTimeRx = regexp.MustCompile(`^(\d{4}-\d{2}-\d{2})T(\d{2}:\d{2}:\d{2}(?:.\d+)?)(Z|[+-]\d{2}:\d{2})$`)

func commonPrefixLength(a, b string) int {
	for i := 0; i < len(a) && i < len(b); i++ {
		if a[i] != b[i] {
			return i
		}
	}
	if len(a) < len(b) {
		return len(a)
	}
	return len(b)
}

func formatTimestamp(buf *buffer.Buffer, ts string, style styleFn, previousTS string) {
	m := dateTimeRx.FindStringSubmatch(ts)
	cpl := commonPrefixLength(ts, previousTS)

	// date
	common, unique := splitAt(m[1], cpl)
	style(buf, commonDatePartColor, common)
	buf.AppendString(unique)

	// T
	style(buf, deemphasizedDatePartColor, "T")

	// time
	common, unique = splitAt(m[2], cpl-len(m[1])-1)
	style(buf, commonDatePartColor, common)
	buf.AppendString(unique)

	// timezone
	if m[3] == "Z" {
		style(buf, deemphasizedDatePartColor, "Z")
	} else {
		// Non-UTC timezones are not expected, so they should not be deemphasized
		// Do not deemphasize non-UTC timezones, they are not expected here
		buf.AppendString(m[3])
	}
}

func splitAt(s string, pos int) (before, after string) {
	if pos <= 0 {
		return "", s
	}
	if pos >= len(s) {
		return s, ""
	}
	return s[:pos], s[pos:]
}

func formatTimestampOrString(buf *buffer.Buffer, s string, style styleFn) {
	if dateTimeRx.MatchString(s) {
		formatTimestamp(buf, s, style, "")
	} else {
		formatString(buf, s)
	}
}

func formatString(buf *buffer.Buffer, s string) {
	if strings.Contains(s, `"`) || strings.Contains(s, `\`) {
		fmt.Fprintf(buf, "%#q", s)
		return
	}
	fmt.Fprintf(buf, "%q", s)
}

func formatMultilineString(buf *buffer.Buffer, field string, value string, style styleFn, color color) {
	style(buf, color, "----- "+field+" -----")
	buf.AppendByte('\n')
	buf.AppendString(value)
	if !strings.HasSuffix(value, "\n") {
		buf.AppendByte('\n')
	}
}
