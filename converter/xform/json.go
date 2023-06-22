package xform

import (
	"encoding/json"
	"net"

	"github.com/ridge/must/v2"
	"time"
)

// Marshal returns its argument encoded as json.RawMessage
func Marshal(v any) json.RawMessage {
	return must.OK1(json.Marshal(v))
}

// String decodes a json.RawMessage as a string
func String(m json.RawMessage) string {
	var v string
	must.OK(json.Unmarshal(m, &v))
	return v
}

// Int decodes a json.RawMessage as an int
func Int(m json.RawMessage) int {
	var v int
	must.OK(json.Unmarshal(m, &v))
	return v
}

// Bool decodes a json.RawMessage as a bool
func Bool(m json.RawMessage) bool {
	var v bool
	must.OK(json.Unmarshal(m, &v))
	return v
}

// Time decodes a json.RawMessage as a time
func Time(m json.RawMessage) time.Time {
	var v time.Time
	must.OK(json.Unmarshal(m, &v))
	return v
}

// IP decodes a json.RawMessage as a IP
func IP(m json.RawMessage) net.IP {
	var v net.IP
	must.OK(json.Unmarshal(m, &v))
	return v
}
