package wire

import "time"

// Header comes before the message body in kafka/local files and kafka/server
// streams.
//
// The general format is: JSON(Header), '\n', body, '\n'.
//
// Notes on Offset:
//
//   - It is filled by kafka/server to represent the sparse sequence of offsets.
//   - It is left out by kafka/local because several clients can append a file
//     concurrently.
//   - It is nevertheless respected by kafka/local when reading; when it's
//     missing, the offset of the previous record is incremented.
type Header struct {
	TS      time.Time
	Offset  *int64            `json:",omitempty"`
	Key     string            `json:",omitempty"`
	Headers map[string]string `json:",omitempty"`
	Len     int
}
