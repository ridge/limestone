package formatter

import (
	"encoding/json"
	"testing"

	"github.com/ridge/must/v2"
	"github.com/ridge/tj"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/buffer"
)

// These tests overconstrain implementation a bit, so feel free to adjust them

func TestFormat(t *testing.T) {
	logMessage := tj.O{
		"ts":         "2000-01-01T00:00:00.123Z",
		"level":      "info",
		"caller":     "foo.go:42",
		"msg":        "Hello World",
		"logger":     "main.sub.subsub",
		"error":      "whoa\nwhoa\nwhoa",
		"stacktrace": "goroutine 1 [running]:\n\tmain.main()",
		"stack":      "goroutine 42 [running]:\n\tsecondary.secondary()",
		"obj":        tj.O{"arr": tj.A{"str", "2000-01-01T00:00:00Z"}},
		"float":      1.5,
		"bool":       true,
		"null":       nil,
		"arr":        tj.A{1.5, true, nil, "foo"},
		"mls":        "a\nb\nc\n",
		"mlsnt":      "a\nb",
	}

	expected := `2000-01-01T00:00:00.123Z INF Hello World arr=[1.5, true, null, "foo"] bool=true float=1.5 null=null obj={arr: ["str", 2000-01-01T00:00:00Z]} [main.sub.subsub] (foo.go:42)
----- error -----
whoa
whoa
whoa
----- mls -----
a
b
c
----- mlsnt -----
a
b
----- stack -----
goroutine 42 [running]:
	secondary.secondary()
----- stacktrace -----
goroutine 1 [running]:
	main.main()
----------
`

	buffer, ts, err := JSONLogMessage(must.OK1(json.Marshal(logMessage)), "", false)
	require.NoError(t, err)
	defer buffer.Free()

	assert.Equal(t, expected, buffer.String())
	assert.Equal(t, "2000-01-01T00:00:00.123Z", ts)
}

func TestFormatNoLogger(t *testing.T) {
	logMessage := tj.O{
		"ts":     "2000-01-01T00:00:00Z",
		"level":  "error",
		"caller": "no.go",
		"msg":    "no logger",
	}

	expected := "2000-01-01T00:00:00Z ERR no logger [] (no.go)\n"

	buffer, _, err := JSONLogMessage(must.OK1(json.Marshal(logMessage)), "", false)
	require.NoError(t, err)
	defer buffer.Free()

	assert.Equal(t, expected, buffer.String())
}

func TestFormatTimestamp(t *testing.T) {
	pool := buffer.NewPool()

	deemT := string(deemphasizedDatePartColor + "T" + reset)
	deemZ := string(deemphasizedDatePartColor + "Z" + reset)

	for date, formatted := range map[string]string{
		"2000-01-01T00:00:00Z":             "2000-01-01" + deemT + "00:00:00" + deemZ,
		"2000-01-01T00:00:00.0Z":           "2000-01-01" + deemT + "00:00:00.0" + deemZ,
		"2000-01-01T00:00:00.000000Z":      "2000-01-01" + deemT + "00:00:00.000000" + deemZ,
		"2000-01-01T00:00:00.000000+01:00": "2000-01-01" + deemT + "00:00:00.000000+01:00",
	} {
		buffer := pool.Get()
		formatTimestamp(buffer, date, consoleStyles[true], "")
		assert.Equal(t, formatted, buffer.String())
		buffer.Free()
	}
}

func TestFormatMultilineString(t *testing.T) {
	pool := buffer.NewPool()

	for multiline, formatted := range map[string]string{
		"hello\nworld\n": string(fieldColor) + "----- multilinekey -----" + string(reset) + `
hello
world
`,
		"hello\nworld": string(fieldColor) + "----- multilinekey -----" + string(reset) + `
hello
world
`,
	} {
		buffer := pool.Get()
		formatMultilineString(buffer, "multilinekey", multiline, consoleStyles[true], fieldColor)
		assert.Equal(t, formatted, buffer.String())
		buffer.Free()
	}
}

func TestFormatWithPreviousDate(t *testing.T) {
	c := string(commonDatePartColor)
	d := string(deemphasizedDatePartColor)
	r := string(reset)

	pool := buffer.NewPool()

	for prevDate, formatted := range map[string]string{
		// full match
		"2000-01-01T00:00:00.000000Z": c + "2000-01-01" + r + d + "T" + r + c + "00:00:00.000000" + r + d + "Z" + r,
		// match up to time
		"2000-01-01T00:00:00.000000+00:00": c + "2000-01-01" + r + d + "T" + r + c + "00:00:00.000000" + r + d + "Z" + r,
		// match up to part of time
		"2000-01-01T00:00:00.000123Z": c + "2000-01-01" + r + d + "T" + r + c + "00:00:00.000" + r + "000" + d + "Z" + r,
		// match up to date+T
		"2000-01-01T11:22:33.445566Z": c + "2000-01-01" + r + d + "T" + r + "00:00:00.000000" + d + "Z" + r,
		// match up to date
		"2000-01-01U11:22:33.445566Z": c + "2000-01-01" + r + d + "T" + r + "00:00:00.000000" + d + "Z" + r,
		// match up to part of date
		"2000-01-12T00:00:00.000000Z": c + "2000-01-" + r + "01" + d + "T" + r + "00:00:00.000000" + d + "Z" + r,
		"1999-01-01T00:00:00.000000Z": "2000-01-01" + d + "T" + r + "00:00:00.000000" + d + "Z" + r,
	} {
		t.Run(prevDate, func(t *testing.T) {
			buffer := pool.Get()
			formatTimestamp(buffer, "2000-01-01T00:00:00.000000Z", consoleStyles[true], prevDate)
			assert.Equal(t, formatted, buffer.String())
			buffer.Free()
		})
	}
}
