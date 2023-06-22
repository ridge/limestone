package formatter

import "go.uber.org/zap/buffer"

type color string

// See https://en.wikipedia.org/wiki/ANSI_escape_code#SGR_(Select_Graphic_Rendition)_parameters
const (
	reset          color = "\x1b[0m"
	bold           color = "\x1b[1m"
	italic         color = "\x1b[3m"
	fgRed          color = "\x1b[31m"
	fgGreen        color = "\x1b[32m"
	fgYellow       color = "\x1b[33m"
	fgBlue         color = "\x1b[34m"
	fgMagenta      color = "\x1b[35m"
	fgDimWhite     color = "\x1b[37m"
	fgGray         color = "\x1b[90m"
	fgBrightYellow color = "\x1b[93m"
)

const (
	debugColor                = fgMagenta
	infoColor                 = fgBlue
	warnColor                 = fgYellow
	errorColor                = fgRed
	fieldColor                = fgGreen
	subFieldColor             = fgBlue
	messageColor              = bold
	callerColor               = italic
	deemphasizedDatePartColor = fgGray
	commonDatePartColor       = fgBlue
	objectPunctuationColor    = fgYellow
	arrayPunctuationColor     = fgBrightYellow
)

type styleFn func(*buffer.Buffer, color, string)

var consoleStyles = map[bool]styleFn{
	true: func(buffer *buffer.Buffer, color color, text string) {
		if text != "" {
			buffer.AppendString(string(color))
			buffer.AppendString(text)
			buffer.AppendString(string(reset))
		}
	},
	false: func(buffer *buffer.Buffer, color color, text string) {
		buffer.AppendString(text)
	},
}
