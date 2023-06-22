package meta

import (
	"reflect"
	"strings"
)

type option struct {
	key   string // includes trailing '=' for options with values
	value string
}

func (opt option) String() string {
	return opt.key + opt.value
}

func parseTag(tag reflect.StructTag) []option {
	str, ok := tag.Lookup("limestone")
	if !ok {
		return nil
	}

	parts := strings.Split(str, ",")
	options := make([]option, 0, len(parts))
	for _, part := range parts {
		if key, value, ok := strings.Cut(part, "="); ok {
			options = append(options, option{key: key + "=", value: value})
		} else {
			options = append(options, option{key: key})
		}
	}
	return options
}
