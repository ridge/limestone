package meta

import (
	"fmt"
	"reflect"
	"strings"
)

func panicf(format string, a ...any) {
	panic(fmt.Sprintf(format, a...))
}

func (s *Struct) setDBName(dbName string) {
	if dbName == "" || s.DBName == dbName {
		return
	}
	if s.DBName != "" {
		panicf("conflicting DB name settings for struct %v: %s vs. %s", s.Type, s.DBName, dbName)
	}
	s.DBName = dbName
}

// Survey produces a Struct describing a type
func Survey(t reflect.Type) Struct {
	s := surveyInternal(t, true, true)
	if s.DBName == "" {
		panicf("missing struct-level DB name setting in struct %v", s.Type)
	}
	if s.identity == noIdentity {
		panicf("missing identity field in struct %v", s)
	}
	return s
}

// SurveyOld produces a Struct describing a type,
// ignoring all sections with upgrade=old
func SurveyOld(t reflect.Type) Struct {
	return surveyInternal(t, true, false)
}

// SurveyNew produces a Struct describing a type,
// ignoring all sections with upgrade=new
func SurveyNew(t reflect.Type) Struct {
	return surveyInternal(t, false, true)
}

func surveyInternal(t reflect.Type, includeOld, includeNew bool) Struct {
	if t.Kind() != reflect.Struct {
		panicf("%v expected to be a struct type", t)
	}

	n := t.NumField()
	s := Struct{
		Type:     t,
		Fields:   make([]Field, 0, n),
		identity: noIdentity,
	}
	dbNames := map[string]int{} // holds indexes into s.Fields
	var producer string

loop:
	for i := 0; i < n; i++ {
		f := t.Field(i)
		options := parseTag(f.Tag)
		switch {
		case f.Type == metaType:
			for _, opt := range options {
				switch opt.key {
				case "name=":
					s.setDBName(opt.value)
				case "producer=":
					if producer != "" && producer != opt.value {
						panicf("conflicting producer settings for %v: %s vs. %s", t, producer, opt.value)
					}
					producer = opt.value
				case "upgrade=":
					switch opt.value {
					case "old":
						if !includeOld {
							s.Fields = nil
							return s
						}
					case "new":
						if !includeNew {
							s.Fields = nil
							return s
						}
					case "common": // explicit name for default behavior
					default:
						panicf("invalid struct-level option for %v: %s", t, opt)
					}
				default:
					panicf("invalid struct-level option for %v: %s", t, opt)
				}
			}

		case f.Anonymous:
			allConst := false
			for _, opt := range options {
				switch opt.key {
				case "-":
					if len(options) != 1 {
						panicf("option - for field %v.%s cannot be combined with other options", t, f.Name)
					}
					continue loop
				case "const":
					allConst = true
				default:
					panicf("invalid option for %v.%s: %s", t, f.Name, opt)
				}
			}
			sub := surveyInternal(f.Type, includeOld, includeNew)
			s.setDBName(sub.DBName)
			if sub.identity != noIdentity {
				if s.identity != noIdentity {
					panicf("duplicate identity fields %v.%s and %v.%s",
						t, s.Fields[s.identity], t, sub.Fields[sub.identity])
				}
				s.identity = len(s.Fields) + sub.identity
			}
			for _, field := range sub.Fields {
				field.Index = append([]int{i}, field.Index...)
				if idx, ok := dbNames[field.DBName]; ok {
					panicf("duplicate DB name %s for fields %v.%s and %v.%s",
						field.DBName, t, s.Fields[idx], t, field)
				}
				dbNames[field.DBName] = len(s.Fields)
				if allConst {
					field.Const = true
				}
				s.Fields = append(s.Fields, field)
			}

		default:
			field := Field{
				GoName: f.Name,
				Index:  f.Index,
				Type:   f.Type,
			}
			for _, opt := range options {
				switch opt.key {
				case "-":
					if len(options) != 1 {
						panicf("option - for field %v.%s cannot be combined with other options", t, field)
					}
					continue loop
				case "name=":
					// To be compatible with Mongo we disallow dots in field names and
					// initial $
					if strings.Contains(opt.value, ".") {
						panicf("%v.%s: dots are not allowed in Kafka field names", t, field)
					}
					if strings.HasPrefix(opt.value, "$") {
						panicf("%v.%s: Kafka field name is not allowed to start with $",
							t, field)
					}
					field.DBName = opt.value
				case "const":
					field.Const = true
				case "required":
					field.Required = true
				case "identity":
					if s.identity != noIdentity {
						panicf("duplicate identity fields %v.%s and %v.%s",
							t, s.Fields[s.identity], t, field)
					}
					if f.Type.Kind() != reflect.String {
						panicf("identity field %v.%s must be string-based", t, field)
					}
					field.Const = true
					field.Required = true
					s.identity = len(s.Fields)
				default:
					panicf("invalid option for %v.%s: %s", t, field, opt)
				}
			}
			if !f.IsExported() {
				panicf("unexported field %v.%s must be skipped using a `limestone:\"-\"` tag", t, field)
			}
			if field.DBName == "" {
				field.DBName = field.GoName
			}
			if idx, ok := dbNames[field.DBName]; ok {
				panicf("duplicate DB name %s for fields %v.%s and %v.%s",
					field.DBName, t, s.Fields[idx], t, field)
			}
			dbNames[field.DBName] = len(s.Fields)
			s.Fields = append(s.Fields, field)
		}
	}

	if producer != "" {
		producers := ParseProducerSet(producer)
		for i := range s.Fields {
			field := &s.Fields[i]
			if field.Producers != nil && !reflect.DeepEqual(field.Producers, producers) {
				panicf("conflicting producer settings for field %v.%s: %s vs. %s",
					t, field, field.Producers, producers)
			}
			field.Producers = producers
		}
	}

	return s
}
