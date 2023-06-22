package meta

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSurveySimple(t *testing.T) {
	type FooID string
	type Foo struct {
		Meta               `limestone:"name=foo"`
		ID                 FooID `limestone:"identity"`
		Field              int
		ConstField         int `limestone:"const"`
		RequiredField      int `limestone:"required"`
		ConstRequiredField int `limestone:"const,required"`
		RenamedField       int `limestone:"name=SomethingElse"`
		SkippedField       int `limestone:"-"`
	}
	require.Equal(t, Struct{
		DBName: "foo",
		Type:   reflect.TypeOf(Foo{}),
		Fields: []Field{
			{
				GoName:   "ID",
				DBName:   "ID",
				Index:    []int{1},
				Type:     reflect.TypeOf(FooID("")),
				Const:    true,
				Required: true,
			},
			{
				GoName: "Field",
				DBName: "Field",
				Index:  []int{2},
				Type:   reflect.TypeOf(0),
			},
			{
				GoName: "ConstField",
				DBName: "ConstField",
				Index:  []int{3},
				Type:   reflect.TypeOf(0),
				Const:  true,
			},
			{
				GoName:   "RequiredField",
				DBName:   "RequiredField",
				Index:    []int{4},
				Type:     reflect.TypeOf(0),
				Required: true,
			},
			{
				GoName:   "ConstRequiredField",
				DBName:   "ConstRequiredField",
				Index:    []int{5},
				Type:     reflect.TypeOf(0),
				Const:    true,
				Required: true,
			},
			{
				GoName: "RenamedField",
				DBName: "SomethingElse",
				Index:  []int{6},
				Type:   reflect.TypeOf(0),
			},
		},
		identity: 0,
	}, Survey(reflect.TypeOf(Foo{})))
}

func TestSurveyNested(t *testing.T) {
	type FooID string
	type Foo struct {
		Meta `limestone:"name=foo_bar,producer=p1"`
		ID   FooID `limestone:"identity"`
	}
	type Bar struct {
		Meta  `limestone:"name=foo_bar,producer=p2"`
		Field int
	}
	type foobar struct {
		Bar
		Foo
	}
	require.Equal(t, Struct{
		DBName: "foo_bar",
		Type:   reflect.TypeOf(foobar{}),
		Fields: []Field{
			{
				GoName:    "Field",
				DBName:    "Field",
				Index:     []int{0, 1},
				Type:      reflect.TypeOf(0),
				Producers: ProducerSet{"p2": true},
			},
			{
				GoName:    "ID",
				DBName:    "ID",
				Index:     []int{1, 1},
				Type:      reflect.TypeOf(FooID("")),
				Const:     true,
				Required:  true,
				Producers: ProducerSet{"p1": true},
			},
		},
		identity: 1,
	}, Survey(reflect.TypeOf(foobar{})))
}

func TestSurveyEmbedded(t *testing.T) {
	type FooBarID string
	type Foo struct {
		FooField      int
		FooConstField int `limestone:"const"`
	}
	type Bar struct {
		BarField      int
		BarConstField int `limestone:"const"`
	}
	type Skipped struct {
		SkippedField int
	}
	type foobar struct {
		Meta `limestone:"name=foo_bar"`
		ID   FooBarID `limestone:"identity"`
		Foo
		Bar     `limestone:"const"`
		Skipped `limestone:"-"`
	}
	require.Equal(t, Struct{
		DBName: "foo_bar",
		Type:   reflect.TypeOf(foobar{}),
		Fields: []Field{
			{
				GoName:   "ID",
				DBName:   "ID",
				Index:    []int{1},
				Type:     reflect.TypeOf(FooBarID("")),
				Const:    true,
				Required: true,
			},
			{
				GoName: "FooField",
				DBName: "FooField",
				Index:  []int{2, 0},
				Type:   reflect.TypeOf(0),
			},
			{
				GoName: "FooConstField",
				DBName: "FooConstField",
				Index:  []int{2, 1},
				Type:   reflect.TypeOf(0),
				Const:  true,
			},
			{
				GoName: "BarField",
				DBName: "BarField",
				Index:  []int{3, 0},
				Type:   reflect.TypeOf(0),
				Const:  true,
			},
			{
				GoName: "BarConstField",
				DBName: "BarConstField",
				Index:  []int{3, 1},
				Type:   reflect.TypeOf(0),
				Const:  true,
			},
		},
		identity: 0,
	}, Survey(reflect.TypeOf(foobar{})))
}

func TestSurveyInvalidType(t *testing.T) {
	require.PanicsWithValue(t, "int expected to be a struct type",
		func() { Survey(reflect.TypeOf(0)) })
}

func TestSurveyInvalidMetaOption(t *testing.T) {
	type Foo struct {
		Meta `limestone:"invalid"`
	}
	require.PanicsWithValue(t, "invalid struct-level option for meta.Foo: invalid",
		func() { Survey(reflect.TypeOf(Foo{})) })
}

func TestSurveyInvalidFieldOption(t *testing.T) {
	type Foo struct {
		Field string `limestone:"invalid"`
	}
	require.PanicsWithValue(t, "invalid option for meta.Foo.Field: invalid",
		func() { Survey(reflect.TypeOf(Foo{})) })
}

func TestSurveyIncompatibleOptions(t *testing.T) {
	type Foo struct {
		Field string `limestone:"-,invalid"`
	}
	require.PanicsWithValue(t, "option - for field meta.Foo.Field cannot be combined with other options",
		func() { Survey(reflect.TypeOf(Foo{})) })
}

func TestSurveyInvalidEmbeddedStructOption(t *testing.T) {
	type Bar struct{}
	type Foo struct {
		Bar `limestone:"invalid"`
	}
	require.PanicsWithValue(t, "invalid option for meta.Foo.Bar: invalid",
		func() { Survey(reflect.TypeOf(Foo{})) })
}

func TestSurveyDBNameConflict(t *testing.T) {
	type Foo struct {
		Field1 string `limestone:"name=foo"`
		Field2 string `limestone:"name=foo"`
	}
	require.PanicsWithValue(t, "duplicate DB name foo for fields meta.Foo.Field1 (foo) and meta.Foo.Field2 (foo)",
		func() { Survey(reflect.TypeOf(Foo{})) })
}

func TestSurveyDBNameConflictNested(t *testing.T) {
	type Foo struct {
		Meta   `limestone:"name=foo_bar"`
		Field1 string `limestone:"name=foo,identity"`
	}
	type Bar struct {
		Meta   `limestone:"name=foo_bar"`
		Field2 string `limestone:"name=foo"`
	}
	type FooBar struct {
		Foo
		Bar
	}
	require.PanicsWithValue(t, "duplicate DB name foo for fields meta.FooBar.Field1 (foo) and meta.FooBar.Field2 (foo)",
		func() { Survey(reflect.TypeOf(FooBar{})) })
}

func TestSurveyStructDBNameConflict(t *testing.T) {
	type Foo struct {
		Meta `limestone:"name=foo"`
	}
	type Bar struct {
		Meta `limestone:"name=bar"`
	}
	type FooBar struct {
		Foo
		Bar
	}
	require.PanicsWithValue(t, "conflicting DB name settings for struct meta.FooBar: foo vs. bar",
		func() { Survey(reflect.TypeOf(FooBar{})) })
}

func TestSurveyNoIdentity(t *testing.T) {
	type Foo struct {
		Meta   `limestone:"name=foo"`
		Field1 string
	}
	require.PanicsWithValue(t, "missing identity field in struct meta.Foo (foo)",
		func() { Survey(reflect.TypeOf(Foo{})) })
}

func TestSurveyInvalidIdentityType(t *testing.T) {
	type Foo struct {
		Meta   `limestone:"name=foo"`
		Field1 int `limestone:"identity"`
	}
	require.PanicsWithValue(t, "identity field meta.Foo.Field1 must be string-based",
		func() { Survey(reflect.TypeOf(Foo{})) })
}

func TestSurveyDuplicateIdentity(t *testing.T) {
	type Foo struct {
		Meta   `limestone:"name=foo"`
		Field1 string `limestone:"identity"`
		Field2 string `limestone:"identity"`
	}
	require.PanicsWithValue(t, "duplicate identity fields meta.Foo.Field1 and meta.Foo.Field2",
		func() { Survey(reflect.TypeOf(Foo{})) })
}

func TestSurveyDuplicateIdentityNested(t *testing.T) {
	type Foo struct {
		Meta   `limestone:"name=foo_bar"`
		Field1 string `limestone:"identity"`
	}
	type Bar struct {
		Meta   `limestone:"name=foo_bar"`
		Field2 string `limestone:"identity"`
	}
	type FooBar struct {
		Foo
		Bar
	}
	require.PanicsWithValue(t, "duplicate identity fields meta.FooBar.Field1 and meta.FooBar.Field2",
		func() { Survey(reflect.TypeOf(FooBar{})) })
}

func TestSurveyUnexportedField(t *testing.T) {
	type Foo struct {
		Meta   `limestone:"name=foo"`
		field1 int `limestone:"-"`
		field2 int
	}
	require.PanicsWithValue(t, "unexported field meta.Foo.field2 must be skipped using a `limestone:\"-\"` tag",
		func() { Survey(reflect.TypeOf(Foo{field1: 1, field2: 2})) })
}

func TestSurveyInvalidProducer(t *testing.T) {
	type Foo struct {
		Meta `limestone:"name=foo,producer=|"`
		ID   string `limestone:"identity"`
	}
	require.Panics(t, func() { Survey(reflect.TypeOf(Foo{})) })
}

func TestSurveyProducerConflict(t *testing.T) {
	type Foo struct {
		Meta1 Meta   `limestone:"producer=foo"`
		Meta2 Meta   `limestone:"producer=bar"`
		ID    string `limestone:"identity"`
	}
	require.PanicsWithValue(t, "conflicting producer settings for meta.Foo: foo vs. bar",
		func() { Survey(reflect.TypeOf(Foo{})) })
}

func TestSurveyProducerConflictNested(t *testing.T) {
	type Foo struct {
		Meta `limestone:"producer=foo"`
		ID   string `limestone:"identity"`
	}
	type Bar struct {
		Meta `limestone:"producer=bar"`
		Foo
	}
	require.PanicsWithValue(t, "conflicting producer settings for field meta.Bar.ID: foo vs. bar",
		func() { Survey(reflect.TypeOf(Bar{})) })
}

func TestSurveyFieldWithDot(t *testing.T) {
	type Foo struct {
		Meta         `limestone:"producer=foo"`
		ID           string `limestone:"identity"`
		FieldWithDot string `limestone:"name=field.WithDot"`
	}
	require.PanicsWithValue(t, "meta.Foo.FieldWithDot: dots are not allowed in Kafka field names",
		func() { Survey(reflect.TypeOf(Foo{})) })
}

func TestSurveyFieldStartsDollar(t *testing.T) {
	type Foo struct {
		Meta  `limestone:"producer=foo"`
		ID    string `limestone:"identity"`
		Money string `limestone:"name=$$$"`
	}
	require.PanicsWithValue(t, "meta.Foo.Money: Kafka field name is not allowed to start with $",
		func() { Survey(reflect.TypeOf(Foo{})) })
}
