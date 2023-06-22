package test

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type type1 struct{}

type type2 struct{}

// FIXME (misha): add test for timeout

func TestEventsEmptyOk(t *testing.T) {
	t.Parallel()
	test := &testing.T{}
	assert.True(t, AssertForefrontEvents(test, make(chan any)))
	assert.False(t, test.Failed())
}

func TestEventsEmptyUnexpected(t *testing.T) {
	t.Parallel()
	test := &testing.T{}
	assert.False(t, AssertForefrontEvents(test, make(chan any), type1{}))
	assert.True(t, test.Failed())
}

func TestEventsOK(t *testing.T) {
	t.Parallel()
	test := &testing.T{}
	events := make(chan any, 1)
	events <- type1{}
	assert.True(t, AssertForefrontEvents(test, events, type1{}))
	assert.False(t, test.Failed())
}

func TestEventsNoExpected1(t *testing.T) {
	t.Parallel()
	test := &testing.T{}
	events := make(chan any, 1)
	events <- type1{}
	assert.False(t, AssertForefrontEvents(test, events, type2{}))
	assert.True(t, test.Failed())
}

func TestEventsNoExpected2(t *testing.T) {
	t.Parallel()
	test := &testing.T{}
	events := make(chan any, 1)
	events <- type1{}
	assert.False(t, AssertForefrontEvents(test, events, type1{}, type1{}))
	assert.True(t, test.Failed())
}

func TestEventsUnexpectedOK(t *testing.T) {
	t.Parallel()
	test := &testing.T{}
	events := make(chan any, 2)
	events <- type1{}
	events <- type2{}
	assert.True(t, AssertForefrontEvents(test, events, type1{}))
	assert.False(t, test.Failed())
}

func TestEventsUnexpectedError(t *testing.T) {
	t.Parallel()
	test := &testing.T{}
	events := make(chan any, 2)
	events <- type1{}
	events <- type2{}
	assert.False(t, AssertEvents(test, events, type1{}))
	assert.True(t, test.Failed())
}

func TestEventsSequence(t *testing.T) {
	t.Parallel()
	test := &testing.T{}
	events := make(chan any, 2)
	events <- type1{}
	events <- type2{}
	assert.True(t, AssertForefrontEvents(test, events, type1{}, type2{}))
	assert.False(t, test.Failed())
}

func TestEventsPointer(t *testing.T) {
	t.Parallel()
	test := &testing.T{}
	events := make(chan any, 1)
	events <- type1{}
	assert.False(t, AssertForefrontEvents(test, events, &type1{}))
	assert.True(t, test.Failed())
}
