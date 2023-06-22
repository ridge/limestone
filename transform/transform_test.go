package transform

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFromSlice(t *testing.T) {
	iter := FromSlice([]int{1, 2, 3})
	var v int
	require.True(t, iter(&v))
	require.Equal(t, 1, v)
	require.True(t, iter(nil))
	require.True(t, iter(&v))
	require.Equal(t, 3, v)
	require.False(t, iter(&v))
	require.Equal(t, 3, v)
	require.False(t, iter(&v))

	iter = FromSlice([]int{})
	require.False(t, iter(&v))
	require.Equal(t, 3, v)
}

func TestIsEmpty(t *testing.T) {
	require.True(t, IsEmpty(Null))
	require.False(t, IsEmpty(FromSlice([]int{1, 2, 3})))
}

func TestIsShorter(t *testing.T) {
	require.True(t, IsShorter(FromSlice([]int{0, 1, 2}), 4))
	require.False(t, IsShorter(FromSlice([]int{0, 1, 2}), 3))
	require.False(t, IsShorter(FromSlice([]int{}), 0))
}

func TestCount(t *testing.T) {
	require.Equal(t, 0, Count(Null))
	require.Equal(t, 3, Count(FromSlice([]int{1, 2, 3})))
}

func TestGetUnique(t *testing.T) {
	require.False(t, GetUnique(Null, nil))
	require.True(t, GetUnique(FromSlice([]int{1}), nil))
	require.Panics(t, func() { GetUnique(FromSlice([]int{2, 3}), nil) })

	var v int
	require.False(t, GetUnique(Null, &v))
	require.Equal(t, 0, v)
	require.True(t, GetUnique(FromSlice([]int{1}), &v))
	require.Equal(t, 1, v)
	require.Panics(t, func() { GetUnique(FromSlice([]int{2, 3}), &v) })
}

func TestMustGetUnique(t *testing.T) {
	require.Panics(t, func() { MustGetUnique(Null, nil) })
	MustGetUnique(FromSlice([]int{1}), nil)
	require.Panics(t, func() { MustGetUnique(FromSlice([]int{2, 3}), nil) })

	var v int
	require.Panics(t, func() { MustGetUnique(Null, &v) })
	MustGetUnique(FromSlice([]int{1}), &v)
	require.Equal(t, 1, v)
	require.Panics(t, func() { MustGetUnique(FromSlice([]int{2, 3}), &v) })
}

func TestLimit(t *testing.T) {
	iter := Limit(FromSlice([]int{1, 2, 3}), 2)
	var v int
	require.True(t, iter(&v))
	require.Equal(t, 1, v)
	require.True(t, iter(&v))
	require.Equal(t, 2, v)
	require.False(t, iter(&v))

	iter = Limit(FromSlice([]int{1, 2}), 2)
	require.True(t, iter(&v))
	require.Equal(t, 1, v)
	require.True(t, iter(&v))
	require.Equal(t, 2, v)
	require.False(t, iter(&v))

	iter = Limit(FromSlice([]int{1}), 2)
	require.True(t, iter(&v))
	require.Equal(t, 1, v)
	require.False(t, iter(&v))
}

func TestCollect(t *testing.T) {
	var s []int
	require.Equal(t, 3, Collect(FromSlice([]int{1, 2, 3}), &s))
	require.Equal(t, []int{1, 2, 3}, s)
	require.Equal(t, 2, Collect(FromSlice([]int{4, 5}), &s))
	require.Equal(t, []int{1, 2, 3, 4, 5}, s)
}

func TestForEach(t *testing.T) {
	var s []int
	require.Equal(t, 3, ForEach(FromSlice([]int{1, 2, 3}), func(v int) {
		s = append(s, v)
	}))
	require.Equal(t, []int{1, 2, 3}, s)

	s = nil
	require.Equal(t, 2, ForEach(FromSlice([]int{1, 2, 3}), func(v int) bool {
		s = append(s, v)
		return v != 2
	}))
	require.Equal(t, []int{1, 2}, s)
}

func TestFilter(t *testing.T) {
	iter := Filter(FromSlice([]int{1, 2, 3}), func(v int) bool { return false })
	var v int
	require.False(t, iter(&v))

	iter = Filter(FromSlice([]int{1, 2, 3}), func(v int) bool { return true })
	require.True(t, iter(&v))
	require.Equal(t, 1, v)
	require.True(t, iter(&v))
	require.Equal(t, 2, v)
	require.True(t, iter(&v))
	require.Equal(t, 3, v)
	require.False(t, iter(&v))

	iter = Filter(FromSlice([]int{}), func(v int) bool { return true })
	require.False(t, iter(&v))

	iter = Filter(FromSlice([]int{1, 2, 3}), func(v int) bool { return v != 2 })
	require.True(t, iter(&v))
	require.Equal(t, 1, v)
	require.True(t, iter(&v))
	require.Equal(t, 3, v)
	require.False(t, iter(&v))
}

func TestMap(t *testing.T) {
	iter := Map(FromSlice([]int{1, 2, 3}), strconv.Itoa)
	var s string
	require.True(t, iter(&s))
	require.Equal(t, "1", s)
	require.True(t, iter(&s))
	require.Equal(t, "2", s)
	require.True(t, iter(&s))
	require.Equal(t, "3", s)
	require.False(t, iter(&s))

	iter = Map(FromSlice([]int{1, 2, 3}), func(v int, index int) string {
		return strconv.Itoa(index) + ":" + strconv.Itoa(v)
	})
	require.True(t, iter(&s))
	require.Equal(t, "0:1", s)
	require.True(t, iter(&s))
	require.Equal(t, "1:2", s)
	require.True(t, iter(&s))
	require.Equal(t, "2:3", s)
	require.False(t, iter(&s))
}

func TestConcatenate(t *testing.T) {
	iter := Concatenate()
	require.True(t, IsEmpty(iter))

	iter = Concatenate(Null, Null)
	require.True(t, IsEmpty(iter))

	iter = Concatenate(Null, FromSlice([]int{1, 2}), Null, FromSlice([]int{3, 4}))
	var v int
	require.True(t, iter(&v))
	require.Equal(t, 1, v)
	require.True(t, iter(&v))
	require.Equal(t, 2, v)
	require.True(t, iter(&v))
	require.Equal(t, 3, v)
	require.True(t, iter(&v))
	require.Equal(t, 4, v)
	require.False(t, iter(&v))
}
