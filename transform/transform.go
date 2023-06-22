// Package transform contains transformation utilities that work on sequences
// represented by iterator functions.
//
// They implement patterns commonly used with iterators returned by Limestone's
// snapshot.Search and snapshot.All methods. However, this library isn't tied to
// Limestone.
package transform

import (
	"reflect"
)

// Iterator is an iterator over a sequence. Every call fills in another object
// into ptr argument. Returns false when the end of dataset is reached (the data
// at ptr won't be modified in this call).
//
// Iterator must accept nil instead of a pointer. When the argument is nil, the
// iterator must discard one item. The calling code can use this to skip
// sequence items.
type Iterator = func(ptr any) bool

// Null is an iterator over an empty sequence
func Null(ptr any) bool {
	return false
}

// FromSlice returns an iterator over a slice
func FromSlice(slice any) Iterator {
	rSlice := reflect.ValueOf(slice)
	if rSlice.Kind() != reflect.Slice {
		panic("argument must be a slice")
	}
	i := 0
	return func(ptr any) bool {
		if i >= rSlice.Len() {
			return false
		}
		if ptr != nil {
			reflect.ValueOf(ptr).Elem().Set(rSlice.Index(i))
		}
		i++
		return true
	}
}

// IsEmpty returns true if the sequence is empty
func IsEmpty(iter Iterator) bool {
	return !iter(nil)
}

// IsShorter returns whether the length of the sequence is less than a given number
func IsShorter(iter Iterator, length int) bool {
	if length <= 0 {
		return false // no sequence is shorter than 0 (or less)
	}

	i := 0
	for iter(nil) {
		i++
		if i == length {
			return false
		}
	}
	return true
}

// Count returns the length of a sequence
func Count(iter Iterator) int {
	i := 0
	for iter(nil) {
		i++
	}
	return i
}

// GetUnique returns true if the sequence has one item, and copies that item to
// the memory at ptr. Returns false if the sequence is empty. Panics if the
// sequence has more than one item.
func GetUnique(iter Iterator, ptr any) bool {
	if !iter(ptr) {
		return false
	}
	if iter(nil) {
		panic("item is not unique")
	}
	return true
}

// MustGetUnique retrieves the only item from the sequence and copies it to the
// memory at ptr. Panics unless the sequence has exactly one item.
func MustGetUnique(iter Iterator, ptr any) {
	if !iter(ptr) {
		panic("item not found")
	}
	if iter(nil) {
		panic("item is not unique")
	}
}

// Limit truncates the sequence if it's longer than max items
func Limit(iter Iterator, max int) Iterator {
	i := 0
	return func(ptr any) bool {
		if i >= max {
			return false
		}
		i++
		return iter(ptr)
	}
}

// Collect appends all items from the sequence to a slice to which ptrSlice
// points. Returns the number of items collected.
func Collect(iter Iterator, ptrSlice any) int {
	rPtrSlice := reflect.ValueOf(ptrSlice)
	if rPtrSlice.Kind() != reflect.Ptr {
		panic("ptrSlice must be a pointer")
	}
	rSlice := rPtrSlice.Elem()
	if rSlice.Kind() != reflect.Slice {
		panic("ptrSlice must point to a slice")
	}
	tItem := rSlice.Type().Elem()
	i := 0
	rPtrItem := reflect.New(tItem)
	for {
		if !iter(rPtrItem.Interface()) {
			return i
		}
		i++
		rSlice.Set(reflect.Append(rSlice, rPtrItem.Elem()))
	}
}

// ForEach calls fn for every item of the sequence. The function must take a
// sequence item by value and return either bool or nothing. If it returns bool,
// it can break out of the iteration by returning false.
//
// Returns the number of times fn was called.
func ForEach(iter Iterator, fn any) int {
	rFn := reflect.ValueOf(fn)
	if rFn.Kind() != reflect.Func {
		panic("fn must be a function")
	}
	tFn := rFn.Type()
	if tFn.NumIn() != 1 {
		panic("fn must accept exactly one argument")
	}
	tItem := tFn.In(0)
	if tFn.NumOut() > 1 || tFn.NumOut() == 1 && tFn.Out(0).Kind() != reflect.Bool {
		panic("fn must return nothing or bool")
	}
	i := 0
	rPtrItem := reflect.New(tItem)
	for iter(rPtrItem.Interface()) {
		i++
		res := rFn.Call([]reflect.Value{rPtrItem.Elem()})
		if len(res) > 0 && !res[0].Bool() {
			break
		}
	}
	return i
}

// Filter returns an iterator for the sequence made of only those items of the
// original sequence for which predicate returns true. The predicate must be a
// function taking a sequence item by value and returning bool.
func Filter(iter Iterator, predicate any) Iterator {
	rPredicate := reflect.ValueOf(predicate)
	if rPredicate.Kind() != reflect.Func {
		panic("predicate must be a function")
	}
	tPredicate := rPredicate.Type()
	if tPredicate.NumIn() != 1 {
		panic("predicate must accept exactly one argument")
	}
	tItem := tPredicate.In(0)
	if tPredicate.NumOut() != 1 || tPredicate.Out(0).Kind() != reflect.Bool {
		panic("predicate must return bool")
	}
	rPtrItem := reflect.New(tItem)
	return func(ptr any) bool {
		for iter(rPtrItem.Interface()) {
			if rPredicate.Call([]reflect.Value{rPtrItem.Elem()})[0].Bool() {
				if ptr != nil {
					reflect.ValueOf(ptr).Elem().Set(rPtrItem.Elem())
				}
				return true
			}
		}
		return false
	}
}

// Map returns a new sequence made by applying the given function to each item.
// The function must accept an item by value and return an item. The output item
// may be of a different type. The mapping function can optionally accept a
// second argument of type int. It will receive the 0-based index of the current
// item within the sequence.
func Map(iter Iterator, mapping any) Iterator {
	rMapping := reflect.ValueOf(mapping)
	if rMapping.Kind() != reflect.Func {
		panic("mapping must be a function")
	}
	tMapping := rMapping.Type()
	if tMapping.NumIn() < 1 || tMapping.NumIn() > 2 {
		panic("mapping must accept one or two arguments")
	}
	tItem := tMapping.In(0)
	if tMapping.NumIn() == 2 && tMapping.In(1).Kind() != reflect.Int {
		panic("the second argument of mapping must be int")
	}
	if tMapping.NumOut() != 1 {
		panic("mapping must return one value")
	}
	i := 0
	rPtrItem := reflect.New(tItem)
	arg := make([]reflect.Value, tMapping.NumIn(), tMapping.NumIn())
	return func(ptr any) bool {
		if !iter(rPtrItem.Interface()) {
			return false
		}
		arg[0] = rPtrItem.Elem()
		if len(arg) == 2 {
			arg[1] = reflect.ValueOf(i)
		}
		res := rMapping.Call(arg)
		if ptr != nil {
			reflect.ValueOf(ptr).Elem().Set(res[0])
		}
		i++
		return true
	}
}

// Concatenate returns a sequence made by concatenating all the given sequences
func Concatenate(iter ...Iterator) Iterator {
	i := 0
	return func(ptr any) bool {
		for i < len(iter) {
			if iter[i](ptr) {
				return true
			}
			i++
		}
		return false
	}
}
