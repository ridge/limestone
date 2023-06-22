package retry

import (
	"errors"
	"fmt"
	"testing"

	"github.com/ridge/limestone/test"
	"github.com/stretchr/testify/require"
)

func TestDo12(t *testing.T) {
	ctx := test.Context(t)
	config := FixedConfig{}

	count0 := 0
	err := Do(ctx, config, func() error {
		count0++
		if count0 == 10 {
			return errors.New("ten")
		}
		return Retriable(fmt.Errorf("%d", count0))
	})
	require.EqualError(t, err, "ten")

	count1 := 0
	ret1, err := Do1(ctx, config, func() (int, error) {
		count1++
		if count1 == 5 {
			return 5, errors.New("five")
		}
		return count1, Retriable(fmt.Errorf("%d", count1))
	})
	require.EqualError(t, err, "five")
	require.Equal(t, 5, ret1)

	count2 := 0
	ret21, ret22, err := Do2(ctx, config, func() (int, float64, error) {
		count2++
		if count2 == 3 {
			return 3, 3.3, errors.New("three")
		}
		return count2, float64(count2) / 2.0, Retriable(fmt.Errorf("%d", count2))
	})
	require.EqualError(t, err, "three")
	require.Equal(t, 3, ret21)
	require.Equal(t, 3.3, ret22)
}
