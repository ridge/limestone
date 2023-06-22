package wire

import "fmt"

// ErrMismatch is an error type for fatal problems:
// * database version mismatch
// * position continuity broken
type ErrMismatch string

func (err ErrMismatch) Error() string {
	return string(err)
}

// ExitCode fulfils run.WithExitCode.
//
// ErrMismatch is expected during database maintenance such as a version
// upgrade. To distinguish is from other errors, it causes the process to exit
// with code 100.
func (ErrMismatch) ExitCode() int {
	return 100
}

// ErrContinuityBroken is an ErrMismatch error that happens when a client tries to
// resume reading transactions from a position that is no longer valid
const ErrContinuityBroken ErrMismatch = "continuity broken"

// ErrVersionMismatch returns an ErrMismatch error for the situation when the
// database version doesn't match the expectation
func ErrVersionMismatch(expected, actual int) ErrMismatch {
	return ErrMismatch(fmt.Sprintf("version mismatch: expected %d, actual %d", expected, actual))
}
