package run

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/ridge/limestone/tlog"
	"github.com/ridge/must/v2"
	"github.com/ridge/parallel"
	"github.com/spf13/pflag"
	"go.uber.org/zap"
)

var fs = pflag.NewFlagSet(os.Args[0], pflag.ContinueOnError)

func init() {
	fs.ParseErrorsWhitelist.UnknownFlags = true
	fs.String("log-format", "", "Log format (json|text)")
	fs.String("log-color", "", "Colored logs (yes|no|auto)")
	fs.BoolP("verbose", "v", false, "Enable verbose (debug level) messages")
	// Hide usage while parsing the command line here, will be covered by a regular command line parsing.
	fs.Usage = func() {}

	// Add options help to the main command-line parser.
	pflag.CommandLine.AddFlagSet(fs)
}

// Tool runs the top-level task of your program, watching for signals.
//
// The context passed to the task will contain a logger and an ID generator.
//
// If an interruption or termination signal arrives, the context passed to the
// task will be closed.
//
// Tool does not return. It exits with code 0 if the task returns nil, and
// with code 1 if the task returns an error.
//
// Any defer handlers installed before calling Main are ignored. For this
// reason, it is recommended that most or all your main code is inside the task.
//
// Simple example:
//
//	func main() {
//	    pflag.Parse()
//	    srv := service.New(...)
//	    run.Tool(srv.Run)
//	}
//
// Medium-complexity example:
//
//	func main() {
//	    pflag.Parse()
//	    run.Tool(func(ctx context.Context) error {
//	        if err := Step1(ctx); err != nil {
//	            return err
//	        }
//	        if err := Step2(ctx); err != nil {
//	            return err
//	        }
//	        return nil
//	    })
//	}
//
// Complex example:
//
//	func main() {
//	    pflag.Parse()
//	    run.Tool(func(ctx context.Context) error {
//	        return parallel.Run(func(ctx context.Context, spawn SpawnFn) {
//	            s1, err := service1.New(...)
//	            if err != nil {
//	                return err
//	            }
//
//	            s2, err := service2.New(...)
//	            if err != nil {
//	                return err
//	            }
//
//	            if err := s1.HeavyInit(ctx); err != nil {
//	                return err
//	            }
//
//	            spawn("service1", parallel.Fail, s1.Run)
//	            spawn("serivce2", parallel.Fail, s2.Run)
//	            return nil
//	        })
//	    })
//	}
func Tool(task func(ctx context.Context) error) {
	// os.Exit doesn't run deferred functions, so we'll call it in the first
	// defer which runs last
	var err error
	defer func() {
		var wec WithExitCode
		if errors.As(err, &wec) {
			os.Exit(wec.ExitCode())
		}
		if err != nil {
			os.Exit(1)
		}
	}()

	ctx := rootContext()

	err = parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
		spawn("main", parallel.Exit, func(ctx context.Context) error {
			return task(ctx)
		})
		spawn("signals", parallel.Exit, handleSignals)
		return nil
	})
	if err != nil {
		tlog.Get(ctx).Error("Error", zap.Error(err))
	}
}

// Server runs the top-level task of your program similar To Tool.
//
// The difference is in signal handling: if the top-level task exits with
// (possibly wrapped) context.Canceled while handling the signal, the program
// exits with code 0.
//
// Note that any other error returned during signal handling is still considered
// an error and makes Server exit with code 1.
func Server(task func(ctx context.Context) error) {
	Tool(func(ctx context.Context) error {
		err := task(ctx)
		if errors.Is(err, ctx.Err()) {
			return nil
		}
		return err
	})
}

// WithExitCode is an optional interface that can be implemented by an error.
//
// When a (possibly wrapped) error implementing WithExitCode reaches the top
// level, the value returned by the ExitCode method becomes the exit code of the
// process. The default exit code for other errors is 1.
type WithExitCode interface {
	ExitCode() int
}

// cliConfig returns the Config derived from the command line
func cliConfig() tlog.Config {
	if err := fs.Parse(os.Args[1:]); err != nil && !errors.Is(err, pflag.ErrHelp) {
		fmt.Println(err)
		os.Exit(2)
	}

	format := tlog.FormatText
	if fs.Lookup("log-format").Changed {
		format = tlog.Format(must.OK1(fs.GetString("log-format")))
	}
	color := tlog.ColorAuto
	if fs.Lookup("log-color").Changed {
		colorArg := must.OK1(fs.GetString("log-color"))
		switch colorArg {
		case "", "auto":
			color = tlog.ColorAuto
		case "yes":
			color = tlog.ColorYes
		case "no":
			color = tlog.ColorNo
		default:
			panic(fmt.Sprintf("invalid --log-color value %q", colorArg))
		}
	}
	verbose := false
	if fs.Lookup("verbose").Changed {
		verbose = must.OK1(fs.GetBool("verbose"))
	}

	return tlog.Config{
		Format:  format,
		Color:   color,
		Verbose: verbose,
	}
}

func rootContext() context.Context {
	logger := tlog.New(cliConfig())
	return tlog.WithLogger(context.Background(), logger)
}
