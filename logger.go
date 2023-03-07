package testcontainer

import (
	"context"
	"fmt"
	zero "github.com/rs/zerolog"
	"os"
)

// Logger - interface for logging inside module testcontainers
//
type Logger interface {
	LogPanic(ctx context.Context, msg string, err error)
	LogDebug(ctx context.Context, msg string)
	LogError(ctx context.Context, msg string, err error)
}

// Local implementation for the Logger interface
type logger struct {
	log zero.Logger
}

func newLogger() Logger {
	var target logger
	target.log = zero.New(os.Stdout).With().
		Caller().
		Timestamp().
		Str("scope", "TestContainer").
		Logger()
	target.log.Level(zero.DebugLevel)
	return &target
}

// LogPanic - writes message with panic level
func (l *logger) LogPanic(_ context.Context, msg string, err error) {
	l.log.Panic().Err(err).Msg(msg)
}

// LogDebug - writes message with debug level
func (l *logger) LogDebug(_ context.Context, msg string) {
	l.log.Debug().Msg(msg)
}

// LogError - writes message with error level
func (l *logger) LogError(_ context.Context, msg string, err error) {
	l.log.Error().Err(err).Msg(msg)
}

// containerLogger - logger for using inside testcontainer engine
// struct must implement only one method - Printf
type containerLogger struct {
	log Logger
}

// Printf write log message
func (l *containerLogger) Printf(format string, v ...interface{}) {
	l.log.LogDebug(nil, "[TestContainer]"+fmt.Sprintf(format, v...))
}
