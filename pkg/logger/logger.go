package logger

import (
	"fmt"
	"os"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// Init configures the global logger.
func Init(level, format string) {
	// Parse level
	lvl, err := zerolog.ParseLevel(level)
	if err != nil {
		lvl = zerolog.InfoLevel
	}
	zerolog.SetGlobalLevel(lvl)
	zerolog.TimeFieldFormat = time.RFC3339

	if format == "console" {
		output := zerolog.ConsoleWriter{
			Out:        os.Stdout,
			TimeFormat: "2006-01-02 15:04:05",
		}
		log.Logger = zerolog.New(output).With().Timestamp().Caller().Logger()
	} else {
		log.Logger = zerolog.New(os.Stdout).With().Timestamp().Caller().Logger()
	}
}

// L returns the global logger.
func L() zerolog.Logger {
	return log.Logger
}

// Debug logs a debug message.
func Debug(msg string) { log.Debug().Msg(msg) }

// Info logs an info message.
func Info(msg string) { log.Info().Msg(msg) }

// Warn logs a warning message.
func Warn(msg string) { log.Warn().Msg(msg) }

// Error logs an error message.
func Error(msg string) { log.Error().Msg(msg) }

// Errorf logs a formatted error message.
func Errorf(format string, args ...interface{}) {
	log.Error().Msgf(format, args...)
}

// Infof logs a formatted info message.
func Infof(format string, args ...interface{}) {
	log.Info().Msgf(format, args...)
}

// Debugf logs a formatted debug message.
func Debugf(format string, args ...interface{}) {
	log.Debug().Msgf(format, args...)
}

// Warnf logs a formatted warning message.
func Warnf(format string, args ...interface{}) {
	log.Warn().Msgf(format, args...)
}

// WithField returns a logger with a single field.
func WithField(key string, value interface{}) zerolog.Logger {
	return log.With().Interface(key, value).Logger()
}

// WithFields returns a logger with multiple fields.
func WithFields(fields map[string]interface{}) zerolog.Logger {
	ctx := log.With()
	for k, v := range fields {
		ctx = ctx.Interface(k, v)
	}
	return ctx.Logger()
}

// Fatal logs and exits.
func Fatal(msg string) { log.Fatal().Msg(msg) }

// Fatalf logs formatted and exits.
func Fatalf(format string, args ...interface{}) {
	log.Fatal().Msgf(format, args...)
}

// Printf for compatibility.
func Printf(format string, args ...interface{}) {
	log.Info().Msg(fmt.Sprintf(format, args...))
}
