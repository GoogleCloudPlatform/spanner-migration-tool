package logger

import (
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const LOG_FILE_NAME = "harbourbridge.log"

var Log *zap.Logger

func InitializeLogger(inputLogLevel string) error {
	// create zapper encoding config object
	config := zap.NewProductionEncoderConfig()
	// set logging timestamp format
	config.EncodeTime = zapcore.ISO8601TimeEncoder
	// create encoder for logs that are written to file
	fileEncoder := zapcore.NewJSONEncoder(config)
	// create encoder for logs that are written to console
	// we create two encoders because we want to write human readable logs to console and
	// JSON parsable logs to the file
	consoleEncoder := zapcore.NewConsoleEncoder(config)
	// specify log file.
	logFile, _ := os.OpenFile(LOG_FILE_NAME, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	writer := zapcore.AddSync(logFile)
	// create and set the log level from the user input
	zapLogLevel := new(zapcore.Level)
	err := zapLogLevel.Set(inputLogLevel)
	if err != nil {
		return err
	}
	logLevel := zap.NewAtomicLevelAt(*zapLogLevel)
	// create the logger
	core := zapcore.NewTee(
		zapcore.NewCore(fileEncoder, writer, logLevel),
		zapcore.NewCore(consoleEncoder, zapcore.AddSync(os.Stdout), logLevel),
	)
	Log = zap.New(core, zap.AddCaller(), zap.AddStacktrace(zapcore.ErrorLevel))
	return nil
}
