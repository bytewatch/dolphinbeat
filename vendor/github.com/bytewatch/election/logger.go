package election

import (
	"log"
)

// Logger is an interface that can be implemented to provide custom log output.
type Logger interface {
	Printf(string, ...interface{})
}

// DefaultLogger uses the stdlib log package for logging.
var DefaultLogger Logger = defaultLogger{}

type defaultLogger struct{}

func (defaultLogger) Printf(format string, a ...interface{}) {
	log.Printf(format, a...)
}
