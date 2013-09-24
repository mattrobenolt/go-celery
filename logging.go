package celery

import (
	log "code.google.com/p/log4go"
	"flag"
	"strings"
)

var (
	loglevel = flag.String("l", "error", "Log level")
)

var logger log.Logger

func SetupLogging() {
	flag.Parse()

	level := log.ERROR

	switch strings.ToLower(*loglevel) {
	case "debug":
		level = log.DEBUG
	case "trace":
		level = log.TRACE
	case "info":
		level = log.INFO
	case "warning":
		level = log.WARNING
	case "error":
		level = log.ERROR
	case "critical":
		level = log.CRITICAL
	}

	logger = log.NewDefaultLogger(level)
}
