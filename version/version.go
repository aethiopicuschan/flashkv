package version

import (
	"runtime/debug"
)

var version = "unknown"

func init() {
	bi, ok := debug.ReadBuildInfo()
	if ok {
		version = bi.Main.Version
	} else {
		version = "unknown"
	}
}

func GetVersion() string {
	return version
}
