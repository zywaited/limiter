package limiter

import "runtime"

func stack() string {
	stackBuf := make([]byte, 4096)
	n := runtime.Stack(stackBuf, false)
	return string(stackBuf[:n])
}
