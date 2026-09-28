package main

import (
	"os"
	"strconv"
	"strings"
	"syscall"
)

func sample() process {
	var usage syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &usage)
	cpu := uint64(usage.Utime.Sec+usage.Stime.Sec)*1000 + uint64(usage.Utime.Usec+usage.Stime.Usec)/1000
	status, _ := os.ReadFile("/proc/self/status")
	return process{cpu, field(string(status), "VmRSS:"), field(string(status), "VmHWM:")}
}

func field(status, name string) uint64 {
	i := strings.Index(status, name)
	if i < 0 {
		return 0
	}
	parts := strings.Fields(status[i+len(name):])
	if len(parts) == 0 {
		return 0
	}
	v, _ := strconv.ParseUint(parts[0], 10, 64)
	return v
}
