//go:build !linux

package main

// Windows metrics come from the runner.
func sample() process { return process{} }
