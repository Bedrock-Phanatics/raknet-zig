package main

import (
	"encoding/binary"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/sandertv/go-raknet"
)

var base = time.Now()

func nowNs() uint64 { return uint64(time.Since(base).Nanoseconds()) }

type process struct{ cpuMs, rssKb, peakRssKb uint64 }

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

func main() {
	args := os.Args
	switch {
	case len(args) == 4 && args[1] == "server":
		seconds, _ := strconv.Atoi(args[3])
		server(args[2], seconds)
	case len(args) == 7 && args[1] == "client":
		connections, _ := strconv.Atoi(args[3])
		payload, _ := strconv.Atoi(args[4])
		seconds, _ := strconv.Atoi(args[5])
		warmup, _ := strconv.Atoi(args[6])
		client(args[2], connections, payload, seconds, warmup)
	default:
		fmt.Fprintln(os.Stderr, "usage: raknet-interop-go server <ip:port> <seconds> | client <ip:port> <connections> <payload> <seconds> <warmup_ms>")
		os.Exit(2)
	}
}

func server(address string, seconds int) {
	baseline := sample()
	listener, err := raknet.Listen(address)
	if err != nil {
		panic(err)
	}
	listener.PongData([]byte("MCPE;go-raknet interop;11;1.21;0;1000;0;interop;Survival;1;19132;19133;"))
	var echoed, dropped, connected, active, peak atomic.Int64
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			connected.Add(1)
			now := active.Add(1)
			for {
				old := peak.Load()
				if now <= old || peak.CompareAndSwap(old, now) {
					break
				}
			}
			go func(c *raknet.Conn) {
				defer active.Add(-1)
				defer c.Close()
				for {
					packet, err := c.ReadPacket()
					if err != nil {
						return
					}
					if _, err := c.Write(packet); err != nil {
						dropped.Add(1)
						continue
					}
					echoed.Add(1)
				}
			}(conn.(*raknet.Conn))
		}
	}()
	if seconds >= 60 {
		go func() {
			for range time.Tick(10 * time.Second) {
				p := sample()
				fmt.Fprintf(os.Stderr, "progress impl=go role=server sessions=%d echoed=%d cpu_ms=%d rss_kb=%d\n", active.Load(), echoed.Load(), p.cpuMs-baseline.cpuMs, p.rssKb)
			}
		}()
	}
	time.Sleep(time.Duration(seconds) * time.Second)
	metrics := raknet.MetricsSnapshot()
	p := sample()
	fmt.Fprintf(os.Stderr, "server impl=go sessions=%d connected=%d echoed=%d dropped=%d retransmits=%d malformed=0 rejected=0 cpu_ms=%d rss_kb=%d peak_rss_kb=%d baseline_rss_kb=%d\n",
		peak.Load(), connected.Load(), echoed.Load(), dropped.Load(), metrics.Retransmits, p.cpuMs-baseline.cpuMs, p.rssKb, p.peakRssKb, baseline.rssKb)
	_ = listener.Close()
}

const maximumSamples = 2048

type connection struct {
	setupUs, messages, bytes, mismatches uint64
	failed, incomplete                   bool
	samples                              []uint64
}

func client(address string, connections, payloadSize, seconds, warmupMs int) {
	if payloadSize < 16 {
		panic("payload too small")
	}
	baseline := sample()
	results := make([]connection, connections)
	var ready sync.WaitGroup
	var done sync.WaitGroup
	start := make(chan uint64)
	ready.Add(connections)
	done.Add(connections)
	for i := range results {
		go func(result *connection) {
			defer done.Done()
			began := nowNs()
			conn, err := raknet.DialTimeout(address, 5*time.Second)
			if err != nil {
				fmt.Fprintln(os.Stderr, "connection error:", err)
				result.failed = true
				ready.Done()
				<-start
				return
			}
			defer conn.Close()
			result.setupUs = (nowNs() - began) / 1000
			ready.Done()
			startNs := <-start
			run(conn, result, payloadSize, startNs+uint64(warmupMs)*uint64(time.Millisecond), uint64(seconds)*uint64(time.Second))
		}(&results[i])
	}
	ready.Wait()
	connectedRss := sample().rssKb
	startNs := nowNs()
	for range results {
		start <- startNs
	}
	done.Wait()

	var setup, rtt []uint64
	var messages, bytes, failures, incomplete, mismatches uint64
	for _, r := range results {
		if r.failed {
			failures++
		}
		if r.incomplete {
			incomplete++
		}
		if r.setupUs != 0 {
			setup = append(setup, r.setupUs)
		}
		rtt = append(rtt, r.samples...)
		messages += r.messages
		bytes += r.bytes
		mismatches += r.mismatches
	}
	sort.Slice(setup, func(i, j int) bool { return setup[i] < setup[j] })
	sort.Slice(rtt, func(i, j int) bool { return rtt[i] < rtt[j] })
	p := sample()
	metrics := raknet.MetricsSnapshot()
	fmt.Fprintf(os.Stderr, "client impl=go connections=%d payload=%d setup_p50_us=%d setup_p95_us=%d setup_p99_us=%d rtt_p50_us=%d rtt_p95_us=%d rtt_p99_us=%d msgs_per_s=%.0f mib_per_s=%.2f cpu_ms=%d rss_kb=%d peak_rss_kb=%d kb_per_conn=%d retransmits=%d mismatches=%d incomplete=%d failures=%d\n",
		connections, payloadSize, pct(setup, .5), pct(setup, .95), pct(setup, .99), pct(rtt, .5), pct(rtt, .95), pct(rtt, .99),
		float64(messages)/float64(seconds), float64(bytes)/float64(seconds)/(1024*1024), p.cpuMs-baseline.cpuMs, p.rssKb, p.peakRssKb,
		(connectedRss-min(connectedRss, baseline.rssKb))/uint64(max(connections, 1)), metrics.Retransmits, mismatches, incomplete, failures)
}

func pct(values []uint64, fraction float64) uint64 {
	if len(values) == 0 {
		return 0
	}
	return values[int(float64(len(values)-1)*fraction)]
}

func run(conn *raknet.Conn, result *connection, size int, measureFrom, duration uint64) {
	measureUntil := measureFrom + duration
	window := min(max(262144/size, 1), 32)
	var outstanding atomic.Int64
	credits := make(chan struct{}, window)
	for range window {
		credits <- struct{}{}
	}
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		for {
			packet, err := conn.ReadPacket()
			if err != nil {
				return
			}
			outstanding.Add(-1)
			credits <- struct{}{}
			if len(packet) != size || packet[0] != 0xfe || packet[len(packet)-1] != packet[9] {
				result.mismatches++
				continue
			}
			sent := binary.LittleEndian.Uint64(packet[1:9])
			now := nowNs()
			if sent < measureFrom || sent >= measureUntil {
				continue
			}
			result.messages++
			result.bytes += uint64(len(packet))
			rtt := (now - sent) / 1000
			if len(result.samples) < maximumSamples {
				result.samples = append(result.samples, rtt)
			} else {
				result.samples[result.messages%maximumSamples] = rtt
			}
		}
	}()
	payload := make([]byte, size)
	var sequence uint32
	for nowNs() < measureUntil {
		select {
		case <-credits:
		case <-time.After(5 * time.Millisecond):
			continue
		}
		payload[0] = 0xfe
		binary.LittleEndian.PutUint64(payload[1:9], nowNs())
		payload[9] = byte(sequence)
		for i := 10; i < size; i++ {
			payload[i] = payload[9]
		}
		if _, err := conn.Write(payload); err != nil {
			result.failed = true
			return
		}
		sequence++
		outstanding.Add(1)
	}
	drain := time.Now().Add(3 * time.Second)
	for outstanding.Load() > 0 && time.Now().Before(drain) {
		time.Sleep(time.Millisecond)
	}
	if outstanding.Load() != 0 {
		result.incomplete = true
	}
	_ = conn.Close()
	<-readerDone
}
