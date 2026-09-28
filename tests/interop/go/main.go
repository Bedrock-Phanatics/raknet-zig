package main

import (
	"encoding/binary"
	"fmt"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sandertv/go-raknet"
)

var base = time.Now()

func nowNs() uint64 { return uint64(time.Since(base).Nanoseconds()) }

type process struct{ cpuMs, rssKb, peakRssKb uint64 }

func main() {
	args := os.Args
	switch {
	case len(args) == 4 && args[1] == "server":
		seconds, _ := strconv.Atoi(args[3])
		server(args[2], seconds)
	case len(args) >= 7 && len(args) <= 9 && args[1] == "client":
		connections, _ := strconv.Atoi(args[3])
		payload, _ := strconv.Atoi(args[4])
		seconds, _ := strconv.Atoi(args[5])
		warmup, _ := strconv.Atoi(args[6])
		window, interval := 32, 0
		if len(args) >= 8 {
			window, _ = strconv.Atoi(args[7])
		}
		if len(args) >= 9 {
			interval, _ = strconv.Atoi(args[8])
		}
		if connections <= 0 || seconds <= 0 || warmup < 0 || window < 0 || window > 32 || interval < 0 {
			panic("invalid benchmark arguments")
		}
		client(args[2], connections, payload, seconds, warmup, window, interval)
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
	if seconds > 0 {
		go func() {
			for range time.Tick(time.Second) {
				p := sample()
				fmt.Fprintf(os.Stderr, "progress impl=go role=server sessions=%d echoed=%d cpu_ms=%d rss_kb=%d retransmits=%d\n", active.Load(), echoed.Load(), p.cpuMs-baseline.cpuMs, p.rssKb, raknet.MetricsSnapshot().Retransmits)
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
	rttCount                             uint64
	failed, incomplete                   bool
	samples                              []uint64
}

func client(address string, connections, payloadSize, seconds, warmupMs, window, intervalMs int) {
	if payloadSize != 0 && payloadSize < 16 {
		panic("payload too small")
	}
	baseline := sample()
	results := make([]connection, connections)
	var ready sync.WaitGroup
	var done sync.WaitGroup
	start := make(chan struct{})
	// Limit dial bursts to avoid overflowing the server's UDP buffer.
	dialing := make(chan struct{}, 64)
	var startNs uint64
	rampStart := nowNs()
	ready.Add(connections)
	done.Add(connections)
	for i := range results {
		go func(result *connection) {
			defer done.Done()
			dialing <- struct{}{}
			began := nowNs()
			conn, err := raknet.DialTimeout(address, 5*time.Second)
			<-dialing
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
			<-start
			if intervalMs != 0 {
				time.Sleep(time.Duration(i) * time.Duration(intervalMs) * time.Millisecond / time.Duration(connections))
			}
			run(conn, result, payloadSize, startNs+uint64(warmupMs)*uint64(time.Millisecond), uint64(seconds)*uint64(time.Second), window, intervalMs)
		}(&results[i])
	}
	ready.Wait()
	connectedRss := sample().rssKb
	fmt.Fprintf(os.Stderr, "phase name=ready ramp_us=%d\n", (nowNs()-rampStart)/1000)
	startNs = nowNs()
	close(start)
	time.Sleep(time.Duration(warmupMs) * time.Millisecond)
	fmt.Fprintln(os.Stderr, "phase name=measure_start")
	time.Sleep(time.Duration(seconds) * time.Second)
	fmt.Fprintln(os.Stderr, "phase name=measure_end")
	done.Wait()

	var setup, rtt []uint64
	var messages, bytes, failures, incomplete, mismatches uint64
	minimum, maximum, squares := ^uint64(0), uint64(0), float64(0)
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
		minimum = min(minimum, r.messages)
		maximum = max(maximum, r.messages)
		squares += float64(r.messages) * float64(r.messages)
	}
	sort.Slice(setup, func(i, j int) bool { return setup[i] < setup[j] })
	sort.Slice(rtt, func(i, j int) bool { return rtt[i] < rtt[j] })
	p := sample()
	metrics := raknet.MetricsSnapshot()
	fairness := float64(0)
	if squares != 0 {
		fairness = float64(messages) * float64(messages) / (float64(connections) * squares)
	}
	fmt.Fprintf(os.Stderr, "fairness min_messages=%d max_messages=%d jain=%.6f\n", minimum, maximum, fairness)
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

func run(conn *raknet.Conn, result *connection, size int, measureFrom, duration uint64, requestedWindow, intervalMs int) {
	measureUntil := measureFrom + duration
	capacity := size
	if capacity == 0 {
		capacity = 8192
	}
	window := min(max(262144/capacity, 1), requestedWindow)
	var outstanding atomic.Int64
	credits := make(chan struct{}, window)
	for range window {
		credits <- struct{}{}
	}
	readerDone := make(chan struct{})
	defer func() { _ = conn.Close(); <-readerDone }()
	go func() {
		defer close(readerDone)
		for {
			packet, err := conn.ReadPacket()
			if err != nil {
				return
			}
			outstanding.Add(-1)
			credits <- struct{}{}
			if len(packet) < 16 || packet[0] != 0xfe || len(packet) != messageSize(size, packet[9]) || packet[len(packet)-1] != packet[9] {
				result.mismatches++
				continue
			}
			sent := binary.LittleEndian.Uint64(packet[1:9])
			now := nowNs()
			if sent < measureFrom || sent >= measureUntil {
				continue
			}
			if now < measureUntil {
				result.messages++
				result.bytes += uint64(len(packet))
			}
			rtt := (now - sent) / 1000
			if len(result.samples) < maximumSamples {
				result.samples = append(result.samples, rtt)
			} else {
				result.samples[result.rttCount%maximumSamples] = rtt
			}
			result.rttCount++
		}
	}()
	storage := make([]byte, capacity)
	var sequence uint32
	waitTimer := time.NewTicker(5 * time.Millisecond)
	defer waitTimer.Stop()
	for nowNs() < measureUntil {
		select {
		case <-credits:
		case <-waitTimer.C:
			continue
		}
		if nowNs() >= measureUntil {
			break
		}
		payload := storage[:messageSize(size, byte(sequence))]
		payload[0] = 0xfe
		binary.LittleEndian.PutUint64(payload[1:9], nowNs())
		payload[9] = byte(sequence)
		for i := 10; i < len(payload); i++ {
			payload[i] = payload[9]
		}
		outstanding.Add(1)
		if _, err := conn.Write(payload); err != nil {
			outstanding.Add(-1)
			result.failed = true
			return
		}
		sequence++
		if intervalMs != 0 {
			time.Sleep(time.Duration(intervalMs) * time.Millisecond)
		}
	}
	drain := time.Now().Add(3 * time.Second)
	for outstanding.Load() > 0 && time.Now().Before(drain) {
		time.Sleep(time.Millisecond)
	}
	if outstanding.Load() != 0 {
		result.incomplete = true
	}
}

func messageSize(size int, sequence byte) int {
	if size != 0 {
		return size
	}
	return [...]int{32, 128, 512, 1200, 8192}[int(sequence)%5]
}
