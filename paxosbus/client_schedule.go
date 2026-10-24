package paxosbus

import (
	"bufio"
	"fmt"
	"math"
	"net"
	"time"
)

func (c *Client) SetBusInterval(intervalMs uint64) error {
	if intervalMs > math.MaxInt64/1_000_000 {
		return fmt.Errorf("bus interval overflows nanoseconds")
	}
	c.scheduleMu.Lock()
	c.desiredInterval = intervalMs
	c.scheduleMu.Unlock()
	return nil
}

func scheduleTarget(now int64, window time.Duration) uint64 {
	return uint64((now/int64(window) + 2) * int64(window))
}

func (c *Client) acceptHandshake(m HsReply, requests map[uint64]HsRequest, replica int) bool {
	request, ok := requests[m.StartTime]
	if !ok || m.ReplicaId != uint32(replica) || replica != c.config.LeaderIndex(m.ViewId) || m.EndTime != m.StartTime+uint64(c.config.ScheduleWindow()) || m.EndTime > math.MaxInt64 {
		return false
	}
	c.scheduleMu.Lock()
	defer c.scheduleMu.Unlock()
	if m.IntervalMs != request.IntervalMs && !c.submitted[m.StartTime][m.IntervalMs] {
		return false
	}
	if old, exists := c.windows[m.StartTime]; exists && (old.IntervalMs != m.IntervalMs || old.EndTime != m.EndTime) {
		return false
	}
	c.pauseMu.Lock()
	defer c.pauseMu.Unlock()
	if m.ViewId < c.pauseView || m.ViewId < c.viewHint {
		return false
	}
	c.windows[m.StartTime] = m
	if c.paused {
		c.activeView = m.ViewId
		c.paused = false
	}
	return true
}

func (c *Client) scheduleLoop() {
	requests := make(map[uint64]HsRequest)
	window := c.config.ScheduleWindow()
	poll := window / 20
	if poll > clientControlPoll {
		poll = clientControlPoll
	}
	if poll < time.Millisecond {
		poll = time.Millisecond
	}
	for {
		c.pauseMu.Lock()
		view := c.viewHint
		c.pauseMu.Unlock()
		leader := c.config.LeaderIndex(view)
		conn, err := net.DialTimeout("tcp", c.config.Replicas[leader], time.Second)
		if err != nil {
			time.Sleep(poll)
			continue
		}
		if tcp, ok := conn.(*net.TCPConn); ok {
			tcp.SetNoDelay(true)
		}
		lw := &lockedWriter{w: bufio.NewWriter(conn), conn: conn}
		replies := make(chan HsReply, 16)
		done := make(chan struct{})
		stop := make(chan struct{})
		go func() {
			defer close(done)
			reader := bufio.NewReader(conn)
			for {
				code, err := reader.ReadByte()
				if err != nil || code != MsgHsReply {
					return
				}
				var m HsReply
				if m.Unmarshal(reader) != nil {
					return
				}
				select {
				case replies <- m:
				case <-stop:
					return
				}
			}
		}()
		ticker := time.NewTicker(poll)
		connected := true
		for connected {
			now := wallNs()
			target := c.scheduleRequestTarget(now)
			interval := c.scheduleInterval(target)
			requests[target] = HsRequest{ClientId: c.clientId, StartTime: target, IntervalMs: interval}
			for start, req := range requests {
				if start+uint64(window) <= uint64(now) {
					delete(requests, start)
					c.scheduleMu.Lock()
					delete(c.submitted, start)
					c.scheduleMu.Unlock()
					continue
				}
				c.scheduleMu.Lock()
				ready, confirmed := c.windows[start]
				confirmed = confirmed && ready.ViewId >= view
				c.scheduleMu.Unlock()
				if !confirmed {
					if lw.sendMsg(MsgHsRequest, &req) != nil {
						connected = false
						break
					}
					c.scheduleMu.Lock()
					if c.submitted[start] == nil {
						c.submitted[start] = make(map[uint64]bool)
					}
					c.submitted[start][req.IntervalMs] = true
					c.scheduleMu.Unlock()
				}
			}
			if !connected {
				break
			}
			select {
			case m := <-replies:
				if c.acceptHandshake(m, requests, leader) {
					select {
					case c.handshakeReplies <- m:
					default:
					}
				}
			case <-done:
				connected = false
			case <-ticker.C:
				c.pauseMu.Lock()
				connected = c.config.LeaderIndex(c.viewHint) == leader
				c.pauseMu.Unlock()
			}
		}
		ticker.Stop()
		close(stop)
		conn.Close()
		<-done
	}
}

func (c *Client) awaitInitialSchedule() {
	go c.scheduleLoop()
	for m := range c.handshakeReplies {
		if int64(m.EndTime)-c.maxOwdNs <= wallNs() {
			continue
		}
		c.syncWallNs = int64(m.StartTime) - int64(c.startDelayMs)*1e6
		c.pauseMu.Lock()
		c.activeView = m.ViewId
		c.startupComplete = true
		c.pauseMu.Unlock()
		return
	}
}

func (c *Client) generationEnabled() bool {
	now := wallNs() + c.maxOwdNs
	window := uint64(c.config.ScheduleWindow())
	start := uint64(now) / window * window
	c.scheduleMu.Lock()
	m, ok := c.windows[start]
	c.scheduleMu.Unlock()
	return ok && m.IntervalMs > 0 && c.scheduleViewAllowed(m.ViewId)
}

func (c *Client) nextBusArrival(now, last uint64) (uint64, bool) {
	c.scheduleMu.Lock()
	defer c.scheduleMu.Unlock()
	var next uint64
	found := false
	for start, m := range c.windows {
		if start > now {
			continue
		}
		if m.IntervalMs == 0 || !c.scheduleViewAllowed(m.ViewId) {
			if m.EndTime <= now {
				delete(c.windows, start)
			}
			continue
		}
		arrival := start
		if last >= start {
			step := m.IntervalMs * 1_000_000
			arrival += ((last-start)/step + 1) * step
		}
		if arrival >= m.EndTime {
			if m.EndTime <= now {
				delete(c.windows, start)
			}
			continue
		}
		if !found || arrival < next {
			next, found = arrival, true
		}
	}
	return next, found
}

func (c *Client) busLoop() {
	var lastArrival uint64
	lastStats := wallNs()
	for {
		next, ok := c.nextBusArrival(uint64(wallNs()+c.maxOwdNs), lastArrival)
		if !ok {
			time.Sleep(time.Millisecond)
			continue
		}
		if delay := int64(next) - c.maxOwdNs - wallNs(); delay > 0 {
			time.Sleep(time.Duration(delay))
			continue
		}
		c.arrivalNs = next
		c.sendBus()
		lastArrival = next
		if now := wallNs(); now-lastStats >= int64(time.Second) {
			c.emitStats()
			lastStats = now
		}
	}
}

func (c *Client) scheduleViewAllowed(view uint64) bool {
	c.pauseMu.Lock()
	defer c.pauseMu.Unlock()
	return !c.paused && view >= c.activeView && view >= c.viewHint && view >= c.pauseView
}
