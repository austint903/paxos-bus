package paxosbus

import (
	"fmt"
	"math"
	"time"
)

func (c *Client) SetScheduleStart(start uint64) error {
	c.scheduleMu.Lock()
	defer c.scheduleMu.Unlock()
	if c.firstSchedule != 0 || start == 0 || start > math.MaxInt64 || start%uint64(c.config.ScheduleWindow()) != 0 || int64(start) <= wallNs() {
		return fmt.Errorf("schedule-start-ns must be a future, window-aligned timestamp and set before scheduling starts")
	}
	c.scheduleStart = start
	return nil
}

func (c *Client) scheduleRequestTarget(now int64) uint64 {
	target := scheduleTarget(now, c.config.ScheduleWindow())
	c.scheduleMu.Lock()
	defer c.scheduleMu.Unlock()
	if target < c.scheduleStart {
		return c.scheduleStart
	}
	return target
}

func (c *Client) PauseScheduleWindowAfter(after time.Duration) error {
	c.scheduleMu.Lock()
	defer c.scheduleMu.Unlock()
	if c.firstSchedule != 0 || after <= 0 || after%c.config.ScheduleWindow() != 0 {
		return fmt.Errorf("pause-window-after must be a positive multiple of the schedule window and set before scheduling starts")
	}
	c.pauseWindowAfter = after
	return nil
}

func (c *Client) scheduleInterval(target uint64) uint64 {
	c.scheduleMu.Lock()
	defer c.scheduleMu.Unlock()
	if c.firstSchedule == 0 {
		c.firstSchedule = target
		var start, end uint64
		if c.pauseWindowAfter > 0 {
			start = target + uint64(c.pauseWindowAfter)
			end = start + uint64(c.config.ScheduleWindow())
		}
		Notice("[%s] schedule-plan first-start=%d pause-start=%d pause-end=%d", c.self, target, start, end)
	}
	if c.pauseWindowAfter > 0 && target == c.firstSchedule+uint64(c.pauseWindowAfter) {
		return 0
	}
	return c.desiredInterval
}
