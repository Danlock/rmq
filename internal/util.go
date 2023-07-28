package internal

import "time"

func CalculateDelay(min, max, current time.Duration) time.Duration {
	if current == 0 {
		return min
	} else if current < max {
		return current * 2
	} else {
		return max
	}
}
