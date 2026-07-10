package executor

import (
	"math/rand"
	"time"
)

func BackoffDelay(kind string, attempt int, rng *rand.Rand) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	switch kind {
	case "none":
		return 0
	case "linear":
		return time.Duration(attempt) * time.Second
	default:
		shift := attempt - 1
		if shift > 30 {
			shift = 30
		}
		base := time.Second * time.Duration(uint64(1)<<uint(shift))
		jitter := time.Duration(0)
		if rng != nil {
			jitter = time.Duration(rng.Int63n(int64(time.Second)))
		}
		return base + jitter
	}
}
