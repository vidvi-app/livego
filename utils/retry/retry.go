package retry

import (
	"fmt"
	"time"
)

type Retry struct {
	Times    int // how many times to try. not how many times should retry! will always try at least 1 time!
	Cooldown time.Duration
}

func (r *Retry) Do(f func() error) error {
	var err error
	i := 1
	for {
		err = f()
		if err == nil {
			return nil
		}
		i++
		if i > r.Times {
			break
		}
		<-time.After(r.Cooldown)
	}
	return fmt.Errorf("maximum retries exceeded, last_err=%w", err)
}
