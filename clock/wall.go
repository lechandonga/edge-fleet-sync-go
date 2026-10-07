package clock

import "time"

func timeNowMs() int64 { return time.Now().UnixNano() / int64(time.Millisecond) }
