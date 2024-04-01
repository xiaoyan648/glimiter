# glimiter
go limiter is a multi-level current limiting (Using the compressed version sliding log method to achieve )

# how to use

like go/x/time/rate, 提供 Allow 和 Wait 方法
```go
    // 50 qps && 1000 mps
	lim := NewLimiter(
		WithStrategies(
			NewStrategy(50, StrategyLevelSecond),
            NewStrategy(50, StrategyLevelMinute),
		),
	)

    // ...

    lim.Allow()
    // lim.Wait(ctx)

```