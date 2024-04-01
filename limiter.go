package slidinglog

import (
	"context"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"
)

/*
 结合计数进行内存压缩，牺牲了一定精准性
*/

type (
	StrategyLevel    int
	SmallWindowLevel int
)

const (
	StrategyLevelSecond StrategyLevel = iota
	StrategyLevelMinute
	StrategyLevelHour
)

const (
	SmallWindowLevelMini   SmallWindowLevel = iota // 10ms
	SmallWindowLevelMiddle                         // 100 ms
	SmallWindowLevelBig                            // 1s 退化为单秒的计数法
)

// InfDuration is the duration returned by Delay when a Reservation is not OK.
const InfDuration = time.Duration(1<<63 - 1)

const defaultSmallLevel = SmallWindowLevelMiddle

// Strategy 滑动日志限流器的策略
type Strategy struct {
	limit          int   // 窗口请求上限
	window         int64 // 窗口时间大小
	smallWindowNum int64 // 小窗口数量
}

func NewStrategy(limit int, wl StrategyLevel) *Strategy {
	window := time.Second
	switch wl {
	case StrategyLevelSecond:
		window = time.Second
	case StrategyLevelMinute:
		window = time.Minute
	case StrategyLevelHour:
		window = time.Hour
	}
	return &Strategy{
		limit:  limit,
		window: int64(window),
	}
}

// SlidingLogLimiter 滑动日志限流器
type Limiter struct {
	mutex sync.Mutex // 避免并发问题

	strategies  []*Strategy // 滑动日志限流器策略列表
	minLimit    int         // 最小请求上限
	smallWindow int64       // 小窗口时间大小 ms
	counters    []int       // 小窗口计数器

	cycle     int64     // counters 循环次数
	flag      bool      // 标记
	startTime time.Time // ns
}

func parseCount(count int) (flag bool, n int) {
	return (count & 1) == 1, count >> 1
}

func makeCount(flag bool, count int) int {
	flagint := 0
	if flag {
		flagint = 1
	}
	return (count << 1) | flagint
}

type Option func(l *Limiter)

func (l *Limiter) Allow() bool {
	return l.AllowN(time.Now(), 1)
}

func (l *Limiter) AllowN(now time.Time, n int) bool {
	return l.reserveN(now, n, 0).ok
}

// Wait is shorthand for WaitN(ctx, 1).
func (l *Limiter) Wait(ctx context.Context) (err error) {
	return l.WaitN(ctx, 1)
}

// Cancel is shorthand for CancelAt(time.Now()).
func (r *Reservation) Cancel() {
	// r.CancelAt(time.Now())
	r.CancelAt()
}

// CancelAt indicates that the reservation holder will not perform the reserved action
// and reverses the effects of this Reservation on the rate limit as much as possible,
// considering that other reservations may have already been made.
// 注意 目前 timeout 或 cancel 回收当前token后, 不会重新计算后续需要等待的时间.
func (r *Reservation) CancelAt() {
	// if !r.ok {
	// 	return
	// }

	// r.lim.mutex.Lock()
	// defer r.lim.mutex.Unlock()

	// flag, count := parseCount(r.lim.counters[r.index])
	// r.lim.counters[r.index] = makeCount(flag, count-r.count)
}

// WaitN blocks until lim permits n events to happen.
// It returns an error if n exceeds the Limiter's burst size, the Context is
// canceled, or the expected wait time exceeds the Context's Deadline.
// The burst limit is ignored if the rate limit is Inf.1609904126124 1609904126224
func (l *Limiter) WaitN(ctx context.Context, n int) (err error) {
	// The test code calls lim.wait with a fake timer generator.
	// This is the real timer generator.
	newTimer := func(d time.Duration) (<-chan time.Time, func() bool, func()) {
		timer := time.NewTimer(d)
		return timer.C, timer.Stop, func() {}
	}

	needWait := true

	for needWait {
		nw, err := l.wait(ctx, n, time.Now(), newTimer)
		if err != nil {
			return err
		}
		needWait = nw
	}
	return nil
}

// smallWindowIndex 结束小窗口索引
func (l *Limiter) currentWindowIndex(now time.Time) (endIndex, cycle int64) {
	durtion := now.UnixMicro() - l.startTime.UnixMicro()
	if durtion < 0 {
		return 0, 0
	}
	durtionms := int64(time.Duration(durtion/1000) * time.Millisecond)
	return (durtionms / l.smallWindow) % int64(len(l.counters)), (durtionms / l.smallWindow) / int64(len(l.counters))
}

// startWindowIndex 起始小窗口索引
func (l *Limiter) startWindowIndex(endIndex int64, mallWindowNum int) int64 {
	startIndex := endIndex - int64(mallWindowNum) + 1
	if startIndex < 0 {
		startIndex += int64(len(l.counters))
	}
	return startIndex
}

// wait is the internal implementation of WaitN.
func (l *Limiter) wait(
	ctx context.Context, n int, t time.Time,
	newTimer func(d time.Duration) (<-chan time.Time, func() bool, func()),
) (needWait bool, err error) {
	if n > l.minLimit {
		return false, fmt.Errorf("rate: Wait(n=%d) exceeds min limit %d", n, l.minLimit)
	}
	// Check if ctx is already cancelled
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	default:
	}
	// Determine wait limit
	waitLimit := InfDuration
	if deadline, ok := ctx.Deadline(); ok {
		waitLimit = deadline.Sub(t)
	}
	// Reserve
	r := l.reserveN(t, n, waitLimit)
	if !r.ok {
		return false, fmt.Errorf("rate: Wait(n=%d) would exceed context deadline", n)
	}
	// Wait if necessary
	delay := r.DelayFrom(t)
	if delay == 0 {
		return false, nil
	}
	ch, stop, advance := newTimer(delay)
	defer stop()
	advance() // only has an effect when testing
	select {
	case <-ch:
		// We can proceed.
		return true, nil
	case <-ctx.Done():
		// Context was canceled before we could proceed.  Cancel the
		// reservation, which may permit other events to proceed sooner.
		r.Cancel()
		return false, ctx.Err()
	}
}

func WithSmallWindowLevel(swl SmallWindowLevel) Option {
	return func(l *Limiter) {
		switch swl {
		case SmallWindowLevelMini:
			l.smallWindow = int64(10 * time.Millisecond)
		case SmallWindowLevelMiddle:
			l.smallWindow = int64(100 * time.Millisecond)
		case SmallWindowLevelBig:
			l.smallWindow = int64(1000 * time.Millisecond)
		}
	}
}

func WithStrategies(ss ...*Strategy) Option {
	return func(l *Limiter) {
		l.strategies = ss
	}
}

func WithStartTime(startTime time.Time) Option {
	return func(l *Limiter) {
		l.startTime = startTime
	}
}

func NewLimiter(opts ...Option) *Limiter {
	lim := &Limiter{
		smallWindow: int64(100 * time.Millisecond),
		startTime:   time.Now(),
	}
	for _, opt := range opts {
		opt(lim)
	}

	if len(lim.strategies) == 0 {
		return lim
	}

	strategies := make([]*Strategy, len(lim.strategies))
	for i := range lim.strategies {
		strategies[i] = &Strategy{
			limit:  lim.strategies[i].limit,
			window: lim.strategies[i].window,
		}
	}

	// 排序策略，窗口时间大的排前面，相同窗口上限大的排前面
	sort.Slice(strategies, func(i, j int) bool {
		a, b := strategies[i], strategies[j]
		if a.window == b.window {
			return a.limit > b.limit
		}
		return a.window > b.window
	})

	minLimit := math.MaxInt
	var maxWindowNum int64
	for _, strategy := range strategies {
		if minLimit > strategy.limit {
			minLimit = strategy.limit
		}
		strategy.smallWindowNum = strategy.window / lim.smallWindow
		if maxWindowNum < strategy.smallWindowNum {
			maxWindowNum = strategy.smallWindowNum
		}
	}

	lim.counters = make([]int, maxWindowNum)
	lim.strategies = strategies
	lim.minLimit = minLimit

	return lim
}

type Reservation struct {
	ok bool
	// index     int64 // 预分配的小窗口索引
	// count     int   // 预分配的小窗口内的计数
	// lim       *Limiter
	timeToAct time.Time
}

func (l *Limiter) reserveN(now time.Time, n int, maxFutureReserve time.Duration) Reservation {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	if len(l.strategies) == 0 {
		return Reservation{ok: true}
	}

	// 新的一圈覆盖老值
	index, cycle := l.currentWindowIndex(now)
	if cycle > l.cycle {
		l.cycle++
		l.flag = !l.flag
	}
	flag, count := parseCount(l.counters[index])
	if l.flag != flag {
		l.counters[index] = makeCount(l.flag, 0)
		flag = l.flag
		count = 0
	}

	// 计算需要等待的时间
	var maxWaitDuration time.Duration
	for _, s := range l.strategies {
		if waitDuration := l.durationFromStrategy(s, n, now); waitDuration > maxWaitDuration {
			maxWaitDuration = waitDuration
		}
	}

	ok := n <= l.minLimit && maxWaitDuration <= maxFutureReserve

	r := Reservation{
		ok: ok,
	}
	if ok {
		r.timeToAct = now.Add(maxWaitDuration)
		// if r.timeToAct.After(now) {
		// 	index, _ = l.currentWindowIndex(r.timeToAct)
		// }
		// r.index = index
		// r.count = n
		// r.lim = l
		if maxWaitDuration == 0 {
			l.counters[index] = makeCount(flag, count+n)
		}
	}

	return r
}

func (l *Limiter) durationFromStrategy(s *Strategy, n int, now time.Time) time.Duration {
	alreadyCount := 0
	end, _ := l.currentWindowIndex(now)
	start := l.startWindowIndex(end, int(s.smallWindowNum))
	for i := 0; i < int(s.smallWindowNum); i++ {
		_, count := parseCount(l.counters[(int(start)+i)%len(l.counters)])
		alreadyCount += count
	}

	tokens := s.limit - (alreadyCount + n)
	if tokens >= 0 {
		return 0
	}

	for i := 0; i < int(s.smallWindowNum); i++ {
		_, count := parseCount(l.counters[(int(start)+i)%len(l.counters)])
		tokens += count
		if tokens >= 0 {
			return time.Duration(i+1) * time.Duration(l.smallWindow)
		}
	}

	return time.Duration(s.window)
}

func (r *Reservation) DelayFrom(now time.Time) time.Duration {
	if !r.ok {
		return InfDuration
	}
	delay := r.timeToAct.Sub(now)
	if delay < 0 {
		return 0
	}
	return delay
}
