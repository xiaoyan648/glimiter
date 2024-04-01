package slidinglog

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// 基于官方测试用例：https://cs.opensource.google/go/x/time/+/master:rate/rate_test.go

const (
	d = 100 * time.Millisecond
)

var (
	t0 = time.Now()
	t1 = t0.Add(time.Duration(1) * d)
	t2 = t0.Add(time.Duration(2) * d)
	t3 = t0.Add(time.Duration(3) * d)
	t4 = t0.Add(time.Duration(4) * d)
	t5 = t0.Add(time.Duration(5) * d)
	t9 = t0.Add(time.Duration(9) * d)

	s0  = t0.Add(time.Duration(10) * d) // 1 s
	s1  = s0.Add(time.Duration(1) * d)  // 1.1 s
	s2  = s0.Add(time.Duration(2) * d)  // 1.2 s
	s10 = s0.Add(time.Duration(10) * d) // 2 s
	s11 = s0.Add(time.Duration(11) * d) // 2.1 s
	s20 = s0.Add(time.Duration(20) * d) // 3 s

	m0 = t0.Add(time.Minute)
	m1 = m0.Add(time.Duration(10) * d)
	m2 = m0.Add(time.Duration(20) * d)
)

type allow struct {
	t  time.Time
	n  int
	ok bool
}

func run(t *testing.T, lim *Limiter, allows []allow) {
	t.Helper()
	for i, allow := range allows {
		ok := lim.AllowN(allow.t, allow.n)
		if ok != allow.ok {
			t.Errorf("step %d: lim.AllowN(%v, %v) = %v want %v",
				i, allow.t, allow.n, ok, allow.ok)
		}
	}
}

func TestNolimit(t *testing.T) {
	lim := NewLimiter()
	ok := lim.AllowN(time.Now(), 100)
	assert.True(t, ok)
}

func TestLimiterBurst1(t *testing.T) {
	lim := NewLimiter(
		WithStrategies(
			NewStrategy(10, StrategyLevelSecond),
		),
	)
	run(t, lim, []allow{
		{t0, 9, true},
		{t1, 2, false}, // 1 sec over 10:false
		{t9, 1, true},
		{s1, 10, false}, // 0.1 - 1.1, 1 + 10 over 10: false
		{s1, 9, true},
	})
}

func TestMultiStrategy(t *testing.T) {
	lim := NewLimiter(
		WithStartTime(t0),
		WithStrategies(
			NewStrategy(1, StrategyLevelSecond),
			NewStrategy(3, StrategyLevelMinute),
		),
	)

	run(t, lim, []allow{
		{t0, 1, true}, // 0 s
		{t0, 1, false},
		{s0, 1, true}, // 1 s
		{s1, 1, false},
		{s10, 1, true},  // 2 s
		{s20, 1, false}, // 3 s, complies with the first strategy, but does not satisfy the second strategy
		{m0, 1, true},   // 1 minute
	})
}

func TestConcurrentRequests(t *testing.T) {
	const (
		limit       = 5
		numRequests = 15
	)
	var (
		wg    sync.WaitGroup
		numOK = uint32(0)
	)

	// Very slow replenishing bucket.
	lim := NewLimiter(
		WithStrategies(
			NewStrategy(limit, StrategyLevelSecond),
		),
	)

	// Tries to take a token, atomically updates the counter and decreases the wait
	// group counter.
	f := func() {
		defer wg.Done()
		if ok := lim.Allow(); ok {
			atomic.AddUint32(&numOK, 1)
		}
	}

	wg.Add(numRequests)
	for i := 0; i < numRequests; i++ {
		go f()
	}
	wg.Wait()
	if numOK != limit {
		t.Errorf("numOK = %d, want %d", numOK, limit)
	}
}

func TestConcurrentRequestsMultiStrategy(t *testing.T) {
	const (
		limit       = 5
		limit5Sec   = 10
		numRequests = 15
	)
	var (
		wg    sync.WaitGroup
		numOK = uint32(0)
	)

	// Very slow replenishing bucket.
	lim := NewLimiter(
		WithSmallWindowLevel(SmallWindowLevelMini),
		WithStrategies(
			NewStrategy(limit, StrategyLevelSecond),
			&Strategy{
				limit:  limit5Sec,
				window: int64(5 * time.Second),
			},
		),
	)

	// Tries to take a token, atomically updates the counter and decreases the wait
	// group counter.
	f := func() {
		defer wg.Done()
		if ok := lim.Allow(); ok {
			atomic.AddUint32(&numOK, 1)
		}
	}

	end := time.Now().Add(5 * time.Second)
	for time.Now().Before(end) {
		wg.Add(1)
		go f()

		time.Sleep(10 * time.Millisecond) // 100 / 1 q/s
	}
	wg.Wait()
	if numOK > limit5Sec+1 {
		t.Errorf("numOK = %d, want %d", numOK, limit5Sec)
	}
}

func TestLongRunningQPS(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	if runtime.GOOS == "openbsd" {
		t.Skip("low resolution time.Sleep invalidates test (golang.org/issue/14183)")
		return
	}

	// The test runs for a few seconds executing many requests and then checks
	// that overall number of requests is reasonable.
	const (
		limit = 100
	)
	numOK := int32(0)

	// note: as smallWindow increases, the control becomes less precise. such as: default smallWindow 100ms, Probably 10 more requests
	lim := NewLimiter(
		WithSmallWindowLevel(SmallWindowLevelMini),
		WithStrategies(
			NewStrategy(limit, StrategyLevelSecond),
		),
	)

	var wg sync.WaitGroup
	f := func() {
		if ok := lim.Allow(); ok {
			atomic.AddInt32(&numOK, 1)
		}
		wg.Done()
	}

	start := time.Now()
	end := start.Add(5 * time.Second)
	for time.Now().Before(end) {
		wg.Add(1)
		go f()

		// This will still offer ~500 requests per second, but won't consume
		// outrageous amount of CPU.
		time.Sleep(2 * time.Millisecond)
	}
	wg.Wait()
	elapsed := time.Since(start)
	ideal := limit * float64(elapsed) / float64(time.Second)

	// We should never get more requests than allowed.
	if want := int32(ideal + 5); numOK > want {
		t.Errorf("numOK = %d, want %d (ideal %f)", numOK, want, ideal)
	}
	// We should get very close to the number of requests allowed.
	if want := int32(0.999 * ideal); numOK < want {
		t.Errorf("numOK = %d, want %d (ideal %f)", numOK, want, ideal)
	}
}

func TestLongRunningQPSUseWait(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	if runtime.GOOS == "openbsd" {
		t.Skip("low resolution time.Sleep invalidates test (golang.org/issue/14183)")
		return
	}

	// The test runs for a few seconds executing many requests and then checks
	// that overall number of requests is reasonable.
	const (
		limit = 100
	)
	numOK := int32(0)

	// note: as smallWindow increases, the control becomes less precise. such as: default smallWindow 100ms, Probably 10 more requests
	lim := NewLimiter(
		WithStrategies(
			NewStrategy(limit, StrategyLevelSecond),
		),
	)

	var wg sync.WaitGroup

	start := time.Now()
	end := start.Add(2 * time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	for time.Now().Before(end) {
		wg.Add(1)
		go func() {
			err := lim.Wait(ctx)
			if err == nil {
				atomic.AddInt32(&numOK, 1)
			}
			wg.Done()
		}()

		// This will still offer ~500 requests per second, but won't consume
		// outrageous amount of CPU.
		time.Sleep(2 * time.Millisecond)
	}
	cancel()
	wg.Wait()
	elapsed := time.Since(start)
	ideal := limit * float64(elapsed) / float64(time.Second)

	// We should never get more requests than allowed.
	if want := int32(ideal + 10); numOK > want {
		t.Errorf("numOK = %d, want %d (ideal %f)", numOK, want, ideal)
	}
	// We should get very close to the number of requests allowed.
	if want := int32(0.999 * ideal); numOK < want {
		t.Errorf("numOK = %d, want %d (ideal %f)", numOK, want, ideal)
	}
}

type wait struct {
	name     string
	ctx      context.Context
	n        int
	delay    int  // in multiples of d
	needWait bool // 是否需要再等待
	nilErr   bool
}

func runWait(t *testing.T, tt *testTime, lim *Limiter, w wait) {
	t.Helper()
	start := tt.now()
	needWait, err := lim.wait(w.ctx, w.n, start, tt.newTimer)
	delay := tt.since(start)

	if (w.nilErr && err != nil) || (!w.nilErr && err == nil) || d*time.Duration(w.delay) != delay || needWait != w.needWait {
		errString := "<nil>"
		if !w.nilErr {
			errString = "<non-nil error>"
		}
		t.Errorf("lim.WaitN(%v, lim, %v) = (%v, %v) with delay %v; want (%v, %v) with delay %v (±%v)",
			w.name, w.n, needWait, err, delay, w.needWait, errString, d*time.Duration(w.delay), d/2)
	}
}

func TestWaitSimple(t *testing.T) {
	now := time.Now()
	t0 := makeTestTime(now)
	t1 := makeTestTime(now.Add(100 * time.Millisecond))
	t10 := makeTestTime(now.Add(1000 * time.Millisecond))
	t11 := makeTestTime(now.Add(1100 * time.Millisecond))
	t12 := makeTestTime(now.Add(1200 * time.Millisecond))

	lim := NewLimiter(
		WithStartTime(now),
		WithStrategies(
			NewStrategy(5, StrategyLevelSecond),
		),
	)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	runWait(t, t0, lim, wait{"already-cancelled", ctx, 1, 0, false, false})

	runWait(t, t0, lim, wait{"act-now", context.Background(), 2, 0, false, true})    // 0ms
	runWait(t, t1, lim, wait{"act-100ms", context.Background(), 3, 0, false, true})  // 100ms
	runWait(t, t1, lim, wait{"act-wait1", context.Background(), 2, 9, true, true})   // after wait 1000 ms
	runWait(t, t10, lim, wait{"act-wait2", context.Background(), 3, 1, true, true})  // after wait 1100 ms
	runWait(t, t11, lim, wait{"act-wait3", context.Background(), 1, 0, false, true}) // after wait 1200 ms
	runWait(t, t12, lim, wait{"act-nowait", context.Background(), 1, 0, false, true})
	runWait(t, t12, lim, wait{"act-nowait", context.Background(), 1, 0, false, true})
	runWait(t, t12, lim, wait{"act-nowait", context.Background(), 1, 0, false, true})
}

func TestWaitCancel(t *testing.T) {
	now := time.Now()
	tt := makeTestTime(now)

	lim := NewLimiter(
		WithStartTime(now),
		WithStrategies(
			NewStrategy(5, StrategyLevelSecond),
		))

	ctx, cancel := context.WithCancel(context.Background())
	runWait(t, tt, lim, wait{"act-now", ctx, 2, 0, false, true})
	ch, _, _ := tt.newTimer(d)
	go func() {
		<-ch
		cancel()
	}()
	runWait(t, tt, lim, wait{"will-cancel", ctx, 4, 1, false, false})
	// after cancel, 4 token will be cancel, lim.Wait should return immediately
	runWait(t, tt, lim, wait{"act-now-after-cancel", context.Background(), 2, 0, false, true})
}

// func TestWaitTimeout(t *testing.T) {
// 	tt := makeTestTime(t)

// 	lim := NewLimiter(10, 3)

// 	ctx, cancel := context.WithTimeout(context.Background(), d)
// 	defer cancel()
// 	runWait(t, tt, lim, wait{"act-now", ctx, 2, 0, true})
// 	runWait(t, tt, lim, wait{"w-timeout-err", ctx, 3, 0, false})
// }

// func TestWaitInf(t *testing.T) {
// 	tt := makeTestTime(t)

// 	lim := NewLimiter(Inf, 0)

// 	runWait(t, tt, lim, wait{"exceed-burst-no-error", context.Background(), 3, 0, true})
// }

func TestDurationFromStrategy(t *testing.T) {
	lim := NewLimiter(
		WithStrategies(
			NewStrategy(50, StrategyLevelSecond),
			NewStrategy(1000, StrategyLevelMinute),
		),
	)

	wait := lim.durationFromStrategy(lim.strategies[1], 1, t0)
	assert.Equal(t, time.Duration(0), wait)
	wait = lim.durationFromStrategy(lim.strategies[1], 49, t1)
	assert.Equal(t, time.Duration(0), wait)
	wait = lim.durationFromStrategy(lim.strategies[1], 1, t1)
	assert.Equal(t, time.Duration(0), wait)

	wait = lim.durationFromStrategy(lim.strategies[0], 1, t0)
	assert.Equal(t, time.Duration(0), wait)
}

// As the time window becomes longer, the increase in request qps will lead to a rapid decline in performance.
// window(1s) smallWindow(10Millisecond) qps 100 BenchmarkAllowN-8   	 2354732	       432.0 ns/op	      16 B/op	       2 allocs/op
// window(1s) smallWindow(10Millisecond) qps 1000 BenchmarkAllowNBigWindow-8   	  700444	      1960 ns/op	      16 B/op	       2 allocs/op
// window(1m) smallWindow(10Millisecond) qps 100 BenchmarkAllowNBigWindow-8   	   78618	     19874 ns/op	      17 B/op	       2 allocs/op
// window(1h) smallWindow(10Millisecond) qps 100 BenchmarkAllowNBigWindow-8   	   13075	     86537 ns/op	      69 B/op	       2 allocs/op
func BenchmarkAllowNBigWindow(b *testing.B) {
	lim := NewLimiter(
		WithStrategies(
			NewStrategy(1000, StrategyLevelSecond),
		),
	)
	now := time.Now()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			now = now.Add(1 * time.Millisecond)
			lim.AllowN(now, 1)
		}
	})
}

// func BenchmarkWaitNNoDelay(b *testing.B) {
// 	lim := NewLimiter(Limit(b.N), b.N)
// 	ctx := context.Background()
// 	b.ReportAllocs()
// 	b.ResetTimer()
// 	for i := 0; i < b.N; i++ {
// 		lim.WaitN(ctx, 1)
// 	}
// }

// testTime is a fake time used for testing.
type testTime struct {
	mu     sync.Mutex
	cur    time.Time   // current fake time
	timers []testTimer // fake timers
}

// testTimer is a fake timer.
type testTimer struct {
	when time.Time
	ch   chan<- time.Time
}

// now returns the current fake time.
func (tt *testTime) now() time.Time {
	tt.mu.Lock()
	defer tt.mu.Unlock()
	return tt.cur
}

// newTimer creates a fake timer. It returns the channel,
// a function to stop the timer (which we don't care about),
// and a function to advance to the next timer.
func (tt *testTime) newTimer(dur time.Duration) (<-chan time.Time, func() bool, func()) {
	tt.mu.Lock()
	defer tt.mu.Unlock()
	ch := make(chan time.Time, 1)
	timer := testTimer{
		when: tt.cur.Add(dur),
		ch:   ch,
	}
	tt.timers = append(tt.timers, timer)
	return ch, func() bool { return true }, tt.advanceToTimer
}

// since returns the fake time since the given time.
func (tt *testTime) since(t time.Time) time.Duration {
	tt.mu.Lock()
	defer tt.mu.Unlock()
	return tt.cur.Sub(t)
}

// advance advances the fake time.
func (tt *testTime) advance(dur time.Duration) {
	tt.mu.Lock()
	defer tt.mu.Unlock()
	tt.advanceUnlocked(dur)
}

// advanceUnlock advances the fake time, assuming it is already locked.
func (tt *testTime) advanceUnlocked(dur time.Duration) {
	tt.cur = tt.cur.Add(dur)
	i := 0
	for i < len(tt.timers) {
		if tt.timers[i].when.After(tt.cur) {
			i++
		} else {
			tt.timers[i].ch <- tt.cur
			copy(tt.timers[i:], tt.timers[i+1:])
			tt.timers = tt.timers[:len(tt.timers)-1]
		}
	}
}

// advanceToTimer advances the time to the next timer.
func (tt *testTime) advanceToTimer() {
	tt.mu.Lock()
	defer tt.mu.Unlock()
	if len(tt.timers) == 0 {
		panic("no timer")
	}
	when := tt.timers[0].when
	for _, timer := range tt.timers[1:] {
		if timer.when.Before(when) {
			when = timer.when
		}
	}
	tt.advanceUnlocked(when.Sub(tt.cur))
}

// makeTestTime hooks the testTimer into the package.
func makeTestTime(cur time.Time) *testTime {
	return &testTime{
		cur: cur,
	}
}
