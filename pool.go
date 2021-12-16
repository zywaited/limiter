package limiter

import (
	"context"
	"errors"
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

const defaultCheckTime = time.Minute * 5

var (
	ErrTimeout    = errors.New("pool handle timeout")
	ErrNotRunning = errors.New("pool handle not running")
)

type statusType int32

const (
	statusInitialized statusType = iota
	statusRunning
	statusGraceFulST
	statusForceSTW
)

type TaskJob func()

type (
	Pool interface {
		Run() error
		Stop() error
		Submit(context.Context, TaskJob) error
		SubmitChan() chan<- TaskJob
	}

	WorkerGroup interface {
		Len() int
		Add(Worker)
		AcquireOne() Worker
		Acquire(int) []Worker
		Expire(time.Duration, int) []Worker
	}
)

type (
	PoolOptions   func(*pool)
	WorkerCreator func() Worker
)

type pool struct {
	ap        *sync.Pool
	wc        WorkerCreator
	ws        WorkerGroup
	logger    Logger
	status    int32
	running   int32
	initLimit int32
	limit     int32
	checkNum  int
	idle      int
	spanNum   int
	wt        chan TaskJob
	wr        chan Worker
	idleTime  time.Duration
	blockTime time.Duration
}

func NewPool(ws WorkerGroup, opts ...PoolOptions) *pool {
	limit := runtime.GOMAXPROCS(0) * 100
	p := &pool{
		ws:        ws,
		initLimit: int32(limit),
		checkNum:  int(math.Ceil(float64(limit) * 0.25)),
		idle:      int(math.Ceil(float64(limit) * 0.25)),
		idleTime:  defaultCheckTime,
		wt:        make(chan TaskJob, 128),
		wr:        make(chan Worker, 128),
		status:    int32(statusInitialized),
	}
	p.ap = &sync.Pool{New: func() interface{} {
		return newPoolWorker(p.wc())
	}}
	for _, opt := range opts {
		opt(p)
	}
	if p.idle == 0 || p.idle > int(p.initLimit) {
		p.idle = int(p.initLimit)
	}
	p.limit = p.initLimit
	if p.wc == nil {
		p.wc = defaultWorkerCreator
	}
	return p
}

func (p *pool) Run() error {
	for {
		cv := atomic.LoadInt32(&p.status)
		if cv == int32(statusRunning) {
			return nil
		}
		if !atomic.CompareAndSwapInt32(&p.status, cv, int32(statusRunning)) {
			continue
		}
		if cv == int32(statusForceSTW) || cv == int32(statusInitialized) {
			go p.run()
		}
		return nil
	}
}

func (p *pool) Stop() error {
	for {
		cv := atomic.LoadInt32(&p.status)
		if cv != int32(statusRunning) {
			return nil
		}
		if !atomic.CompareAndSwapInt32(&p.status, cv, int32(statusGraceFulST)) {
			continue
		}
		return nil
	}
}

func (p *pool) Submit(ctx context.Context, tj TaskJob) error {
	if p.Stopped() {
		return ErrNotRunning
	}
	select {
	case p.wt <- tj:
	case <-ctx.Done():
		return ErrTimeout
	}
	return nil
}

func (p *pool) SubmitChan() chan<- TaskJob {
	if p.Stopped() {
		return nil
	}
	return p.wt
}

func (p *pool) Running() bool {
	return atomic.LoadInt32(&p.status) == int32(statusRunning)
}

func (p *pool) Stopped() bool {
	return !p.Running()
}

func (p *pool) run() {
	i := time.NewTicker(p.idleTime)
	defer func() {
		atomic.StoreInt32(&p.status, int32(statusForceSTW))
		if err := recover(); err != nil && p.logger != nil {
			p.logger.Errorf("limiter-pool acquire panic: %v, stack: %s", err, stack())
		}
		i.Stop()
	}()
	var (
		tj      TaskJob
		wc      chan<- TaskJob
		ewc     chan<- TaskJob
		wt      = p.wt
		ic      = i.C
		ews     workers
		bc      <-chan time.Time
		spanNum = 0
	)
	acquireWs := func() chan<- TaskJob {
		w := p.acquire()
		if w == nil {
			w = ews.acquire()
		}
		if w == nil {
			return nil
		}
		return w.(*poolWorker).tasks
	}
	acquireEws := func() chan<- TaskJob {
		w := ews.acquire()
		if w == nil {
			return nil
		}
		return w.(*poolWorker).tasks
	}
	acquireW := func() chan<- TaskJob {
		c := acquireWs()
		if c == nil && ewc != nil {
			c = ewc
			ewc = nil
		}
		return c
	}
	for {
		ic = i.C
		if tj != nil && wc == nil {
			wc = acquireW()
		}
		if ewc == nil {
			ewc = acquireEws()
		}
		if ews.len() > 0 {
			ic = nil
		}
		if wc != nil {
			bc = nil
		}
		select {
		case <-ic:
			ews = p.expire()
			if spanNum > 0 && p.limit-atomic.LoadInt32(&p.running) > p.initLimit<<uint(spanNum-1) {
				spanNum--
				p.limit -= p.initLimit << uint(spanNum)
			}
		case j := <-wt:
			tj = j
			wt = nil
			wc = acquireWs()
			if wc == nil && p.blockTime > 0 {
				bc = time.After(p.blockTime)
			}
		case wc <- tj:
			wc = nil
			tj = nil
			wt = p.wt
		case ewc <- nil:
			ewc = nil
		case w := <-p.wr:
			if tj != nil && wc == nil {
				wc = w.(*poolWorker).tasks
				break
			}
			p.ws.Add(w)
		case <-bc:
			if spanNum < p.spanNum {
				p.limit += p.initLimit << uint(spanNum)
				spanNum++
			}
			wc = p.newWorker().(*poolWorker).tasks
		}
	}
}

func (p *pool) expire() []Worker {
	checkNum := p.checkNum
	if checkNum <= 0 {
		checkNum = p.ws.Len()
	}
	ews := p.ws.Expire(p.idleTime, checkNum)
	if len(ews) > 0 {
		return ews
	}
	if p.idle > 0 && p.ws.Len() > p.idle {
		ews = p.ws.Acquire(checkNum)
	}
	return ews
}

func (p *pool) acquire() Worker {
	w := p.ws.AcquireOne()
	if w == nil && (p.limit <= 0 || atomic.LoadInt32(&p.running) < p.limit) {
		w = p.newWorker()
	}
	return w
}

func (p *pool) newWorker() Worker {
	w := p.ap.Get().(*poolWorker)
	atomic.AddInt32(&p.running, 1)
	go func() {
		defer func() {
			if err := recover(); err != nil && p.logger != nil {
				p.logger.Errorf("limiter-pool-worker panic: %v, stack: %s", err, stack())
			}
			p.ap.Put(w)
			atomic.AddInt32(&p.running, -1)
		}()
		for task := range w.tasks {
			if task == nil {
				return
			}
			w.Run(task)
			w.last = time.Now()
			p.wr <- w
		}
	}()
	return w
}
