package limiter

import (
	"container/heap"
	"context"
	"sync/atomic"
	"time"
)

type PriorityPool interface {
	Run() error
	Stop() error
	Submit(context.Context, PriorityTask) error
	SubmitChan() chan<- PriorityTask
}

type PriorityPoolOption func(*priorityPool)

func PriorityPoolOptionWithCompare(compare PriorityCompare) PriorityPoolOption {
	return func(p *priorityPool) {
		p.compare = compare
	}
}

func PriorityPoolOptionWithLogger(logger Logger) PriorityPoolOption {
	return func(p *priorityPool) {
		p.logger = logger
	}
}

type priorityPool struct {
	gp      Pool
	compare PriorityCompare
	logger  Logger
	wt      chan PriorityTask
	tasks   *priorityTasks
	status  int32
}

func NewPriorityPool(gp Pool, opts ...PriorityPoolOption) *priorityPool {
	p := &priorityPool{
		gp: gp,
		wt: make(chan PriorityTask, 128),
	}
	for _, opt := range opts {
		opt(p)
	}
	if p.compare == nil {
		p.compare = NewDefaultCompare()
	}
	p.tasks = NewPriorityTasks(128, p.compare)
	return p
}

func (p *priorityPool) Run() error {
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

func (p *priorityPool) Stop() error {
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

func (p *priorityPool) Submit(ctx context.Context, pt PriorityTask) error {
	if p.Stopped() {
		return ErrNotRunning
	}
	select {
	case p.wt <- pt:
	case <-ctx.Done():
		return ErrTimeout
	}
	return nil
}

func (p *priorityPool) SubmitChan() chan<- PriorityTask {
	if p.Stopped() {
		return nil
	}
	return p.wt
}

func (p *priorityPool) Running() bool {
	return atomic.LoadInt32(&p.status) == int32(statusRunning)
}

func (p *priorityPool) Stopped() bool {
	return !p.Running()
}

func (p *priorityPool) run() {
	defer func() {
		atomic.StoreInt32(&p.status, int32(statusForceSTW))
		if err := recover(); err != nil && p.logger != nil {
			p.logger.Errorf("limiter-pool acquire panic: %v, stack: %s", err, stack())
		}
	}()
	var (
		ctx  context.Context
		task TaskJob
		wc   = p.gp.SubmitChan()
	)
	for {
		wc = p.gp.SubmitChan()
		ctx = context.Background()
		task = nil
		if p.tasks.Len() > 0 {
			pt := p.tasks.Peek().(PriorityTask)
			task = p.decorateTask(pt)
			if pt.Context() != nil {
				ctx = pt.Context()
			}
		}
		if task == nil {
			wc = nil
		}
		select {
		case pt := <-p.wt:
			// 优先级变化
			heap.Push(p.tasks, pt)
		case wc <- task:
			heap.Pop(p.tasks)
		case <-ctx.Done():
			// 超时
			heap.Pop(p.tasks)
		}
	}
}

func (p *priorityPool) decorateTask(pt PriorityTask) TaskJob {
	return func() {
		if p.logger != nil {
			p.logger.Infof("start priority task: %s", pt.Name())
		}
		st := time.Now()
		pt.Task()()
		if p.logger != nil {
			p.logger.Infof("end priority task: %s, cost: %s", pt.Name(), time.Now().Sub(st))
		}
	}
}
