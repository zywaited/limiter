package limiter

import (
	"time"
)

func PoolOptionsWithLimit(limit int32) PoolOptions {
	return func(p *pool) {
		p.initLimit = limit
	}
}

func PoolOptionsWithIdle(idle int) PoolOptions {
	return func(p *pool) {
		p.idle = idle
	}
}

func PoolOptionsWithIdleTime(t time.Duration) PoolOptions {
	return func(p *pool) {
		if t > 0 {
			p.idleTime = t
		}
	}
}

func PoolOptionsWithCheckNum(num int) PoolOptions {
	return func(p *pool) {
		p.checkNum = num
	}
}

func PoolOptionsWithLogger(logger Logger) PoolOptions {
	return func(p *pool) {
		p.logger = logger
	}
}

func PoolOptionsWithWorkerCreator(wc WorkerCreator) PoolOptions {
	return func(p *pool) {
		p.wc = wc
	}
}

func PoolOptionsWithBlockTime(t time.Duration) PoolOptions {
	return func(p *pool) {
		p.blockTime = t
	}
}

func PoolOptionsWithSpanNum(spanNum int) PoolOptions {
	return func(p *pool) {
		p.spanNum = spanNum
	}
}
