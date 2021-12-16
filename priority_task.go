package limiter

import "context"

type PriorityTask interface {
	Context() context.Context
	Name() string
	Task() TaskJob
}

type PriorityCompare interface {
	Equal(PriorityTask, PriorityTask) bool
	Compare(PriorityTask, PriorityTask) bool
}

type defaultPriorityTask struct {
	ctx      context.Context
	name     string
	task     TaskJob
	priority int64
}

func NewDefaultPriorityTask() *defaultPriorityTask {
	return &defaultPriorityTask{ctx: context.Background()}
}

func (task *defaultPriorityTask) Init(
	ctx context.Context,
	name string,
	tj TaskJob,
	priority int64,
) *defaultPriorityTask {
	if ctx != nil {
		task.ctx = ctx
	}
	task.name = name
	task.task = tj
	task.priority = priority
	return task
}

func (task *defaultPriorityTask) Name() string {
	return task.name
}

func (task *defaultPriorityTask) Task() TaskJob {
	return task.task
}

func (task *defaultPriorityTask) Context() context.Context {
	return task.ctx
}

func (task *defaultPriorityTask) Priority() int64 {
	return task.priority
}

type defaultPriorityCompare interface {
	Priority() int64
}

type defaultCompare struct {
}

func NewDefaultCompare() *defaultCompare {
	return &defaultCompare{}
}

func (compare *defaultCompare) Equal(f, s PriorityTask) bool {
	return f == s
}

func (compare *defaultCompare) Compare(f, s PriorityTask) bool {
	df, ok := f.(defaultPriorityCompare)
	if !ok {
		return false
	}
	sf, ok := s.(defaultPriorityCompare)
	if !ok {
		return true
	}
	return df.Priority() > sf.Priority()
}

type priorityTasks struct {
	tasks   []PriorityTask
	compare PriorityCompare
}

func NewPriorityTasks(cap int, compare PriorityCompare) *priorityTasks {
	return &priorityTasks{
		tasks:   make([]PriorityTask, 0, cap),
		compare: compare,
	}
}

func (p *priorityTasks) Len() int {
	return len(p.tasks)
}

func (p *priorityTasks) Less(i, j int) bool {
	return p.compare.Compare(p.tasks[i], p.tasks[j])
}

func (p *priorityTasks) Swap(i, j int) {
	p.tasks[i], p.tasks[j] = p.tasks[j], p.tasks[i]
}

func (p *priorityTasks) Push(t interface{}) {
	p.tasks = append(p.tasks, t.(PriorityTask))
}

func (p *priorityTasks) Pop() interface{} {
	t := p.tasks[p.Len()-1]
	p.tasks = p.tasks[:p.Len()-1]
	return t
}

func (p *priorityTasks) Peek() interface{} {
	return p.tasks[0]
}
