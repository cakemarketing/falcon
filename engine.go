package falcon

import (
	"context"
	"encoding/json"
	"sync"
)

type maxWorkers int
type getWorkerChan chan *Worker
type shutdown struct{}

type Engine struct {
	maxWorkers int
	config     *Config `json:"-"`

	messagesProcessed int64
	workers           *State[int, *Worker]

	msgch    chan any
	workch   chan any
	ctx      context.Context
	cancel   context.CancelFunc
	shutdown bool

	// blah
	mu sync.RWMutex
}

func NewEngine() *Engine {
	ctx, cancel := context.WithCancel(context.Background())

	e := &Engine{
		config:  DefaultConfig,
		workers: NewState[int, *Worker](),
		msgch:   make(chan any),
		workch:  make(chan any),
		ctx:     ctx,
		cancel:  cancel,
	}

	go e.loop()

	return e
}

func (e *Engine) MarshalJSON() ([]byte, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	output := struct {
		MessagesProcessed int64                `json:"messages_processed"`
		Workers           *State[int, *Worker] `json:"workers"`
	}{
		MessagesProcessed: int64(e.GetProcessedCount()),
		Workers:           e.workers,
	}
	return json.Marshal(output)
}

func (e *Engine) String() string {
	b, _ := json.Marshal(e)
	return string(b)
}

func (e *Engine) WithConfig(cfg *Config) *Engine {
	e.config = cfg
	return e
}

func (e *Engine) WithMaxWorkers(max int) *Engine {
	e.maxWorkers = max
	return e
}

func (e *Engine) SetMaxWorkers(max int) {
	e.Receive(maxWorkers(max))
}

func (e *Engine) Start() *Engine {
	// set default config
	if e.config == nil {
		e.config = &Config{}
	}

	// start the pool
	// TODO: this should keep a minimum number of workers ready to work
	for i := 0; i < e.maxWorkers; i++ {
		w := NewWorker(e, i, e.config)
		e.Receive(w)
	}

	return e
}

func (e *Engine) Close() error {
	e.Receive(shutdown{})
	return nil
}

func (e *Engine) Queue(msg any) {
	e.workch <- msg
	e.mu.Lock()
	e.messagesProcessed++
	e.mu.Unlock()
}

func (e *Engine) Receive(msg any) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.shutdown {
		return
	}
	e.msgch <- msg
}

func (e *Engine) GetWorkers() <-chan *Worker {
	out := make(chan *Worker, e.workers.Len())

	go e.Receive(getWorkerChan(out))

	return out
}

// Assuming a single dispatcher per process, get the number of free/waiting workers
// Example usage: before receiving messages from a SQS queue, only retrieve as many as workers available, to prevent messages waiting for workers to free up.
func (e *Engine) WaitingWorkersCount() int {
	counter := 0
	for worker := range e.GetWorkers() {
		if worker.GetStatus() == WorkerStatusWaiting {
			counter++
		}
	}
	return counter
}

func (e *Engine) GetProcessedCount() int {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return int(e.messagesProcessed)
}

func (e *Engine) loop() {
	for msg := range e.msgch {
		// fmt.Println("------:", reflect.TypeOf(msg))
		e.handleMessage(msg)
	}
}

func (e *Engine) handleMessage(message any) {
	switch msg := message.(type) {
	case *Worker:
		e.addWorker(msg)
	case maxWorkers:
		e.updateMaxWorkers(int(msg))
	case getWorkerChan:
		e.handleGetWorkers(msg)

	case shutdown:
		// cancel context for all procs
		e.cancel()

		// shut down all channels
		e.mu.Lock()
		e.shutdown = true
		close(e.msgch)
		close(e.workch)
		e.mu.Unlock()
	}
}

func (e *Engine) handleGetWorkers(out getWorkerChan) {
	e.workers.ForEach(func(k int, w *Worker) {
		out <- w
	})
	close(out)
}

func (e *Engine) updateMaxWorkers(max int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.maxWorkers = max
}

func (e *Engine) addWorker(w *Worker) {
	if e.workers.Len() >= e.maxWorkers {
		return
	}

	w.ctx = e.ctx
	w.queue = e.workch
	w.parent = e

	e.workers.Set(w.GetId(), w)

	// fmt.Println("adding worker: ", w.Id)
	go w.work()
}
