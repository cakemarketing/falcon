package falcon

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
)

type Config struct {
	Before     func(*Worker) error
	Job        func(*Worker) error
	OnSuccess  func(*Worker)
	OnError    func(error, *Worker)
	OnComplete func(*Worker)
}

type Worker struct {
	*Config `json:"-"`

	id     int
	state  *State[string, any]
	parent *Engine

	queue chan any
	ctx   context.Context
	mu    sync.RWMutex
}

type WorkerStatus string

var (
	WorkerStatusUnknown       WorkerStatus = "unknown"
	WorkerStatusWaiting       WorkerStatus = "waiting"
	WorkerStatusPreProcessing WorkerStatus = "pre-processing"
	WorkerStatusProcessing    WorkerStatus = "processing"
	WorkerStatusSuccess       WorkerStatus = "success"
	WorkerStatusClosing       WorkerStatus = "closing"
	WorkerStatusClosed        WorkerStatus = "closed"
	WorkerStatusError         WorkerStatus = "error"
)

var DefaultConfig = &Config{
	Before: func(w *Worker) error {
		return nil
	},
	Job: func(w *Worker) error {
		return nil
	},
	OnSuccess:  func(w *Worker) {},
	OnError:    func(err error, w *Worker) {},
	OnComplete: func(w *Worker) {},
}

func NewWorker(e *Engine, id int, cfg ...*Config) *Worker {
	w := &Worker{
		Config: DefaultConfig,

		id:    id,
		state: NewState[string, any](),
	}
	if len(cfg) == 1 {
		w.Config = cfg[0]
	}
	w.state.Set("status", WorkerStatusWaiting)
	return w
}

func (w *Worker) GetId() int {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.id
}

func (w *Worker) GetStatus() WorkerStatus {
	if st, ok := w.GetState("status"); ok {
		return st.(WorkerStatus)
	}
	return WorkerStatusUnknown
}

func (w *Worker) GetState(state string) (any, bool) {
	return w.state.Get(state)
}

func (w *Worker) GetContext() context.Context {
	return w.ctx
}

func (w *Worker) SetState(state string, v any) {
	w.state.Set(state, v)
}

func (w *Worker) Parent() *Engine {
	return w.parent
}

func (w *Worker) Close() error {
	w.state.Set("status", WorkerStatusClosed)
	return nil
}

func (w *Worker) MarshalJSON() ([]byte, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	output := struct {
		Id    int                 `json:"id"`
		State *State[string, any] `json:"state"`
	}{
		Id:    w.id,
		State: w.state,
	}
	return json.Marshal(output)
}

func (w *Worker) String() string {
	b, _ := json.Marshal(w)
	return string(b)
}

func (w *Worker) work() {
	for {
		select {
		case <-w.ctx.Done():
			w.state.Set("status", WorkerStatusClosing)
			w.Close()
			return
		case msg := <-w.queue:
			if msg == nil {
				// nothing to do
				continue
			}

			// make a done channel to keep
			// track of the worker status
			done := make(chan bool)

			go func(w *Worker) {
				defer func() {
					if r := recover(); r != nil {
						w.OnError(fmt.Errorf("%v", r), w)
					}
					if w.OnComplete != nil {
						w.OnComplete(w)
					}
					w.state.Reset()
					w.state.Set("status", WorkerStatusWaiting)
					done <- true
				}()

				w.state.Set("message", msg)

				if w.Config == nil {
					// fmt.Println("no configured jobs")
					return
				}

				if w.Before != nil {
					w.state.Set("status", WorkerStatusPreProcessing)
					if err := w.Before(w); err != nil {
						w.state.Set("status", WorkerStatusError)
						w.OnError(err, w)
						return
					}
				}

				w.state.Set("status", WorkerStatusProcessing)
				if err := w.Job(w); err != nil {
					w.state.Set("status", WorkerStatusError)
					w.OnError(err, w)
					return
				}

				// fmt.Println("on success:")
				w.state.Set("status", WorkerStatusSuccess)
				if w.OnSuccess != nil {
					w.OnSuccess(w)
				}
			}(w)

			// wait for the job to finish
			<-done
		}
	}
}
