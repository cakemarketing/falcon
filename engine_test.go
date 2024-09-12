package falcon

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"testing"
	"time"
)

type Event struct {
	ProcessId string `json:"process_id"`
}

func TestState(t *testing.T) {
	state := NewState[string, any]()
	state.Set("foo", "bar")

	substate := NewState[string, any]()
	state.Set("sub", substate)
	substate.Set("baz", "boo")

	log.Println(state.String())
}

func TestEngineMaxWorkers(t *testing.T) {
	e := NewEngine().WithMaxWorkers(2).WithConfig(&Config{
		Job: func(w *Worker) error {
			message, _ := w.state.Get("message")
			fmt.Printf("%s: worker %d starting working on %s\n", time.Now(), w.GetId(), message)
			time.Sleep(time.Second * 3)
			fmt.Printf("%s: worker %d done worging on %s\n", time.Now(), w.GetId(), message)
			return nil
		},
		OnError: func(e error, w *Worker) {
			fmt.Printf("%s: OnError worker %d: %s\n", time.Now(), w.GetId(), e)
		},
	}).Start()
	defer e.Close()

	for i := 1; i < 6; i++ {
		fmt.Println(time.Now(), "Queueing", i)
		e.Queue(i)
		fmt.Println(time.Now(), "Queued", i)
		for w := range e.GetWorkers() {
			fmt.Printf("%s: worker %d: %s\n", time.Now(), w.GetId(), w.state)
		}
	}
}

func TestWaitingWorkersCount(t *testing.T) {
	e := NewEngine().WithMaxWorkers(5).WithConfig(&Config{
		Job: func(w *Worker) error {
			delay := time.Millisecond * time.Duration(rand.Float32()*100)
			time.Sleep(delay)
			return nil
		},
		OnSuccess: func(w *Worker) {
			fmt.Println("worker", w.GetId(), "done")
		},
	}).Start()

	go func() {
		time.Sleep(20 * time.Second)
		e.cancel()
	}()
	
	start := time.Now()
	for time.Since(start) < time.Second {
		availWorkers := e.WaitingWorkersCount()
		fmt.Println(time.Now(), "Available workers:", availWorkers)
		if availWorkers > 0 {
			e.Queue(time.Now().UnixMilli())
		}
		time.Sleep(10 * time.Millisecond)
	}

}

func TestEngine(t *testing.T) {
	engine := NewEngine().WithMaxWorkers(5)
	defer engine.Close()

	for i := 0; i < 10; i++ {
		w := NewWorker(engine, i)
		go engine.Receive(w)
	}

	workers1 := engine.GetWorkers()
	for w := range workers1 {
		log.Println("1-worker: ", w.GetId(), w.GetStatus())
	}

	engine.SetMaxWorkers(10)
	log.Println("set workers to 10")

	for i := 10; i < 20; i++ {
		w := NewWorker(engine, i)
		go engine.Receive(w)
	}

	workers2 := engine.GetWorkers()
	for w := range workers2 {
		log.Println("2-worker: ", w.GetId(), w.GetStatus())
	}
}

func TestEngineQueue(t *testing.T) {
	engine := NewEngine().WithMaxWorkers(5).Start()
	defer engine.Close()

	workers := engine.GetWorkers()
	for w := range workers {
		log.Println("q-worker: ", w.GetId(), w.GetStatus())
	}

	ev := &Event{ProcessId: "001"}
	engine.Queue(ev)

	ev2 := &Event{ProcessId: "002"}
	engine.Queue(ev2)
}

type Count struct {
	counter int
	mu      sync.RWMutex
}

func (c *Count) Increment() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.counter++
}
func (c *Count) MarshalJSON() ([]byte, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return json.Marshal(c.counter)
}

func TestEngineConfigError(t *testing.T) {
	wg := sync.WaitGroup{}

	engine := NewEngine().WithMaxWorkers(3).WithConfig(&Config{
		Before: func(w *Worker) error {
			time.Sleep(time.Second)
			workers := w.Parent().GetWorkers()
			for w := range workers {
				_ = w
				return errors.New("failed before check")
			}
			return nil
		},
		OnError: func(err error, w *Worker) {
			wg.Done()
			log.Println("incoming error:", err)
			log.Println("worker: ", w.String())
		},
	}).Start()
	defer engine.Close()

	go func() {
		ticker := time.NewTicker(time.Second)
		for {
			select {
			case <-ticker.C:
				log.Println(engine)
			}
		}
	}()

	for i := 0; i < 10; i++ {
		wg.Add(1)
		ev := &Event{ProcessId: fmt.Sprintf("%03d", i)}
		engine.Queue(ev)
		time.Sleep(time.Second)
	}

	log.Println("waiting for all jobs to finish")
	wg.Wait()

	log.Println(engine)
}

func TestEngineConfig(t *testing.T) {
	wg := sync.WaitGroup{}

	engine := NewEngine().WithMaxWorkers(3).WithConfig(&Config{
		Before: func(w *Worker) error {
			workers := w.Parent().GetWorkers()
			for pw := range workers {
				if w.GetId() == pw.GetId() {
					continue
				}

				state, ok := w.GetState("message")
				if !ok {
					return errors.New("no message state")
				}
				event := state.(*Event)

				log.Println("before worker:", w.GetId(), "compare:", pw.GetId(), "process:", event.ProcessId)
			}
			return nil
		},
		Job: func(w *Worker) error {
			st, ok := w.GetState("message")
			if !ok {
				return errors.New("couldn't find state")
			}
			log.Println("job code from worker:", w.GetId(), "event:", st)

			counter := &Count{}
			w.SetState("counter", counter)

			go func() {
				for i := 0; i < 5; i++ {
					counter.Increment()
					time.Sleep(time.Second)
				}
			}()

			time.Sleep(time.Duration(rand.Intn(10)) * time.Second)
			log.Println(w)
			return nil
		},
		OnSuccess: func(w *Worker) {
			wg.Done()

			st, _ := w.GetState("message")
			log.Println("success from worker:", w.GetId(), "event:", st)
			log.Println("worker: ", w.String())
		},
	}).Start()
	defer engine.Close()

	go func() {
		ticker := time.NewTicker(time.Second)
		for {
			select {
			case <-ticker.C:
				log.Println(engine)
			}
		}
	}()

	for i := 0; i < 10; i++ {
		wg.Add(1)
		ev := &Event{ProcessId: fmt.Sprintf("%03d", i)}
		engine.Queue(ev)
		time.Sleep(time.Second)
	}

	log.Println("waiting for all jobs to finish")
	wg.Wait()

	log.Println(engine)
}

func BenchmarkEngineQueue(b *testing.B) {
	engine := NewEngine().WithMaxWorkers(5).Start()
	defer engine.Close()

	b.ResetTimer()
	b.Run("queue message", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			ev := &Event{ProcessId: fmt.Sprintf("%d", i)}
			engine.Queue(ev)
		}
	})
}
