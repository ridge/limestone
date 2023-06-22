package scheduler

import (
	"sync"

	"time"

	"github.com/ridge/limestone/typeddb"
)

// A Scheduler keeps track of many upcoming alarms associated with entities,
// at most one per EID
type Scheduler struct {
	alarms map[typeddb.EID]*time.Timer
	fired  map[typeddb.EID]bool
	ch     chan struct{}
	mu     sync.Mutex
}

// New creates a snew Scheduler
func New() *Scheduler {
	return &Scheduler{
		alarms: map[typeddb.EID]*time.Timer{},
		ch:     make(chan struct{}, 1),
	}
}

// Schedule adds, modifies or removes the alarm for a given key.
// Set when to zero time to remove.
func (s *Scheduler) Schedule(key typeddb.EID, when time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()

	timer := s.alarms[key]
	if timer != nil {
		timer.Stop()
	}

	if when.IsZero() {
		if timer != nil {
			delete(s.alarms, key)
		}
	} else {
		d := time.Until(when)
		if timer != nil {
			timer.Reset(d)
		} else {
			s.alarms[key] = time.AfterFunc(d, func() {
				s.alarm(key)
			})
		}
	}
}

func (s *Scheduler) alarm(key typeddb.EID) {
	s.mu.Lock()
	if s.fired == nil {
		s.fired = map[typeddb.EID]bool{}
	}
	s.fired[key] = true
	delete(s.alarms, key)
	s.mu.Unlock()

	select {
	case s.ch <- struct{}{}:
	default:
	}
}

// Wait returns the channel where alarms are announced
func (s *Scheduler) Wait() <-chan struct{} {
	return s.ch
}

// Get returns the list of keys for which the alarm has fired
func (s *Scheduler) Get() []typeddb.EID {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.fired == nil {
		return nil
	}

	res := make([]typeddb.EID, 0, len(s.fired))
	for key := range s.fired {
		res = append(res, key)
	}
	s.fired = nil

	return res
}

// Clear removes all alarms
func (s *Scheduler) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, timer := range s.alarms {
		timer.Stop()
	}
	s.alarms = map[typeddb.EID]*time.Timer{}
	s.fired = nil
}
