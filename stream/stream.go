package stream

import "sync"

// Message represents a sequenced message.
type Message interface {
	Done()
}

// Message represents a message.
type message struct {
	inIdx    int
	finishCb func(inIdx int)
}

// Done marks the message as fully processed.
func (m *message) Done() {
	if m.inIdx < 0 {
		return
	}
	m.finishCb(m.inIdx)
	m.inIdx = -1
}

type finishCb func(raw interface{})

type inflightStore struct {
	entry  interface{}
	active bool
}

// SequencedStore manages a sliding window of messages.
type SequencedStore struct {
	inflight []inflightStore
	stIdx    int
	enIdx    int
	cb       finishCb

	cv *sync.Cond
}

// NewSequencedStore creates a new manager with the specified buffer size.
func NewSequencedStore(bufferSize int, cb finishCb) *SequencedStore {
	m := &SequencedStore{
		inflight: make([]inflightStore, bufferSize),
		cb:       cb,

		cv: sync.NewCond(&sync.Mutex{}),
	}
	return m
}

// GetSequencedMessage returns a sequenced message with no data.
func (s *SequencedStore) GetSequencedMessage(raw interface{}) Message {
	s.cv.L.Lock()
	defer s.cv.L.Unlock()
	for s.enIdx+1%len(s.inflight) == s.stIdx {
		s.cv.Wait()
	}

	s.inflight[s.enIdx].active = true
	s.inflight[s.enIdx].entry = raw
	msg := &message{
		inIdx:    s.enIdx,
		finishCb: s.finish,
	}
	s.enIdx = s.enIdx + 1%len(s.inflight)
	return msg
}

func (s *SequencedStore) finish(inIdx int) {
	s.cv.L.Lock()
	defer s.cv.L.Unlock()

	s.inflight[inIdx].active = false
	s.sweep()
}

func (s *SequencedStore) sweep() {
	// SYS: NO LOCK

	for s.stIdx < s.enIdx && !s.inflight[s.stIdx].active {
		s.cb(s.inflight[s.stIdx].entry)
		s.inflight[s.stIdx].entry = nil
		s.stIdx = s.stIdx + 1%len(s.inflight)
	}
}
