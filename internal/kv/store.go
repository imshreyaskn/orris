package kv

import "sync"

type Store struct {
	mu   sync.RWMutex
	data map[string]string
}

func NewStore() *Store {
	return &Store{data: make(map[string]string)}
}

func (s *Store) Set(k, v string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[k] = v
}

func (s *Store) Delete(k string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, k)
}

func (s *Store) Get(k string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[k]
	return v, ok
}

func (s *Store) GetAll() map[string]string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	copy := make(map[string]string, len(s.data))
	for k, v := range s.data {
		copy[k] = v
	}
	return copy
}

