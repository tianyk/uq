package store

import (
	"errors"
	"sync"
)

// 基于内存存储
type MemStore struct {
	mu sync.RWMutex // 读写互斥锁 该锁可以被同时多个读取者持有或唯一个写入者持有
	db map[string][]byte
}

func NewMemStore() (*MemStore, error) {
	// 基于map实现K/V
	db := make(map[string][]byte)
	ms := new(MemStore)
	ms.db = db

	return ms, nil
}

func (m *MemStore) Get(key string) ([]byte, error) {
	m.mu.RLock() // RLock方法将rw锁定为读取状态，禁止其他线程写入，但不禁止读取。(此时可读，不可写)
	defer m.mu.RUnlock()

	data, ok := m.db[key]
	if !ok {
		return nil, errors.New(ErrNotExisted)
	}
	return data, nil
}

func (m *MemStore) Set(key string, data []byte) error {
	m.mu.Lock() // Lock方法将rw锁定为写入状态，禁止其他线程读取或者写入。(此时既不可读，也不可写)
	defer m.mu.Unlock()

	m.db[key] = data
	return nil
}

func (m *MemStore) Del(key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	_, ok := m.db[key]
	if !ok {
		return errors.New(ErrNotExisted)
	}

	delete(m.db, key)
	return nil
}

func (m *MemStore) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.db = nil

	return nil
}
