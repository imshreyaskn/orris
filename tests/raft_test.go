package tests

import (
	"fmt"
	"sync"
	"testing"

	"orris/internal/kv"
)

func TestKVStoreConcurrency(t *testing.T) {
	store := kv.NewStore()
	var wg sync.WaitGroup
	workers := 20
	iterations := 100

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				key := fmt.Sprintf("key_%d_%d", workerID, i%10)
				val := fmt.Sprintf("val_%d_%d", workerID, i)
				store.Set(key, val)
				v, ok := store.Get(key)
				if !ok || v == "" {
					t.Errorf("Get failed for %s", key)
				}
				if i%5 == 0 {
					store.Delete(key)
				}
				_ = store.GetAll()
			}
		}(w)
	}

	wg.Wait()
}
