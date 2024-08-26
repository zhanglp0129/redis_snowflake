package redis_snowflake

import (
	"fmt"
	"github.com/redis/go-redis/v9"
	"github.com/zhanglp0129/snowflake"
	"math/rand"
	"sync"
	"testing"
	"time"
)

func getTestWorker() snowflake.WorkerInterface {
	config := snowflake.DefaultConfig
	startTime, err := time.Parse("2006-01-02 15:04:05", "2024-08-14 00:00:00")
	if err != nil {
		panic(err)
	}
	config.SetStartTime(startTime)

	rdb := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6379",
	})
	rw, err := NewRedisWorker(rdb, "key", "lock_key", config, 0)
	if err != nil {
		panic(err)
	}
	return rw
}

func TestNewRedisWorker(t *testing.T) {
	getTestWorker()
}

func TestGenerateId(t *testing.T) {
	worker := getTestWorker()
	id, err := worker.GenerateId()
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(id)
}

func TestGenerateIdConcurrency(t *testing.T) {
	var wg sync.WaitGroup
	var idSet sync.Map       // 所有生成的id
	numProcesses := 3        // 模拟进程数量
	numGoroutines := 20      // 每个进程启动的协程数量
	numIdsPerGoroutine := 10 // 每个协程生成id数量

	for i := 0; i < numProcesses; i++ {
		worker := getTestWorker()
		for j := 0; j < numGoroutines; j++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for k := 0; k < numIdsPerGoroutine; k++ {
					id, err := worker.GenerateId()
					if err != nil {
						t.Errorf("expected no error, got %v", err)
						continue
					}
					if _, loaded := idSet.LoadOrStore(id, true); loaded {
						t.Errorf("duplicate id found: %d", id)
					}
					fmt.Printf("%d\r", id)
					time.Sleep(time.Duration(100+rand.Int()%400) * time.Millisecond)
				}
			}()
			time.Sleep(time.Duration(100+rand.Int()%400) * time.Millisecond)
		}
		time.Sleep(time.Duration(100+rand.Int()%400) * time.Millisecond)
	}

	wg.Wait()
	count := 0
	idSet.Range(func(key, value any) bool {
		count++
		return true
	})
	fmt.Printf("\n成功生成%d个id\n", count)
}

func TestGetMachineId(t *testing.T) {
	config := snowflake.DefaultConfig
	startTime, err := time.Parse("2006-01-02 15:04:05", "2024-08-14 00:00:00")
	if err != nil {
		panic(err)
	}
	config.SetStartTime(startTime)
	rdb := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6379",
	})

	workers := make([]snowflake.WorkerInterface, 0, 16)
	for i := 0; i < 16; i++ {
		rw, err := NewRedisWorker(rdb, fmt.Sprintf("key%d", i), fmt.Sprintf("lock_key%d", i), config, int64(i))
		if err != nil {
			t.Fatal(err)
		}
		workers = append(workers, rw)
	}

	// 生成id
	for i, worker := range workers {
		for j := 0; j < 100; j++ {
			id, err := worker.GenerateId()
			if err != nil {
				t.Fatal(err)
			}
			machineId, err := snowflake.GetMachineId(config, id)
			if machineId != int64(i) {
				t.Errorf("机器码错误, %d", machineId)
			}
		}
	}
}
