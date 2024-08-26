package redis_snowflake

import (
	"github.com/redis/go-redis/v9"
	"github.com/zhanglp0129/snowflake"
)

type RedisWorkerNoLock struct {
	rdb redis.UniversalClient
	key string
}

// NewRedisWorkerNoLock 创建一个不使用分布式锁的雪花算法redis工作节点；rdb，redis实例
// key，存储在redis中，雪花id生成参数对应的key； config，雪花算法相关配置；machineId，机器码
func NewRedisWorkerNoLock(rdb redis.UniversalClient, key string, config snowflake.SnowFlakeConfig, machineId int64) (snowflake.WorkerInterface, error) {
	return nil, nil
}

func (w *RedisWorkerNoLock) GenerateId() (int64, error) {
	return 0, nil
}
