# RedisSnowflake
将雪花算法生成分布式id的参数放到Redis中，并采用分布式锁，保证分布式场景下id的正确性

## 使用
1. 安装依赖
```shell
go get -u github.com/zhanglp0129/redis_snowflake
```

2. 创建雪花算法配置
```go
cfg := snowflake.DefaultConfig
startTime, _ := time.Parse("2006-01-02 15:04:05", "2024-08-14 00:00:00")
cfg.SetStartTime(startTime)
```

3. 创建redis实例
```go
rdb := redis.NewClient(&redis.Options{
    Addr: "127.0.0.1:6379",
})
defer rdb.Close()
```

4. 创建工作节点
```go
// 使用分布式锁保证并发安全
worker, err := redis_snowflake.NewRedisWorker(rdb, "key", "lock-key", cfg, 0)
// 使用lua脚本保证并发安全
worker, err := redis_snowflake.NewRedisWorkerNoLock(rdb, "key", cfg, 0)
```

5. 生成id
```go
id, err := worker.GenerateId()
```

## LICENSE

MIT