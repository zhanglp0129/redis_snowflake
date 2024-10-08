package redis_snowflake

import (
	"context"
	"github.com/bsm/redislock"
	"github.com/redis/go-redis/v9"
	"github.com/zhanglp0129/snowflake"
	"reflect"
	"slices"
	"time"
)

const (
	// 分布式锁过期时间为500ms
	lockTTL time.Duration = 500 * time.Millisecond
)

type RedisWorker struct {
	rdb     redis.UniversalClient
	key     string
	lockKey string
}

// NewRedisWorker 创建一个雪花算法的redis工作节点；rdb，redis实例
// key，存储在redis中，雪花id生成参数对应的key；lockKey，分布式锁对应的key；
// config，雪花算法相关配置；machineId，机器码
func NewRedisWorker(rdb redis.UniversalClient, key, lockKey string, config snowflake.SnowFlakeConfig, machineId int64) (snowflake.WorkerInterface, error) {
	// 加锁
	lock, err := redislock.New(rdb).Obtain(context.Background(), lockKey, lockTTL, &redislock.Options{
		RetryStrategy: redislock.ExponentialBackoff(50*time.Millisecond, 100*time.Millisecond),
	})
	if err != nil {
		return nil, err
	}
	defer lock.Release(context.Background())

	// 先判断redis中是否有相关配置
	typ, err := rdb.Type(context.Background(), key).Result()
	if err != nil {
		return nil, err
	} else if typ == "hash" {
		// 判断是否存在必要字段，只有当err==nil且必要字段都存在时，才会直接返回；否则都会重新创建
		fields, err := rdb.HKeys(context.Background(), key).Result()
		if err == nil {
			fg := true
			rTyp := reflect.TypeOf(redisModel{})
			for i := 0; i < rTyp.NumField(); i++ {
				// 不存在必要字段
				if !slices.Contains(fields, rTyp.Field(i).Name) {
					fg = false
					break
				}
			}
			if fg {
				return &RedisWorker{
					rdb:     rdb,
					key:     key,
					lockKey: lockKey,
				}, nil
			}
		}
	}

	// 检查配置
	sumBits := config.TimestampBits + config.MachineIdBits + config.SeqBits
	if sumBits != 63 {
		return nil, snowflake.BitsSumError
	}

	// 检查机器码
	var machineMax int64 = (1 << config.MachineIdBits) - 1
	if machineId < 0 || machineId > machineMax {
		return nil, snowflake.MachineIdIllegal
	}

	// 创建初始redis数据模型
	model := redisModel{
		Timestamp:       time.Now().UnixMilli() - config.StartTimestamp,
		TimestampMax:    (1 << config.TimestampBits) - 1,
		TimestampOffset: config.SeqBits + config.MachineIdBits,
		MachineId:       machineId,
		MachineIdOffset: config.SeqBits,
		Seq:             0,
		SeqMax:          (1 << config.SeqBits) - 1,
		SeqOffset:       0,
	}

	// 写入数据模型到redis
	err = writeModel(rdb, key, &model)
	if err != nil {
		return nil, err
	}

	return &RedisWorker{
		rdb:     rdb,
		key:     key,
		lockKey: lockKey,
	}, nil
}

// GenerateId 生成雪花id
func (w *RedisWorker) GenerateId() (int64, error) {
	// 加锁
	lock, err := redislock.New(w.rdb).Obtain(context.Background(), w.lockKey, lockTTL, &redislock.Options{
		RetryStrategy: redislock.ExponentialBackoff(100*time.Millisecond, 200*time.Millisecond),
	})
	if err != nil {
		return 0, err
	}
	defer lock.Release(context.Background())

	// 获取相关参数
	var model redisModel
	err = readModel(w.rdb, w.key, &model)
	if err != nil {
		return 0, err
	}

	// 生成id
	id, err := getId(&model)
	if err != nil {
		return 0, err
	}

	// 更新参数
	if model.Seq == model.SeqMax {
		err = w.rdb.HMSet(context.Background(), w.key, "Seq", 0, "Timestamp", model.Timestamp+1).Err()
		if err != nil {
			return 0, err
		}
	} else {
		err = w.rdb.HIncrBy(context.Background(), w.key, "Seq", 1).Err()
		if err != nil {
			return 0, err
		}
	}

	return id, nil
}

// 根据参数获取id
func getId(m *redisModel) (int64, error) {
	// 先校验参数
	if m.Timestamp < 0 || m.Timestamp > m.TimestampMax {
		return 0, snowflake.TimestampIllegal
	}
	if m.Seq < 0 || m.Seq > m.SeqMax {
		return 0, snowflake.SeqIllegal
	}

	// 生成id
	var id int64
	id |= m.Timestamp << m.TimestampOffset
	id |= m.MachineId << m.MachineIdOffset
	id |= m.Seq << m.SeqOffset

	return id, nil
}
