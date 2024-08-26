package redis_snowflake

import (
	"context"
	"github.com/redis/go-redis/v9"
	"github.com/zhanglp0129/snowflake"
	"reflect"
	"strconv"
	"time"
)

var (
	NewWorkerLuaScript  string
	GenerateIdLuaScript string
)

func init() {
	// 加载lua脚本
	workerLuaBytes, err := LuaFS.ReadFile("lua/new_worker.lua")
	if err != nil {
		panic(err)
	}
	NewWorkerLuaScript = string(workerLuaBytes)
	generateLuaBytes, err := LuaFS.ReadFile("lua/generate_id.lua")
	if err != nil {
		panic(err)
	}
	GenerateIdLuaScript = string(generateLuaBytes)
}

type RedisWorkerNoLock struct {
	rdb redis.UniversalClient
	key string
}

// NewRedisWorkerNoLock 创建一个不使用分布式锁的雪花算法redis工作节点；rdb，redis实例
// key，存储在redis中，雪花id生成参数对应的key； config，雪花算法相关配置；machineId，机器码
func NewRedisWorkerNoLock(rdb redis.UniversalClient, key string, config snowflake.SnowFlakeConfig, machineId int64) (snowflake.WorkerInterface, error) {
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
	// 将数据模型转换为切片，偶数下标为字段名，奇数下标为字段值
	rVal := reflect.ValueOf(model)
	rTyp := rVal.Type()
	n := rTyp.NumField()
	values := make([]any, 0, 2*n)
	for i := 0; i < n; i++ {
		values = append(values, rTyp.Field(i).Name, rVal.Field(i).Interface())
	}

	// 执行lua脚本，写入参数
	err := rdb.Eval(context.Background(), NewWorkerLuaScript, []string{key}, values...).Err()
	if err != nil {
		return nil, err
	}

	return &RedisWorkerNoLock{
		rdb: rdb,
		key: key,
	}, nil
}

func (w *RedisWorkerNoLock) GenerateId() (int64, error) {
	// 执行lua脚本，获取参数并自增。values偶数下标为字段名，奇数下标为字段值
	values, err := w.rdb.Eval(context.Background(), GenerateIdLuaScript, []string{w.key}).StringSlice()
	if err != nil {
		return 0, err
	}

	// 解析参数
	var model redisModel
	rVal := reflect.ValueOf(&model).Elem()
	for i := 0; i < len(values); i += 2 {
		field := rVal.FieldByName(values[i])
		if field.CanInt() {
			t, err := strconv.ParseInt(values[i+1], 10, 64)
			if err != nil {
				return 0, err
			}
			field.SetInt(t)
		} else if field.CanUint() {
			t, err := strconv.ParseUint(values[i+1], 10, 64)
			if err != nil {
				return 0, err
			}
			field.SetUint(t)
		}
	}

	// 生成id
	return getId(&model)
}
