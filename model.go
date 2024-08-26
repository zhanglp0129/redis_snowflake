package redis_snowflake

import (
	"context"
	"github.com/redis/go-redis/v9"
	"reflect"
	"strconv"
)

// 存储在redis中hash类型的数据模型
type redisModel struct {
	Timestamp       int64 // 生成id的时间戳，从startTimestamp开始
	TimestampMax    int64
	TimestampOffset uint8
	MachineId       int64
	MachineIdOffset uint8
	Seq             int64
	SeqMax          int64
	SeqOffset       uint8
}

// 将模型写入到redis中
func writeModel(rdb redis.UniversalClient, key string, model *redisModel) error {
	rVal := reflect.ValueOf(model).Elem()
	rTyp := rVal.Type()
	n := rTyp.NumField()
	values := make([]any, 0, 2*n)
	for i := 0; i < n; i++ {
		values = append(values, rTyp.Field(i).Name, rVal.Field(i).Interface())
	}
	return rdb.HMSet(context.Background(), key, values...).Err()
}

// 从redis中读取模型
func readModel(rdb redis.UniversalClient, key string, model *redisModel) error {
	rVal := reflect.ValueOf(model).Elem()
	all, err := rdb.HGetAll(context.Background(), key).Result()
	if err != nil {
		return err
	}

	// 遍历读取到的数据
	for k, v := range all {
		field := rVal.FieldByName(k)
		if field.CanInt() {
			t, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				return err
			}
			field.SetInt(t)
		} else if field.CanUint() {
			t, err := strconv.ParseUint(v, 10, 64)
			if err != nil {
				return err
			}
			field.SetUint(t)
		}
	}
	return nil
}
