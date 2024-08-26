local key = KEYS[1]
-- 获取参数
local result = redis.call("HGETALL", key)

-- 获取Seq和SeqMax
local ss = redis.call("HMGET", key, "Seq", "SeqMax")
if ss[1] == ss[2] then
    -- 序列号达到最大值
    redis.call("HSET", key, "Seq", 0)
    redis.call("HINCRBY", key, "Timestamp", 1)
else
    redis.call("HINCRBY", key, "Seq", 1)
end

return result