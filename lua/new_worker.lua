-- 先判断参数是否存在且正确
local key = KEYS[1]
local num_arg = #ARGV
if redis.call("TYPE", key) == "hash" then
    -- 判断关键字段是否存在，字段名为奇数下标
    local flag = true
    for i = 1, num_arg, 2 do
        if redis.call("HEXISTS", key, ARGV[i]) ~= 1 then
            flag = false
            break
        end
    end
    if flag then
        -- 所有关键字段均存在
        return 0
    end
end

-- 写入数据
redis.call("HMSET", key, unpack(ARGV))
return 0