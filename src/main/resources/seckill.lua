local voucherId = ARGV[1]
local userId = ARGV[2]

local stockKey = "seckill:stock:" .. voucherId
local orderKey = "seckill:order:" .. voucherId

local stockValue = tonumber(redis.call('get', stockKey))
if (stockValue == nil or stockValue <= 0) then
    return 1
end

if (redis.call('sismember', orderKey, userId) == 1) then
    return 2
end

redis.call('incrby', stockKey, -1);
redis.call('sadd', orderKey, userId);
return 0