RED_UPSERT_TOKEN_HOLDER_SCRIPT = r"""
local in_value_zset        = KEYS[1] -- store input value
local ou_value_zset        = KEYS[2] -- store output value
local in_count_zset        = KEYS[3] -- store input count
local ou_count_zset        = KEYS[4] -- store output count
local v_remain_zset        = KEYS[5] -- store value remained == in_value - ou_value
local c_remain_zset        = KEYS[6] -- store count remained == in_count - ou_count
local l_blknum_hash        = KEYS[7] -- store last updated block number


-- updated via block
local token           = ARGV[1]
local address         = ARGV[2]
local c_blknum        = tonumber(ARGV[3])
local recv_value      = tonumber(ARGV[4])
local send_value      = tonumber(ARGV[5])
local recv_count      = tonumber(ARGV[6])
local send_count      = tonumber(ARGV[7])

local l_blknum = tonumber(redis.pcall('HGET', l_blknum_hash, address) or 0)
if l_blknum >= c_blknum then
    return 0
end

if recv_value > 0 then
    redis.pcall("ZINCRBY", in_value_zset, recv_value, address)
end
if send_value > 0 then
    redis.pcall("ZINCRBY", ou_value_zset, send_value, address)
end
if recv_count > 0 then
    redis.pcall("ZINCRBY", in_count_zset, recv_count, address)
end
if send_count > 0 then
    redis.pcall("ZINCRBY", ou_count_zset, send_count, address)
end

local v_remain = redis.pcall("ZINCRBY", v_remain_zset, recv_value - send_value, address)
if tonumber(v_remain) <= 0 then
    redis.pcall("ZREM", v_remain_zset, address)
end

local c_remain = redis.pcall("ZINCRBY", c_remain_zset, recv_count - send_count, address)
if tonumber(c_remain) <= 0 then
    redis.pcall("ZREM", c_remain_zset, address)
end

redis.pcall("HSET", l_blknum_hash, token, c_blknum)

return 1
"""
