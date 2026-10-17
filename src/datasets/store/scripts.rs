//! Short fixed scripts; application values are arguments, never executable code.

pub(super) const REVISION: &str = r#"
if redis.call('HSTRLEN', KEYS[1], 'revision') > 20 then return false end
local revision = redis.call('HGET', KEYS[1], 'revision') or '0'
if #revision > 18 or (revision ~= '0' and not string.match(revision, '^[1-9]%d*$')) then return false end
return revision
"#;

// A changed revision satisfies concurrent startup waiters. Scheduled callers
// check the shared due time so replicas cannot refresh in succession.
pub(super) const CLAIM: &str = r#"
if redis.call('HSTRLEN', KEYS[1], 'revision') > 20 then return {-1} end
local revision = redis.call('HGET', KEYS[1], 'revision') or '0'
if #revision > 18 or (revision ~= '0' and not string.match(revision, '^[1-9]%d*$')) then return {-1} end
local time = redis.call('TIME')
local now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
if ARGV[1] ~= '' then
  if revision ~= ARGV[1] and redis.call('EXISTS', KEYS[3]) == 1 then return {0} end
else
  if redis.call('HSTRLEN', KEYS[1], 'next_refresh_at_ms') > 20 then return {-1} end
  local due = tonumber(redis.call('HGET', KEYS[1], 'next_refresh_at_ms') or '0')
  if not due then return {-1} end
  if due > now then return {0} end
end
if redis.call('EXISTS', KEYS[2]) == 1 then return {1} end
redis.call('HINCRBY', KEYS[1], 'attempt', 1)
local token = redis.call('HGET', KEYS[1], 'attempt') .. ':' .. time[1] .. ':' .. time[2]
redis.call('SET', KEYS[2], token, 'PX', ARGV[2])
-- A lost worker still leaves a shared retry time after its lease expires.
redis.call('HSET', KEYS[1], 'next_refresh_at_ms', now + tonumber(ARGV[2]) + tonumber(ARGV[3]))
return {2, token, revision}
"#;

pub(super) const BEGIN: &str = r#"
redis.call('HSET', KEYS[1], '', '1')
redis.call('PEXPIRE', KEYS[1], ARGV[1])
return 1
"#;

pub(super) const PUBLISH: &str = r#"
if redis.call('GET', KEYS[2]) ~= ARGV[1] then return false end
local revision = redis.call('HGET', KEYS[1], 'revision') or '0'
if revision ~= ARGV[2] or #revision > 18 or
   (revision ~= '0' and not string.match(revision, '^[1-9]%d*$')) then return false end
if redis.call('HGET', KEYS[3], '') ~= '1' then return false end
local time = redis.call('TIME')
local now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
-- Unlink before rename avoids freeing a large previous hash on the Redis thread.
redis.call('UNLINK', KEYS[4])
redis.call('RENAME', KEYS[3], KEYS[4])
redis.call('PERSIST', KEYS[4])
redis.call('HINCRBY', KEYS[1], 'revision', 1)
redis.call('HSET', KEYS[1], 'format_version', '1', 'loaded_at_ms', now,
  'next_refresh_at_ms', now + tonumber(ARGV[3]))
redis.call('DEL', KEYS[2])
return redis.call('HGET', KEYS[1], 'revision')
"#;

pub(super) const FAILED: &str = r#"
if redis.call('GET', KEYS[2]) ~= ARGV[1] then return 0 end
if (redis.call('HGET', KEYS[1], 'revision') or '0') ~= ARGV[2] then return 0 end
local time = redis.call('TIME')
local now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
redis.call('HSET', KEYS[1], 'next_refresh_at_ms', now + tonumber(ARGV[3]))
redis.call('DEL', KEYS[2])
return 1
"#;

// Check lengths before retrieving application values. Return a flat key/value
// array, skipping missing fields; {} remains distinguishable from lost data.
pub(in crate::datasets) const READ: &str = r#"
if redis.call('HSTRLEN', KEYS[1], 'format_version') ~= 1 or
   redis.call('HGET', KEYS[1], 'format_version') ~= '1' then return {-1} end
if redis.call('HSTRLEN', KEYS[2], '') ~= 1 or
   redis.call('HGET', KEYS[2], '') ~= '1' then return {-1} end
local fields = {}
if ARGV[1] == 'all' then
  if redis.call('HLEN', KEYS[2]) > 65 then return {-2} end
  fields = redis.call('HKEYS', KEYS[2])
else
  for i = 3, #ARGV do fields[#fields + 1] = ARGV[i] end
end
local size = 2
local found = 0
for _, key in ipairs(fields) do
  if key ~= '' and redis.call('HEXISTS', KEYS[2], key) == 1 then
    -- Six bytes per key byte covers JSON escaping without loading the value.
    size = size + 6 * string.len(key) + redis.call('HSTRLEN', KEYS[2], key) + 4
    found = found + 1
    if size > tonumber(ARGV[2]) or found > 64 then return {-2} end
  end
end
local result = {}
for _, key in ipairs(fields) do
  if key ~= '' then
    local value = redis.call('HGET', KEYS[2], key)
    if value then result[#result + 1] = key; result[#result + 1] = value end
  end
end
return result
"#;
