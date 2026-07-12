package store

import "github.com/redis/go-redis/v9"

// upsertLeaderboardMemberScript applies a CDC rating using generations from meta.
// Fencing uses positive board projectionVersion (Postgres dirty_version), not wall-clock.
// Return codes: 0=stale/noop, 1=applied, 2=idempotent equal version, 3=conflict.
// Never invents a live generation. When live exists, staging is dual-written only if
// the live apply accepts (1) or idempotent duplicate (2) — stale live rejects must
// not poison empty staging. Rebuild-only mode fences on staging applied alone.
// After a successful live apply, memberCount is refreshed from live ZCARD.
// Meta projectionVersion becomes max(current, eventVersion) — never +1 per player.
//
// KEYS[1]=meta
// ARGV[1]=boardRoot ARGV[2]=playerId ARGV[3]=score ARGV[4]=projectionVersion
// ARGV[5]=previousRating ARGV[6]=generatedAtRFC3339
var upsertLeaderboardMemberScript = redis.NewScript(`
local meta = KEYS[1]
local root = ARGV[1]
local playerId = ARGV[2]
local score = tonumber(ARGV[3])
local eventVer = tonumber(ARGV[4])
local prevRating = tonumber(ARGV[5])
local generatedAt = ARGV[6]
local newRating = -score

local function apply(zkey, akey)
  if (not eventVer) or eventVer <= 0 then
    return 3
  end
  local curRaw = redis.call('HGET', akey, playerId)
  local curVer = 0
  if curRaw and curRaw ~= false then
    curVer = tonumber(curRaw) or 0
  end
  if curVer > eventVer then
    return 0
  end
  local curScore = redis.call('ZSCORE', zkey, playerId)
  local curRating = nil
  if curScore and curScore ~= false then
    curRating = -tonumber(curScore)
  end
  if curVer == eventVer then
    if curRating ~= nil and curRating == newRating then
      redis.call('ZADD', zkey, score, playerId)
      return 2
    end
    return 3
  end
  -- eventVer > curVer
  if curRating ~= nil then
    if curRating == newRating then
      -- Rebuild (or prior apply) already has target; advance fence.
      redis.call('ZADD', zkey, score, playerId)
      redis.call('HSET', akey, playerId, tostring(eventVer))
      return 1
    end
    if curRating ~= prevRating then
      return 3
    end
  end
  redis.call('ZADD', zkey, score, playerId)
  redis.call('HSET', akey, playerId, tostring(eventVer))
  return 1
end

local gen = redis.call('HGET', meta, 'generation')
local rebuilding = redis.call('HGET', meta, 'rebuilding_gen')
local ready = redis.call('HGET', meta, 'ready')
local hasLive = ready and ready == '1' and gen and gen ~= false and gen ~= '' and tonumber(gen) >= 1
local hasRebuild = rebuilding and rebuilding ~= false and rebuilding ~= ''

if (not hasLive) and (not hasRebuild) then
  return 0
end

local applied = 0
if hasLive then
  local liveZ = root .. 'gen:' .. gen .. ':z'
  applied = apply(liveZ, root .. 'gen:' .. gen .. ':applied')
  -- Dual-write staging only when live accepted (not stale/conflict).
  if hasRebuild and rebuilding ~= gen and (applied == 1 or applied == 2) then
    apply(root .. 'gen:' .. rebuilding .. ':z', root .. 'gen:' .. rebuilding .. ':applied')
  end
  if applied == 1 or applied == 2 then
    redis.call('HSET', meta, 'memberCount', tostring(redis.call('ZCARD', liveZ)))
  end
elseif hasRebuild then
  -- Rebuild-only: fence against staging applied hash only. Never sets ready.
  applied = apply(root .. 'gen:' .. rebuilding .. ':z', root .. 'gen:' .. rebuilding .. ':applied')
end

if applied == 1 then
  local curMetaVer = tonumber(redis.call('HGET', meta, 'projectionVersion') or '0') or 0
  if eventVer > curMetaVer then
    redis.call('HSET', meta, 'projectionVersion', tostring(eventVer))
  end
  redis.call('HSET', meta, 'generatedAt', generatedAt)
end
return applied
`)

// beginRebuildScript atomically wipes staging keys then publishes rebuilding_gen + rebuild_token.
// KEYS[1]=meta KEYS[2]=stagingZ KEYS[3]=stagingApplied
// ARGV[1]=newGen ARGV[2]=generatedAtRFC3339 ARGV[3]=rebuildToken
var beginRebuildScript = redis.NewScript(`
local meta = KEYS[1]
local stagingZ = KEYS[2]
local stagingApplied = KEYS[3]
local newGen = ARGV[1]
local generatedAt = ARGV[2]
local token = ARGV[3]
local cur = redis.call('HGET', meta, 'generation')
if (not cur) or cur == false then
  cur = '0'
end
if tonumber(newGen) <= tonumber(cur) then
  return redis.error_reply('rebuild generation must exceed live generation')
end
local rebuilding = redis.call('HGET', meta, 'rebuilding_gen')
if rebuilding and rebuilding ~= false and rebuilding ~= '' then
  return redis.error_reply('rebuild already in progress')
end
redis.call('DEL', stagingZ, stagingApplied)
redis.call('HSET', meta,
  'rebuilding_gen', newGen,
  'rebuild_token', token,
  'generatedAt', generatedAt
)
return tonumber(cur)
`)

// rebuildMemberBatchScript writes staging members only while ownership holds.
// Stamps applied fences with versionWatermark. Never overwrites equal/higher applied.
// KEYS[1]=meta KEYS[2]=stagingZ KEYS[3]=stagingApplied
// ARGV[1]=newGen ARGV[2]=rebuildToken ARGV[3]=versionWatermark
// ARGV[4]=n ARGV[5+2i]=playerId ARGV[6+2i]=score
var rebuildMemberBatchScript = redis.NewScript(`
local meta = KEYS[1]
local zkey = KEYS[2]
local akey = KEYS[3]
local newGen = ARGV[1]
local token = ARGV[2]
local watermark = tonumber(ARGV[3]) or 0
local n = tonumber(ARGV[4])
local rebuilding = redis.call('HGET', meta, 'rebuilding_gen')
if (not rebuilding) or rebuilding == false or rebuilding ~= newGen then
  return redis.error_reply('rebuild ownership lost')
end
local curToken = redis.call('HGET', meta, 'rebuild_token')
if (not curToken) or curToken == false or curToken == '' or curToken ~= token then
  return redis.error_reply('rebuild ownership lost')
end
local written = 0
for i = 0, n - 1 do
  local id = ARGV[5 + i * 2]
  local score = tonumber(ARGV[6 + i * 2])
  local cur = redis.call('HGET', akey, id)
  if cur and cur ~= false and tonumber(cur) >= watermark then
    -- skip: CDC (or prior stamp) owns equal/higher version
  else
    redis.call('ZADD', zkey, score, id)
    redis.call('HSET', akey, id, tostring(watermark))
    written = written + 1
  end
end
return written
`)

// cutoverRebuildScript swaps live generation; projectionVersion = max(current, watermark)
// or current+1 when watermark <= 0. Persists memberCount from staging ZCARD.
// KEYS always include same-slot oldZ/oldApplied (gen:0 placeholders when no prior live).
// KEYS[1]=meta KEYS[2]=oldZ KEYS[3]=oldApplied
// ARGV[1]=newGen ARGV[2]=generatedAtRFC3339 ARGV[3]=watermarkVersion
// ARGV[4]=boardRoot ARGV[5]=rebuildToken
var cutoverRebuildScript = redis.NewScript(`
local meta = KEYS[1]
local oldZ = KEYS[2]
local oldApplied = KEYS[3]
local newGen = ARGV[1]
local generatedAt = ARGV[2]
local watermark = tonumber(ARGV[3])
local root = ARGV[4]
local token = ARGV[5]
local rebuilding = redis.call('HGET', meta, 'rebuilding_gen')
if (not rebuilding) or rebuilding == false or rebuilding ~= newGen then
  return redis.error_reply('rebuild generation mismatch')
end
local curToken = redis.call('HGET', meta, 'rebuild_token')
if (not curToken) or curToken == false or curToken ~= token then
  return redis.error_reply('rebuild token mismatch')
end
local curVer = tonumber(redis.call('HGET', meta, 'projectionVersion') or '0')
local newVer = curVer
if watermark > 0 then
  if watermark > curVer then
    newVer = watermark
  end
else
  newVer = curVer + 1
end
local liveZ = root .. 'gen:' .. newGen .. ':z'
local memberCount = redis.call('ZCARD', liveZ)
redis.call('HSET', meta,
  'generation', newGen,
  'generatedAt', generatedAt,
  'projectionVersion', tostring(newVer),
  'memberCount', tostring(memberCount),
  'ready', '1'
)
redis.call('HDEL', meta, 'rebuilding_gen', 'rebuild_token')
if redis.call('EXISTS', liveZ) == 0 then
  redis.call('ZADD', liveZ, 0, '__ranking_empty_sentinel__')
  redis.call('ZREM', liveZ, '__ranking_empty_sentinel__')
end
redis.call('DEL', oldZ)
redis.call('DEL', oldApplied)
return newVer
`)

// abortRebuildScript deletes staging only when rebuilding_gen matches and token
// exactly matches (missing/empty stored token → error, never abort).
// KEYS[1]=meta KEYS[2]=stagingZ KEYS[3]=stagingApplied
// ARGV[1]=gen ARGV[2]=rebuildToken
var abortRebuildScript = redis.NewScript(`
local meta = KEYS[1]
local stagingZ = KEYS[2]
local stagingApplied = KEYS[3]
local gen = ARGV[1]
local token = ARGV[2]
local live = redis.call('HGET', meta, 'generation')
if (not live) or live == false then
  live = '0'
end
local rebuilding = redis.call('HGET', meta, 'rebuilding_gen')
if (not rebuilding) or rebuilding == false or rebuilding ~= gen then
  return 0
end
local curToken = redis.call('HGET', meta, 'rebuild_token')
if (not curToken) or curToken == false or curToken == '' then
  return redis.error_reply('rebuild token missing')
end
if curToken ~= token then
  return redis.error_reply('rebuild token mismatch')
end
if live == gen then
  redis.call('HDEL', meta, 'rebuilding_gen', 'rebuild_token')
  return 1
end
redis.call('HDEL', meta, 'rebuilding_gen', 'rebuild_token')
redis.call('DEL', stagingZ, stagingApplied)
return 2
`)

// pageScript atomically reads generation + meta + keyset page + rankBase.
// KEYS[1]=meta
// ARGV[1]=boardRoot ARGV[2]=fetch ARGV[3]=hasCursor(0|1)
// ARGV[4]=afterRating ARGV[5]=afterPlayerId ARGV[6]=maxScan (same-score fallback cap)
// returns {status, gen, ver, generatedAt, rankBase, member, score, ...}
// status 0=unavailable, 1=ok, 2=retry (cutover race)
var pageScript = redis.NewScript(`
local meta = KEYS[1]
local root = ARGV[1]
local fetch = tonumber(ARGV[2])
local hasCursor = ARGV[3] == '1'
local afterRating = tonumber(ARGV[4])
local afterPlayerId = ARGV[5]
-- Hard cap for equal-rating cohort walks (production DefaultLeaderboardPageMaxScan).
local maxScan = tonumber(ARGV[6]) or 4096

local ready = redis.call('HGET', meta, 'ready')
if (not ready) or ready == false or ready ~= '1' then
  return {0}
end
local gen = redis.call('HGET', meta, 'generation')
if (not gen) or gen == false or gen == '' or tonumber(gen) < 1 then
  return {0}
end
local zkey = root .. 'gen:' .. gen .. ':z'
local ver = redis.call('HGET', meta, 'projectionVersion') or '0'
local generatedAt = redis.call('HGET', meta, 'generatedAt') or ''
local memberCountRaw = redis.call('HGET', meta, 'memberCount')
local exists = redis.call('EXISTS', zkey)
local card = 0
if exists == 1 then
  card = redis.call('ZCARD', zkey)
end
if exists == 0 or card == 0 then
  local gen2 = redis.call('HGET', meta, 'generation')
  local ready2 = redis.call('HGET', meta, 'ready')
  if (not ready2) or ready2 == false or ready2 ~= '1' then
    return {0}
  end
  if (not gen2) or gen2 == false or gen2 == '' or tonumber(gen2) < 1 then
    return {0}
  end
  if tostring(gen2) ~= tostring(gen) then
    return {2, tonumber(gen2), tonumber(ver), generatedAt, 0}
  end
  -- Absent memberCount ≠ zero: treat as corrupted / pre-flag meta.
  if (not memberCountRaw) or memberCountRaw == false then
    return {0}
  end
  local memberCount = tonumber(memberCountRaw)
  if memberCount == 0 then
    return {1, tonumber(gen), tonumber(ver), generatedAt, 1}
  end
  return {0}
end

-- Nonempty zset: memberCount must be present and match ZCARD.
if (not memberCountRaw) or memberCountRaw == false then
  return {0}
end
local memberCount = tonumber(memberCountRaw)
if (not memberCount) or memberCount ~= card then
  return {0}
end

local function encodeScore(rating)
  return -tonumber(rating)
end

local pairs = {}
local rankBase = 1

if not hasCursor then
  pairs = redis.call('ZRANGE', zkey, 0, fetch - 1, 'WITHSCORES')
  rankBase = 1
else
  local wantScore = encodeScore(afterRating)
  local rank = redis.call('ZRANK', zkey, afterPlayerId)
  local usedRank = false
  if rank then
    local sc = redis.call('ZSCORE', zkey, afterPlayerId)
    if sc and tonumber(sc) == wantScore then
      pairs = redis.call('ZRANGE', zkey, rank + 1, rank + fetch, 'WITHSCORES')
      rankBase = rank + 2
      usedRank = true
    end
  end
  if not usedRank then
    local out = {}
    local offset = 0
    local batchSize = 256
    local scoreStr = tostring(wantScore)
    local scanned = 0
    while (#out / 2) < fetch do
      local batch = redis.call('ZRANGEBYSCORE', zkey, scoreStr, scoreStr, 'WITHSCORES', 'LIMIT', offset, batchSize)
      if #batch == 0 then
        break
      end
      local batchMembers = #batch / 2
      scanned = scanned + batchMembers
      if scanned > maxScan then
        return {0}
      end
      for i = 1, #batch, 2 do
        local member = batch[i]
        if member > afterPlayerId then
          out[#out + 1] = member
          out[#out + 1] = batch[i + 1]
          if (#out / 2) >= fetch then
            break
          end
        end
      end
      offset = offset + batchMembers
      if batchMembers < batchSize then
        break
      end
    end
    local need = fetch - (#out / 2)
    if need > 0 then
      local rest = redis.call('ZRANGEBYSCORE', zkey, '(' .. scoreStr, '+inf', 'WITHSCORES', 'LIMIT', 0, need)
      for i = 1, #rest do
        out[#out + 1] = rest[i]
      end
    end
    pairs = out
    local before = redis.call('ZCOUNT', zkey, '-inf', '(' .. scoreStr)
    local sameBefore = 0
    offset = 0
    -- Rank-boundary walk reuses the same maxScan budget.
    while true do
      local batch = redis.call('ZRANGEBYSCORE', zkey, scoreStr, scoreStr, 'LIMIT', offset, batchSize)
      if #batch == 0 then
        break
      end
      scanned = scanned + #batch
      if scanned > maxScan then
        return {0}
      end
      local done = false
      for i = 1, #batch do
        if batch[i] <= afterPlayerId then
          sameBefore = sameBefore + 1
        else
          done = true
          break
        end
      end
      if done then
        break
      end
      offset = offset + #batch
      if #batch < batchSize then
        break
      end
    end
    rankBase = before + sameBefore + 1
  end
end

local out = {1, tonumber(gen), tonumber(ver), generatedAt, rankBase}
for i = 1, #pairs do
  out[#out + 1] = pairs[i]
end
return out
`)
