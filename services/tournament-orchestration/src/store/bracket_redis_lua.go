package store

import "github.com/redis/go-redis/v9"

// upsertBracketSummaryScript applies a compact summary JSON fenced by projectionVersion.
// Return codes: 0=stale/noop, 1=applied, 2=idempotent equal version, 3=conflict.
// Dual-write is fail-closed: preflight live+staging fences; any conflict returns 3 with no writes.
// Ready is O(1): summary is the per-refresh commit marker and sets ready only when
// expectedSlots(summary)==ZCARD(idx).
//
// KEYS[1]=meta
// ARGV[1]=root ARGV[2]=summaryJSON ARGV[3]=projectionVersion ARGV[4]=generatedAtRFC3339
var upsertBracketSummaryScript = redis.NewScript(`
local meta = KEYS[1]
local root = ARGV[1]
local summaryJSON = ARGV[2]
local eventVer = tonumber(ARGV[3])
local generatedAt = ARGV[4]

local function preflight(summaryKey)
  if (not eventVer) or eventVer <= 0 then
    return 3
  end
  local fenceKey = summaryKey .. ':ver'
  local curRaw = redis.call('GET', fenceKey)
  local curVer = 0
  if curRaw and curRaw ~= false then
    curVer = tonumber(curRaw) or 0
  end
  if curVer > eventVer then
    return 0
  end
  if curVer == eventVer then
    local cur = redis.call('GET', summaryKey)
    if cur and cur ~= false and cur == summaryJSON then
      return 2
    end
    return 3
  end
  return 1
end

local function writeSummary(summaryKey)
  redis.call('SET', summaryKey, summaryJSON)
  redis.call('SET', summaryKey .. ':ver', tostring(eventVer))
end

local function expectedSlots(summaryRaw)
  if (not summaryRaw) or summaryRaw == false or summaryRaw == '' then
    return 0
  end
  local ok, obj = pcall(cjson.decode, summaryRaw)
  if (not ok) or (not obj) then
    return 0
  end
  local rounds = obj['rounds']
  if (not rounds) or type(rounds) ~= 'table' then
    return 0
  end
  local total = 0
  for i = 1, #rounds do
    local sc = tonumber(rounds[i]['slotCount']) or 0
    total = total + sc
  end
  return total
end

local function maybeMarkReady(g)
  local skey = root .. 'gen:' .. g .. ':summary'
  local summaryRaw = redis.call('GET', skey)
  if (not summaryRaw) or summaryRaw == false then
    return
  end
  local want = expectedSlots(summaryRaw)
  local have = redis.call('ZCARD', root .. 'gen:' .. g .. ':idx')
  if want == have then
    redis.call('HSET', meta, 'ready', '1')
  end
end

local gen = redis.call('HGET', meta, 'generation')
local rebuilding = redis.call('HGET', meta, 'rebuilding_gen')
local liveGen = 0
if gen and gen ~= false and gen ~= '' then
  liveGen = tonumber(gen) or 0
end
local hasRebuild = rebuilding and rebuilding ~= false and rebuilding ~= ''

-- Bootstrap writable generation without ready.
if liveGen < 1 and (not hasRebuild) then
  redis.call('HSET', meta,
    'generation', '1',
    'projectionVersion', '0',
    'generatedAt', generatedAt
  )
  redis.call('HDEL', meta, 'ready')
  liveGen = 1
  gen = '1'
end

local liveKey = nil
local stagingKey = nil
local liveCode = 0
local stagingCode = nil

if liveGen >= 1 then
  liveKey = root .. 'gen:' .. liveGen .. ':summary'
  liveCode = preflight(liveKey)
  if hasRebuild and rebuilding ~= tostring(liveGen) then
    stagingKey = root .. 'gen:' .. rebuilding .. ':summary'
    stagingCode = preflight(stagingKey)
  end
elseif hasRebuild then
  liveKey = root .. 'gen:' .. rebuilding .. ':summary'
  liveCode = preflight(liveKey)
end

if liveCode == 3 or (stagingCode ~= nil and stagingCode == 3) then
  return 3
end
if liveCode == 0 then
  return 0
end

if liveCode == 1 then
  writeSummary(liveKey)
end
if stagingKey ~= nil and stagingCode == 1 then
  writeSummary(stagingKey)
end

local applied = liveCode
if applied == 1 then
  local curMetaVer = tonumber(redis.call('HGET', meta, 'projectionVersion') or '0') or 0
  if eventVer > curMetaVer then
    redis.call('HSET', meta, 'projectionVersion', tostring(eventVer))
  end
  redis.call('HSET', meta, 'generatedAt', generatedAt)
end
if (applied == 1 or applied == 2) and liveGen >= 1 then
  maybeMarkReady(tostring(liveGen))
end
return applied
`)

// upsertBracketChunkScript upserts one batch chunk + index members + chunkset entry.
// Fail-closed dual-write: preflight before any mutation. A changed live chunk clears
// ready and does not advance meta projectionVersion; the following summary upsert is
// the refresh commit marker. Reads fall back to Postgres during that bounded window.
// ARGV: root, round, batchID, chunkJSON, version, generatedAt, n, then n×(score, member=round:slot)
var upsertBracketChunkScript = redis.NewScript(`
local meta = KEYS[1]
local root = ARGV[1]
local roundNumber = ARGV[2]
local batchID = ARGV[3]
local chunkJSON = ARGV[4]
local eventVer = tonumber(ARGV[5])
local generatedAt = ARGV[6]
local n = tonumber(ARGV[7]) or 0

local function chunkKey(gen)
  return root .. 'gen:' .. gen .. ':chunk:' .. roundNumber .. ':' .. batchID
end
local function idxKey(gen)
  return root .. 'gen:' .. gen .. ':idx'
end
local function mapKey(gen)
  return root .. 'gen:' .. gen .. ':slotmap'
end
local function chunksetKey(gen)
  return root .. 'gen:' .. gen .. ':chunkset'
end
local function fenceKey(gen)
  return chunkKey(gen) .. ':ver'
end

local function preflight(gen)
  if (not eventVer) or eventVer <= 0 then
    return 3
  end
  local ckey = chunkKey(gen)
  local fkey = fenceKey(gen)
  local curRaw = redis.call('GET', fkey)
  local curVer = 0
  if curRaw and curRaw ~= false then
    curVer = tonumber(curRaw) or 0
  end
  if curVer > eventVer then
    return 0
  end
  if curVer == eventVer then
    local cur = redis.call('GET', ckey)
    if cur and cur ~= false and cur == chunkJSON then
      return 2
    end
    return 3
  end
  return 1
end

local function writeChunk(gen)
  local ckey = chunkKey(gen)
  redis.call('SET', ckey, chunkJSON)
  redis.call('SET', fenceKey(gen), tostring(eventVer))
  local ikey = idxKey(gen)
  local mkey = mapKey(gen)
  for i = 0, n - 1 do
    local score = tonumber(ARGV[8 + i * 2])
    local member = ARGV[9 + i * 2]
    redis.call('ZADD', ikey, score, member)
    redis.call('HSET', mkey, member, batchID)
  end
  redis.call('SADD', chunksetKey(gen), roundNumber .. ':' .. batchID)
end

local function expectedSlots(summaryRaw)
  if (not summaryRaw) or summaryRaw == false or summaryRaw == '' then
    return 0
  end
  local ok, obj = pcall(cjson.decode, summaryRaw)
  if (not ok) or (not obj) then
    return 0
  end
  local rounds = obj['rounds']
  if (not rounds) or type(rounds) ~= 'table' then
    return 0
  end
  local total = 0
  for i = 1, #rounds do
    total = total + (tonumber(rounds[i]['slotCount']) or 0)
  end
  return total
end

local function maybeMarkReady(g)
  local summaryRaw = redis.call('GET', root .. 'gen:' .. g .. ':summary')
  if (not summaryRaw) or summaryRaw == false then
    return
  end
  local want = expectedSlots(summaryRaw)
  local have = redis.call('ZCARD', root .. 'gen:' .. g .. ':idx')
  if want == have then
    redis.call('HSET', meta, 'ready', '1')
  end
end

local gen = redis.call('HGET', meta, 'generation')
local rebuilding = redis.call('HGET', meta, 'rebuilding_gen')
local liveGen = 0
if gen and gen ~= false and gen ~= '' then
  liveGen = tonumber(gen) or 0
end
local hasRebuild = rebuilding and rebuilding ~= false and rebuilding ~= ''

if liveGen < 1 and (not hasRebuild) then
  redis.call('HSET', meta,
    'generation', '1',
    'projectionVersion', '0',
    'generatedAt', generatedAt
  )
  redis.call('HDEL', meta, 'ready')
  liveGen = 1
  gen = '1'
end

local liveTarget = nil
local stagingTarget = nil
local liveCode = 0
local stagingCode = nil

if liveGen >= 1 then
  liveTarget = tostring(liveGen)
  liveCode = preflight(liveTarget)
  if hasRebuild and rebuilding ~= tostring(liveGen) then
    stagingTarget = rebuilding
    stagingCode = preflight(stagingTarget)
  end
elseif hasRebuild then
  liveTarget = rebuilding
  liveCode = preflight(liveTarget)
end

if liveCode == 3 or (stagingCode ~= nil and stagingCode == 3) then
  return 3
end
if liveCode == 0 then
  return 0
end

if liveCode == 1 then
  writeChunk(liveTarget)
end
if stagingTarget ~= nil and stagingCode == 1 then
  writeChunk(stagingTarget)
end

local applied = liveCode
if applied == 1 and liveGen >= 1 then
  local publishedVer = tonumber(redis.call('HGET', meta, 'projectionVersion') or '0') or 0
  if eventVer > publishedVer then
    -- Chunk-first refresh: hide Redis until summary publishes the same version.
    redis.call('HDEL', meta, 'ready')
  elseif eventVer == publishedVer then
    -- Summary-first bootstrap or completion of an already-published version.
    maybeMarkReady(tostring(liveGen))
  end
end
return applied
`)

// beginBracketRebuildScript wipes staging via chunkset (bounded by batch count), never HGETALL slotmap.
// KEYS[1]=meta KEYS[2]=stagingSummary KEYS[3]=stagingIdx KEYS[4]=stagingSlotmap KEYS[5]=stagingChunkset
// ARGV[1]=newGen ARGV[2]=generatedAt ARGV[3]=rebuildToken ARGV[4]=root
var beginBracketRebuildScript = redis.NewScript(`
local meta = KEYS[1]
local stagingSummary = KEYS[2]
local stagingIdx = KEYS[3]
local stagingMap = KEYS[4]
local stagingChunkset = KEYS[5]
local newGen = ARGV[1]
local generatedAt = ARGV[2]
local token = ARGV[3]
local root = ARGV[4]
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
local members = redis.call('SMEMBERS', stagingChunkset)
for i = 1, #members do
  local rb = members[i]
  local ckey = root .. 'gen:' .. newGen .. ':chunk:' .. rb
  redis.call('DEL', ckey, ckey .. ':ver')
end
redis.call('DEL', stagingSummary, stagingSummary .. ':ver', stagingIdx, stagingMap, stagingChunkset)
redis.call('HSET', meta,
  'rebuilding_gen', newGen,
  'rebuild_token', token,
  'generatedAt', generatedAt
)
return tonumber(cur)
`)

// rebuildBracketSummaryScript writes staging summary while ownership holds.
var rebuildBracketSummaryScript = redis.NewScript(`
local meta = KEYS[1]
local summaryKey = KEYS[2]
local newGen = ARGV[1]
local token = ARGV[2]
local watermark = tonumber(ARGV[3]) or 0
local summaryJSON = ARGV[4]
local rebuilding = redis.call('HGET', meta, 'rebuilding_gen')
if (not rebuilding) or rebuilding == false or rebuilding ~= newGen then
  return redis.error_reply('rebuild ownership lost')
end
local curToken = redis.call('HGET', meta, 'rebuild_token')
if (not curToken) or curToken == false or curToken == '' or curToken ~= token then
  return redis.error_reply('rebuild ownership lost')
end
local fence = summaryKey .. ':ver'
local cur = redis.call('GET', fence)
if cur and cur ~= false and tonumber(cur) >= watermark and watermark > 0 then
  return 0
end
redis.call('SET', summaryKey, summaryJSON)
if watermark > 0 then
  redis.call('SET', fence, tostring(watermark))
end
return 1
`)

// rebuildBracketChunkScript writes one staging chunk + index + chunkset while ownership holds.
// ARGV: root, newGen, token, watermark, round, batchID, chunkJSON, n, then n×(score, member)
var rebuildBracketChunkScript = redis.NewScript(`
local meta = KEYS[1]
local root = ARGV[1]
local newGen = ARGV[2]
local token = ARGV[3]
local watermark = tonumber(ARGV[4]) or 0
local roundNumber = ARGV[5]
local batchID = ARGV[6]
local chunkJSON = ARGV[7]
local n = tonumber(ARGV[8]) or 0
local rebuilding = redis.call('HGET', meta, 'rebuilding_gen')
if (not rebuilding) or rebuilding == false or rebuilding ~= newGen then
  return redis.error_reply('rebuild ownership lost')
end
local curToken = redis.call('HGET', meta, 'rebuild_token')
if (not curToken) or curToken == false or curToken == '' or curToken ~= token then
  return redis.error_reply('rebuild ownership lost')
end
local ckey = root .. 'gen:' .. newGen .. ':chunk:' .. roundNumber .. ':' .. batchID
local fence = ckey .. ':ver'
local cur = redis.call('GET', fence)
if cur and cur ~= false and tonumber(cur) >= watermark and watermark > 0 then
  return 0
end
redis.call('SET', ckey, chunkJSON)
if watermark > 0 then
  redis.call('SET', fence, tostring(watermark))
end
local ikey = root .. 'gen:' .. newGen .. ':idx'
local mkey = root .. 'gen:' .. newGen .. ':slotmap'
local cset = root .. 'gen:' .. newGen .. ':chunkset'
for i = 0, n - 1 do
  local score = tonumber(ARGV[9 + i * 2])
  local member = ARGV[10 + i * 2]
  redis.call('ZADD', ikey, score, member)
  redis.call('HSET', mkey, member, batchID)
end
redis.call('SADD', cset, roundNumber .. ':' .. batchID)
return 1
`)

// cutoverBracketRebuildScript swaps live generation; cleans old gen via chunkset (not HGETALL slotmap).
// KEYS[1]=meta KEYS[2]=oldSummary KEYS[3]=oldIdx KEYS[4]=oldSlotmap KEYS[5]=oldChunkset
// ARGV[1]=newGen ARGV[2]=generatedAt ARGV[3]=watermark ARGV[4]=root ARGV[5]=token ARGV[6]=oldGen
var cutoverBracketRebuildScript = redis.NewScript(`
local meta = KEYS[1]
local oldSummary = KEYS[2]
local oldIdx = KEYS[3]
local oldMap = KEYS[4]
local oldChunkset = KEYS[5]
local newGen = ARGV[1]
local generatedAt = ARGV[2]
local watermark = tonumber(ARGV[3])
local root = ARGV[4]
local token = ARGV[5]
local oldGen = ARGV[6]
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
if oldGen and oldGen ~= '' and tonumber(oldGen) >= 1 then
  local members = redis.call('SMEMBERS', oldChunkset)
  for i = 1, #members do
    local rb = members[i]
    local ckey = root .. 'gen:' .. oldGen .. ':chunk:' .. rb
    redis.call('DEL', ckey, ckey .. ':ver')
  end
  redis.call('DEL', oldSummary, oldSummary .. ':ver', oldIdx, oldMap, oldChunkset)
end
redis.call('HSET', meta,
  'generation', newGen,
  'generatedAt', generatedAt,
  'projectionVersion', tostring(newVer),
  'ready', '1'
)
redis.call('HDEL', meta, 'rebuilding_gen', 'rebuild_token')
local liveIdx = root .. 'gen:' .. newGen .. ':idx'
if redis.call('EXISTS', liveIdx) == 0 then
  redis.call('ZADD', liveIdx, 0, '__bracket_empty_sentinel__')
  redis.call('ZREM', liveIdx, '__bracket_empty_sentinel__')
end
return newVer
`)

// abortBracketRebuildScript deletes staging via chunkset when token matches.
// KEYS[1]=meta KEYS[2]=stagingSummary KEYS[3]=stagingIdx KEYS[4]=stagingSlotmap KEYS[5]=stagingChunkset
var abortBracketRebuildScript = redis.NewScript(`
local meta = KEYS[1]
local stagingSummary = KEYS[2]
local stagingIdx = KEYS[3]
local stagingMap = KEYS[4]
local stagingChunkset = KEYS[5]
local gen = ARGV[1]
local token = ARGV[2]
local root = ARGV[3]
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
local members = redis.call('SMEMBERS', stagingChunkset)
for i = 1, #members do
  local rb = members[i]
  local ckey = root .. 'gen:' .. gen .. ':chunk:' .. rb
  redis.call('DEL', ckey, ckey .. ':ver')
end
redis.call('HDEL', meta, 'rebuilding_gen', 'rebuild_token')
redis.call('DEL', stagingSummary, stagingSummary .. ':ver', stagingIdx, stagingMap, stagingChunkset)
return 2
`)

// pageBracketScript atomically reads generation + meta + summary + keyset slot page.
// Missing chunk/slot → unavailable (composite falls back to Postgres).
// KEYS[1]=meta
// ARGV[1]=root ARGV[2]=fetch ARGV[3]=hasCursor ARGV[4]=afterRound ARGV[5]=afterSlot
// ARGV[6]=filterRound (0|1) ARGV[7]=roundFilter
// returns {status, gen, ver, generatedAt, summaryJSON, slotJSON...}
// status 0=unavailable, 1=ok, 2=retry (cutover race)
var pageBracketScript = redis.NewScript(`
local meta = KEYS[1]
local root = ARGV[1]
local fetch = tonumber(ARGV[2])
local hasCursor = ARGV[3] == '1'
local afterRound = tonumber(ARGV[4]) or 0
local afterSlot = tonumber(ARGV[5]) or 0
local filterRound = ARGV[6] == '1'
local roundFilter = tonumber(ARGV[7]) or 0

local ready = redis.call('HGET', meta, 'ready')
if (not ready) or ready == false or ready ~= '1' then
  return {0}
end
local gen = redis.call('HGET', meta, 'generation')
if (not gen) or gen == false or gen == '' or tonumber(gen) < 1 then
  return {0}
end
local ver = redis.call('HGET', meta, 'projectionVersion') or '0'
local generatedAt = redis.call('HGET', meta, 'generatedAt') or ''
local summaryKey = root .. 'gen:' .. gen .. ':summary'
local summary = redis.call('GET', summaryKey)
if (not summary) or summary == false then
  local gen2 = redis.call('HGET', meta, 'generation')
  if tostring(gen2) ~= tostring(gen) then
    return {2, tonumber(gen2), tonumber(ver), generatedAt, ''}
  end
  return {0}
end
local idx = root .. 'gen:' .. gen .. ':idx'
local slotmap = root .. 'gen:' .. gen .. ':slotmap'
local afterScore = afterRound * 1000000000 + afterSlot
local members
if filterRound then
  local minScore = roundFilter * 1000000000
  local maxScore = (roundFilter + 1) * 1000000000 - 1
  if hasCursor then
    if afterRound ~= roundFilter then
      return {0}
    end
    members = redis.call('ZRANGEBYSCORE', idx, '(' .. afterScore, maxScore, 'LIMIT', 0, fetch)
  else
    members = redis.call('ZRANGEBYSCORE', idx, minScore, maxScore, 'LIMIT', 0, fetch)
  end
else
  if hasCursor then
    members = redis.call('ZRANGEBYSCORE', idx, '(' .. afterScore, '+inf', 'LIMIT', 0, fetch)
  else
    members = redis.call('ZRANGEBYSCORE', idx, '-inf', '+inf', 'LIMIT', 0, fetch)
  end
end

local chunkCache = {}
local slots = {}
for i = 1, #members do
  local member = members[i]
  local round, slot = string.match(member, '^(%d+):(%d+)$')
  if (not round) or (not slot) then
    return {0}
  end
  local batch = redis.call('HGET', slotmap, member)
  if (not batch) or batch == false or batch == '' then
    return {0}
  end
  local cacheKey = round .. ':' .. batch
  local lookup = chunkCache[cacheKey]
  if not lookup then
    local raw = redis.call('GET', root .. 'gen:' .. gen .. ':chunk:' .. round .. ':' .. batch)
    if (not raw) or raw == false then
      return {0}
    end
    local arr = cjson.decode(raw)
    lookup = {}
    for j = 1, #arr do
      local s = arr[j]
      local sr = tonumber(s['roundNumber'])
      local si = tonumber(s['slotIndex'])
      if (not sr) or (not si) then
        return {0}
      end
      lookup[tostring(sr) .. ':' .. tostring(si)] = s
    end
    chunkCache[cacheKey] = lookup
  end
  local found = lookup[member]
  if not found then
    return {0}
  end
  slots[#slots + 1] = cjson.encode(found)
end

local gen2 = redis.call('HGET', meta, 'generation')
local ready2 = redis.call('HGET', meta, 'ready')
if (not ready2) or ready2 == false or ready2 ~= '1' then
  return {0}
end
if tostring(gen2) ~= tostring(gen) then
  return {2, tonumber(gen2), tonumber(ver), generatedAt, summary}
end
local out = {1, tonumber(gen), tonumber(ver), generatedAt, summary}
for i = 1, #slots do
  out[#out + 1] = slots[i]
end
return out
`)
