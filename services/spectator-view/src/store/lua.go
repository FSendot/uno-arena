package store

import "github.com/redis/go-redis/v9"

// applyCommitScript atomically CAS-commits one apply:
// KEYS[1]=meta KEYS[2]=state KEYS[3]=outcomes KEYS[4]=stream KEYS[5]=kafka_quarantine
// ARGV[1]=expectedRevision ARGV[2]=expectedSequence
// ARGV[3]=newRevision ARGV[4]=newSequence ARGV[5]=status ARGV[6]=streamClosed (0/1)
// ARGV[7]=eventCount ARGV[8]=mutated (0/1) ARGV[9]=stateJSON (or "")
// ARGV[10]=outcomeCount N; then N pairs eventId,outcomeJSON
// ARGV after outcomes: appendStream (0/1), eventType, sseID, dataJSON, sequence,
// closeFlag (0/1), streamMaxLen, explicitRedisID,
// markQuarantine (0/1), then when 1: consumerGroup, sourceTopic, classification,
// reason, eventId, correlationId, sourcePartition, sourceOffset, quarantinedAt
//
// Returns: "ok" | "cas" | "ok_dup"
var applyCommitScript = redis.NewScript(`
local meta = KEYS[1]
local stateKey = KEYS[2]
local outcomes = KEYS[3]
local streamKey = KEYS[4]
local quarantineKey = KEYS[5]

local expectedRev = ARGV[1]
local expectedSeq = ARGV[2]
local newRev = ARGV[3]
local newSeq = ARGV[4]
local status = ARGV[5]
local streamClosed = ARGV[6]
local eventCount = ARGV[7]
local mutated = ARGV[8]
local stateJSON = ARGV[9]
local outcomeCount = tonumber(ARGV[10])

local curRev = redis.call('HGET', meta, 'revision')
if not curRev then curRev = '0' end
local curSeq = redis.call('HGET', meta, 'sequence')
if not curSeq then curSeq = '0' end
if curRev ~= expectedRev or curSeq ~= expectedSeq then
  return 'cas'
end

local idx = 11
for i = 1, outcomeCount do
  local eid = ARGV[idx + (i - 1) * 2]
  if redis.call('HEXISTS', outcomes, eid) == 1 then
    return 'ok_dup'
  end
end
for i = 1, outcomeCount do
  local eid = ARGV[idx]
  local oval = ARGV[idx + 1]
  idx = idx + 2
  redis.call('HSET', outcomes, eid, oval)
end

if mutated == '1' and stateJSON ~= nil and stateJSON ~= '' then
  redis.call('SET', stateKey, stateJSON)
end

redis.call('HSET', meta,
  'revision', newRev,
  'sequence', newSeq,
  'status', status,
  'streamClosed', streamClosed,
  'eventCount', eventCount)
if redis.call('HGET', meta, 'generation') == false then
  redis.call('HSET', meta, 'generation', '1')
end

local appendStream = ARGV[idx]
idx = idx + 1
if appendStream == '1' then
  local eventType = ARGV[idx]
  local sseID = ARGV[idx + 1]
  local dataJSON = ARGV[idx + 2]
  local seq = ARGV[idx + 3]
  local closeFlag = ARGV[idx + 4]
  local maxlen = ARGV[idx + 5]
  local explicitID = ARGV[idx + 6]
  idx = idx + 7
  redis.call('XADD', streamKey, 'MAXLEN', '~', maxlen, explicitID,
    'event', eventType,
    'id', sseID,
    'data', dataJSON,
    'sequence', seq,
    'closed', closeFlag)
else
  idx = idx + 7
end

if streamClosed == '1' then
  redis.call('HSET', meta, 'terminal', '1')
end

local markQuarantine = ARGV[idx]
idx = idx + 1
if markQuarantine == '1' then
  local ts = ARGV[idx + 8]
  redis.call('HSET', quarantineKey,
    'active', '1',
    'consumer_group', ARGV[idx],
    'source_topic', ARGV[idx + 1],
    'classification', ARGV[idx + 2],
    'reason', ARGV[idx + 3],
    'event_id', ARGV[idx + 4],
    'correlation_id', ARGV[idx + 5],
    'source_partition', ARGV[idx + 6],
    'source_offset', ARGV[idx + 7],
    'quarantined_at', ts,
    'updated_at', ts,
    'released_at', '',
    'release_note', '')
end

return 'ok'
`)

// kafkaQuarantineScript atomically upserts an active Kafka aggregate quarantine.
// KEYS[1]=kafka_quarantine
// ARGV: consumerGroup, sourceTopic, classification, reason, eventId, correlationId,
// sourcePartition, sourceOffset, quarantinedAt
// Schema retains released_at/release_note for atomic recovery release / audit.
var kafkaQuarantineScript = redis.NewScript(`
local qKey = KEYS[1]
local ts = ARGV[9]
redis.call('HSET', qKey,
  'active', '1',
  'consumer_group', ARGV[1],
  'source_topic', ARGV[2],
  'classification', ARGV[3],
  'reason', ARGV[4],
  'event_id', ARGV[5],
  'correlation_id', ARGV[6],
  'source_partition', ARGV[7],
  'source_offset', ARGV[8],
  'quarantined_at', ts,
  'updated_at', ts,
  'released_at', '',
  'release_note', '')
return 'ok'
`)

// kafkaQuarantineReleaseScript sets active=0 with allowlisted release audit fields.
// KEYS[1]=kafka_quarantine
// ARGV[1]=releaseNote ARGV[2]=releasedAt
// Returns: "ok" | "inactive"
var kafkaQuarantineReleaseScript = redis.NewScript(`
local qKey = KEYS[1]
local releaseNote = ARGV[1]
local ts = ARGV[2]
local active = redis.call('HGET', qKey, 'active')
if active ~= '1' then
  if redis.call('EXISTS', qKey) == 1 then
    redis.call('HSET', qKey,
      'active', '0',
      'released_at', ts,
      'release_note', releaseNote,
      'updated_at', ts)
  end
  return 'inactive'
end
redis.call('HSET', qKey,
  'active', '0',
  'released_at', ts,
  'release_note', releaseNote,
  'updated_at', ts)
return 'ok'
`)

// rebuildSwapScript atomically replaces room projection under a new stream generation.
// KEYS[1]=meta KEYS[2]=state KEYS[3]=outcomes KEYS[4]=generation KEYS[5]=newStream
// ARGV[1]=newGeneration ARGV[2]=revision ARGV[3]=sequence ARGV[4]=status
// ARGV[5]=streamClosed ARGV[6]=eventCount ARGV[7]=stateJSON
// ARGV[8]=outcomeCount; then pairs; then optional initial stream entry fields
// (appendStream, eventType, sseID, dataJSON, sequence, closeFlag, streamMaxLen, explicitRedisID)
var rebuildSwapScript = redis.NewScript(`
local meta = KEYS[1]
local stateKey = KEYS[2]
local outcomes = KEYS[3]
local genKey = KEYS[4]
local newStream = KEYS[5]

local newGen = ARGV[1]
local revision = ARGV[2]
local sequence = ARGV[3]
local status = ARGV[4]
local streamClosed = ARGV[5]
local eventCount = ARGV[6]
local stateJSON = ARGV[7]
local outcomeCount = tonumber(ARGV[8])

redis.call('DEL', outcomes)
local idx = 9
for i = 1, outcomeCount do
  local eid = ARGV[idx]
  local oval = ARGV[idx + 1]
  idx = idx + 2
  redis.call('HSET', outcomes, eid, oval)
end

redis.call('SET', stateKey, stateJSON)
redis.call('SET', genKey, newGen)
redis.call('HSET', meta,
  'revision', revision,
  'sequence', sequence,
  'status', status,
  'streamClosed', streamClosed,
  'eventCount', eventCount,
  'generation', newGen,
  'terminal', streamClosed)

-- optional initial projection entry
local appendStream = ARGV[idx]
if appendStream == '1' then
  local eventType = ARGV[idx + 1]
  local sseID = ARGV[idx + 2]
  local dataJSON = ARGV[idx + 3]
  local seq = ARGV[idx + 4]
  local closeFlag = ARGV[idx + 5]
  local maxlen = ARGV[idx + 6]
  local explicitID = ARGV[idx + 7]
  redis.call('XADD', newStream, 'MAXLEN', '~', maxlen, explicitID,
    'event', eventType,
    'id', sseID,
    'data', dataJSON,
    'sequence', seq,
    'closed', closeFlag)
end

return 'ok'
`)

// recoveryRebuildSwapScript CAS-fences a recovery generation-swap.
// KEYS[1]=meta KEYS[2]=state KEYS[3]=outcomes KEYS[4]=generation KEYS[5]=newStream
// KEYS[6]=kafka_quarantine
// KEYS[7]=rebuild_done
// ARGV[1]=expectedGeneration ARGV[2]=expectedSequence
// ARGV[3]=newGeneration ARGV[4]=revision ARGV[5]=sequence ARGV[6]=status
// ARGV[7]=streamClosed ARGV[8]=eventCount ARGV[9]=stateJSON
// ARGV[10]=outcomeCount; then pairs; then optional initial stream entry fields
// (appendStream, eventType, sseID, dataJSON, sequence, closeFlag, streamMaxLen, explicitRedisID)
// then releaseQuarantine (0/1), releaseNote, releasedAt
// then markDone (0/1), ttlSeconds
//
// Returns: "ok" | "already_done" | "stale" | "conflict"
// - already_done: identical idempotency key already committed; no mutation
// - stale: current sequence > expected (newer live Apply won; no mutation)
// - conflict: generation mismatch at equal sequence, or current sequence < expected
var recoveryRebuildSwapScript = redis.NewScript(`
local meta = KEYS[1]
local stateKey = KEYS[2]
local outcomes = KEYS[3]
local genKey = KEYS[4]
local newStream = KEYS[5]
local quarantineKey = KEYS[6]
local doneKey = KEYS[7]

local expectedGen = ARGV[1]
local expectedSeq = tonumber(ARGV[2]) or 0
local newGen = ARGV[3]
local revision = ARGV[4]
local sequence = ARGV[5]
local status = ARGV[6]
local streamClosed = ARGV[7]
local eventCount = ARGV[8]
local stateJSON = ARGV[9]
local outcomeCount = tonumber(ARGV[10])

local idx = 11
for i = 1, outcomeCount do
  idx = idx + 2
end
idx = idx + 8 -- stream fields (appendStream + 7)
local releaseQuarantine = ARGV[idx]
idx = idx + 3 -- releaseQuarantine, releaseNote, releasedAt
local markDone = ARGV[idx]
local ttlSeconds = tonumber(ARGV[idx + 1]) or 0

if markDone == '1' and redis.call('EXISTS', doneKey) == 1 then
  return 'already_done'
end

local curSeqRaw = redis.call('HGET', meta, 'sequence')
if not curSeqRaw then curSeqRaw = '0' end
local curSeq = tonumber(curSeqRaw) or 0

local curGen = redis.call('HGET', meta, 'generation')
if not curGen then
  curGen = redis.call('GET', genKey)
end
if not curGen then curGen = '1' end

if curSeq > expectedSeq then
  return 'stale'
end
if curSeq < expectedSeq then
  return 'conflict'
end
if curGen ~= expectedGen then
  return 'conflict'
end

redis.call('DEL', outcomes)
idx = 11
for i = 1, outcomeCount do
  local eid = ARGV[idx]
  local oval = ARGV[idx + 1]
  idx = idx + 2
  redis.call('HSET', outcomes, eid, oval)
end

redis.call('SET', stateKey, stateJSON)
redis.call('SET', genKey, newGen)
redis.call('HSET', meta,
  'revision', revision,
  'sequence', sequence,
  'status', status,
  'streamClosed', streamClosed,
  'eventCount', eventCount,
  'generation', newGen,
  'terminal', streamClosed)

local appendStream = ARGV[idx]
idx = idx + 1
if appendStream == '1' then
  local eventType = ARGV[idx]
  local sseID = ARGV[idx + 1]
  local dataJSON = ARGV[idx + 2]
  local seq = ARGV[idx + 3]
  local closeFlag = ARGV[idx + 4]
  local maxlen = ARGV[idx + 5]
  local explicitID = ARGV[idx + 6]
  idx = idx + 7
  redis.call('XADD', newStream, 'MAXLEN', '~', maxlen, explicitID,
    'event', eventType,
    'id', sseID,
    'data', dataJSON,
    'sequence', seq,
    'closed', closeFlag)
else
  idx = idx + 7
end

releaseQuarantine = ARGV[idx]
idx = idx + 1
if releaseQuarantine == '1' then
  local releaseNote = ARGV[idx]
  local releasedAt = ARGV[idx + 1]
  redis.call('HSET', quarantineKey,
    'active', '0',
    'released_at', releasedAt,
    'release_note', releaseNote,
    'updated_at', releasedAt)
end
idx = idx + 2

markDone = ARGV[idx]
ttlSeconds = tonumber(ARGV[idx + 1]) or 0
if markDone == '1' then
  if ttlSeconds > 0 then
    redis.call('SET', doneKey, '1', 'EX', ttlSeconds)
  else
    redis.call('SET', doneKey, '1')
  end
end

return 'ok'
`)
