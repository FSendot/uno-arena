package store

import "github.com/redis/go-redis/v9"

// applyCommitScript atomically CAS-commits one apply:
// KEYS[1]=meta KEYS[2]=state KEYS[3]=outcomes KEYS[4]=stream
// ARGV[1]=expectedRevision ARGV[2]=expectedSequence
// ARGV[3]=newRevision ARGV[4]=newSequence ARGV[5]=status ARGV[6]=streamClosed (0/1)
// ARGV[7]=eventCount ARGV[8]=mutated (0/1) ARGV[9]=stateJSON (or "")
// ARGV[10]=outcomeCount N; then N pairs eventId,outcomeJSON
// ARGV after outcomes: appendStream (0/1), eventType, sseID, dataJSON, sequence,
// closeFlag (0/1), streamMaxLen, explicitRedisID
//
// Returns: "ok" | "cas" | "ok_dup"
var applyCommitScript = redis.NewScript(`
local meta = KEYS[1]
local stateKey = KEYS[2]
local outcomes = KEYS[3]
local streamKey = KEYS[4]

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
  redis.call('XADD', streamKey, 'MAXLEN', '~', maxlen, explicitID,
    'event', eventType,
    'id', sseID,
    'data', dataJSON,
    'sequence', seq,
    'closed', closeFlag)
end

if streamClosed == '1' then
  redis.call('HSET', meta, 'terminal', '1')
end

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
