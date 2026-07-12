package store

import "github.com/redis/go-redis/v9"

// sessionInvalidationApplyScript atomically establishes session invalidation +
// eventId control binding and PUBLISHes a wake notification on first accept or
// restore (event binding present, session hash missing).
//
// KEYS[1]=session hash KEYS[2]=event hash
// ARGV: eventId, sessionId, playerId, reason, correlationId, occurredAt, ttlSeconds,
//
//	notifyChannel, notifyPayload
//
// Returns: accepted | restored | duplicate | conflict
var sessionInvalidationApplyScript = redis.NewScript(`
local sessKey = KEYS[1]
local eventKey = KEYS[2]
local eventId = ARGV[1]
local sessionId = ARGV[2]
local playerId = ARGV[3]
local reason = ARGV[4]
local correlationId = ARGV[5]
local occurredAt = ARGV[6]
local ttl = tonumber(ARGV[7])
local channel = ARGV[8]
local payload = ARGV[9]

local existing = redis.call('HGETALL', sessKey)
if #existing > 0 then
  local fields = {}
  for i = 1, #existing, 2 do
    fields[existing[i]] = existing[i + 1]
  end
  if fields['event_id'] == eventId
    and fields['session_id'] == sessionId
    and fields['player_id'] == playerId
    and fields['reason'] == reason then
    return 'duplicate'
  end
  return 'conflict'
end

local boundSession = redis.call('HGET', eventKey, 'session_id')
if boundSession and boundSession ~= '' then
  if boundSession ~= sessionId then
    return 'conflict'
  end
  local boundPlayer = redis.call('HGET', eventKey, 'player_id')
  local boundReason = redis.call('HGET', eventKey, 'reason')
  if (boundPlayer and boundPlayer ~= '' and boundPlayer ~= playerId)
    or (boundReason and boundReason ~= '' and boundReason ~= reason) then
    return 'conflict'
  end
  -- Event bound to this session but session hash missing (uneven TTL / partial delete):
  -- re-establish admission protection + TTLs and wake replicas (restored, not duplicate).
  redis.call('HSET', sessKey,
    'active', '1',
    'event_id', eventId,
    'session_id', sessionId,
    'player_id', playerId,
    'reason', reason,
    'correlation_id', correlationId,
    'occurred_at', occurredAt)
  if ttl and ttl > 0 then
    redis.call('EXPIRE', sessKey, ttl)
    redis.call('EXPIRE', eventKey, ttl)
  end
  redis.call('PUBLISH', channel, payload)
  return 'restored'
end

redis.call('HSET', sessKey,
  'active', '1',
  'event_id', eventId,
  'session_id', sessionId,
  'player_id', playerId,
  'reason', reason,
  'correlation_id', correlationId,
  'occurred_at', occurredAt)
redis.call('HSET', eventKey,
  'session_id', sessionId,
  'event_type', 'SessionInvalidated',
  'player_id', playerId,
  'reason', reason)
if ttl and ttl > 0 then
  redis.call('EXPIRE', sessKey, ttl)
  redis.call('EXPIRE', eventKey, ttl)
end
redis.call('PUBLISH', channel, payload)
return 'accepted'
`)

// kafkaSIQuarantineScript atomically upserts an active Kafka aggregate quarantine.
// KEYS[1]=kafka_quarantine
// ARGV: consumerGroup, sourceTopic, classification, reason, eventId, correlationId,
// sourcePartition, sourceOffset, quarantinedAt
var kafkaSIQuarantineScript = redis.NewScript(`
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
