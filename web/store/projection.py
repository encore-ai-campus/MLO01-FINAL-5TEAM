"""로컬 워커가 DB에 커밋된 담기를 Redis에 반영한다."""
import os
from datetime import datetime, timezone as dt_timezone
from functools import lru_cache

from django.db import transaction
from django.utils import timezone
from redis import Redis
from redis.backoff import NoBackoff
from redis.retry import Retry

from .models import CartAction


# 같은 행동 ID를 다시 받으면 처음 기록한 시각을 반환한다.
APPLY_EVENT = """
local seen = redis.call('HGET', KEYS[1], ARGV[1])
if seen then return seen end
local clock = redis.call('TIME')
local stamp = clock[1] .. '.' .. string.format('%06d', tonumber(clock[2]))
if ARGV[2] == 'clear' then
  redis.call('DEL', KEYS[2])
else
  local delta = tonumber(ARGV[4])
  if delta ~= 0 then
    local next_val = redis.call('HINCRBY', KEYS[2], ARGV[3], delta)
    if next_val == 0 then
      redis.call('HDEL', KEYS[2], ARGV[3])
    end
  end
end
redis.call('HSET', KEYS[1], ARGV[1], stamp)
return stamp
"""


@lru_cache(maxsize=1)
def redis_client():
    return Redis.from_url(
        os.environ.get("REDIS_URL", "redis://redis:6379/0"),
        decode_responses=True, socket_connect_timeout=1, socket_timeout=1,
        retry=Retry(NoBackoff(), 0),
    )


def redis_keys(cart_id):
    prefix = f"cart:{{{cart_id}}}"
    return f"{prefix}:events", f"{prefix}:counts"


def stamp_datetime(value):
    seconds, microseconds = value.split(".")
    return datetime.fromtimestamp(int(seconds), dt_timezone.utc).replace(microsecond=int(microseconds))


def apply_to_redis(action):
    seen, counts = redis_keys(action.cart_id)
    value = redis_client().eval(
        APPLY_EVENT,
        2,
        seen,
        counts,
        str(action.event_id),
        action.action_type,
        action.product_id or "",
        str(action.delta),
    )
    return stamp_datetime(value)


@transaction.atomic
def process_one():
    action = (
        CartAction.objects.select_for_update()
        .filter(status="pending", eligible_at__lte=timezone.now())
        .order_by("pk").first()
    )
    if action is None:
        return False
    applied_at = apply_to_redis(action)
    action.applied_at = applied_at
    action.confirmed_at = timezone.now()
    action.status = "applied"
    action.save(update_fields=["applied_at", "confirmed_at", "status"])
    return True
