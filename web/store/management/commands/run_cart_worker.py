import time

from django.core.management.base import BaseCommand
from django.db import OperationalError, close_old_connections
from redis.exceptions import RedisError

from store.projection import process_one


class Command(BaseCommand):
    help = "커밋된 장바구니 행동을 Redis에 반영하는 로컬 워커"

    def add_arguments(self, parser):
        parser.add_argument("--once", action="store_true", help="준비된 행동 한 건만 처리")

    def handle(self, *args, **options):
        if options["once"]:
            process_one()
            return
        unavailable = False
        while True:
            close_old_connections()
            try:
                processed = process_one()
                if unavailable:
                    self.stdout.write("저장소 연결 복구")
                    unavailable = False
                if not processed:
                    time.sleep(0.25)
            except (RedisError, OperationalError):
                if not unavailable:
                    self.stderr.write("저장소 연결 대기 중. 같은 행동 ID로 재시도합니다.")
                    unavailable = True
                time.sleep(1)
