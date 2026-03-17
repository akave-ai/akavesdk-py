# Copyright (C) 2025 Akave
# See LICENSE for copying information.

import asyncio
import random
import time
from typing import Callable, Tuple


class WithRetry:
    def __init__(self, max_attempts: int, base_delay: float):
        self.max_attempts = max_attempts
        self.base_delay = base_delay

    def do(self, f: Callable[[], Tuple[bool, Exception]]) -> Exception:
        for attempt in range(self.max_attempts + 1):
            needs_retry, err = f()
            if err is None:
                return None

            if not needs_retry or attempt >= self.max_attempts:
                return err

            backoff = self.base_delay * (2**attempt)
            jitter = random.uniform(0, self.base_delay)
            delay = backoff + jitter

            time.sleep(delay)

        return err

    async def do_async(self, ctx, f: Callable[[], Tuple[bool, Exception]]) -> Exception:
        for attempt in range(self.max_attempts + 1):
            if ctx and ctx.done():
                return Exception(f"retry aborted: context cancelled")

            needs_retry, err = f()
            if err is None:
                return None

            if not needs_retry or attempt >= self.max_attempts:
                return err

            backoff = self.base_delay * (2**attempt)
            jitter = random.uniform(0, self.base_delay)
            delay = backoff + jitter

            try:
                await asyncio.wait_for(asyncio.sleep(delay), timeout=delay + 1)
            except asyncio.CancelledError:
                return Exception(f"retry aborted: {err}")

        return err
