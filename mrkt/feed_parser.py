class FeedParser:
    def __init__(self, mrkt_api=None, session_paths=None, notify_callback=None, worker_notify=None):
        self.mrkt_api = mrkt_api
        self.session_paths = session_paths or []
        self.notify_callback = notify_callback
        self.worker_notify = worker_notify
        self._running = False

    async def start(self):
        self._running = True
        import asyncio
        while self._running:
            try:
                await asyncio.sleep(10)
            except asyncio.CancelledError:
                break

    async def stop(self):
        self._running = False
