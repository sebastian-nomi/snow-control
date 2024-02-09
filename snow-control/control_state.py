from concurrent.futures import ThreadPoolExecutor

class ControlState:
    __slots__ = 'connection', 'account', 'executor','snowcache','snowplan','queries','ignore_objects'
    def __init__(self, max_workers = 100):
        self.executor = ThreadPoolExecutor(max_workers=max_workers)

    def __del__(self): 
        self.executor.shutdown()
    