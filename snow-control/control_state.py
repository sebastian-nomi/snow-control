from concurrent.futures import ThreadPoolExecutor

class ControlState:
    __slots__ = 'connection', 'account', 'executor','snowcache','snowplan','queries','ignore_objects','verbosity'
    def __init__(self, max_workers = 100):
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.verbosity = 0 

    def __del__(self): 
        self.executor.shutdown()
    
    def print(self,message,  verbosity_level = 0, **kwargs):
        if verbosity_level >= self.verbosity:
            print(message, **kwargs)