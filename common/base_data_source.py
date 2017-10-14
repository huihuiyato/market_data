import abc
import threading

class DataSource():
    __metaclass__ = abc.ABCMeta

    __singleton_lock = threading.Lock()
    __instance = None

    def __new__(cls, *args, **kwargs):
        if not cls.__instance:
            with cls.__singleton_lock:
                if not cls.__instance:
                    cls.__instance = super(DataSource, cls).__new__(cls, *args, **kwargs)
        return cls.__instance

    def __init__(self, mongo_connect):
        pass

    def close(self):
        pass
