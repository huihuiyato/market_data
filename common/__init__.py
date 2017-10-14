import threading

def singleton(cls):

    class WrappedCls(object):

        __singleton_lock = threading.Lock()
        __instance = None

        def __new__(self, *args, **kwargs):
            if not WrappedCls.__instance:
                with WrappedCls.__singleton_lock:
                    if not WrappedCls.__instance:
                        WrappedCls.__instance = cls(*args, **kwargs)
            return WrappedCls.__instance

        def __getattribute__(self, name):
            x = getattr(WrappedCls.__instance, name)
            return x

    return WrappedCls
