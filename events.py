class DataEvents:

    _events = set()
    _callbacks = {}

    @staticmethod
    def register(event):
        assert event not in DataEvents._events
        DataEvents._events.add(event)
        DataEvents._callbacks[event] = set()

    @staticmethod
    def on(event, callback):
        assert event in DataEvents._events
        DataEvents._callbacks[event].add(callback)

    @staticmethod
    def remove(event, callback):
        assert event in DataEvents._events
        callbacks = DataEvents._callbacks[event]
        callback in callbacks and callbacks.remove(callback)

    @staticmethod
    def trigger(event, *args, **kwargs):
        assert event in DataEvents._events
        for callback in DataEvents._callbacks[event]:
            callback(event, *args, **kwargs)


DataEvents.register('changed')

