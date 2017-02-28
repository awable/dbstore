def first(it, default=None):
    return next(iter(it or []), default)
