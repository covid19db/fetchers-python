import time
import logging

logger = logging.getLogger(__name__)

TIME_DURATION_UNITS = (
    ('week', 60 * 60 * 24 * 7),
    ('day', 60 * 60 * 24),
    ('hour', 60 * 60),
    ('min', 60),
    ('sec', 1)
)


def seconds_to_human(seconds):
    if seconds < 1:
        return f'{seconds}s'
    parts = []
    for unit, div in TIME_DURATION_UNITS:
        amount, seconds = divmod(int(seconds), div)
        if amount > 0:
            parts.append('{} {}{}'.format(amount, unit, "" if amount == 1 else "s"))
    return ', '.join(parts)


def timeit(method):
    def timed(*args, **kw):
        start_time = time.time()
        result = method(*args, **kw)
        hr_time_diff = seconds_to_human(time.time() - start_time)
        logger.info(f'{method.__name__} execution time: {hr_time_diff}')
        return result

    return timed
