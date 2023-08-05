import os
import dataclasses
import logging
import logging.handlers
import threading
import contextlib

from typing import Callable, TypeVar, Optional, Iterator, Iterable, Set


def sanitize_filename(s: str, replacement_char='_') -> str:
    BANNED_CHARS = ('/', '<', '>', ':', '"', '\\', '|', '?', '*')
    return "".join(c if c not in BANNED_CHARS else replacement_char
                   for c in s.strip())


def bool_from_str(s: str) -> bool:
    if s.lower() in ('y', 'yes', 'true', '1'):
        return True
    return False


T = TypeVar('T')


def format_dataclass_fields(
        dc: T,
        filter: Callable[[dataclasses.Field], bool]) -> str:
    builder = []
    for field in dataclasses.fields(dc):
        if filter(field):
            builder.append(f"{field.name} = {getattr(dc, field.name)}")

    return "\n".join(builder)


def unique_filename(path: str) -> str:
    if not os.path.exists(path):
        return path
    base, filename = os.path.split(path)
    filename, ext = os.path.splitext(filename)
    inc = 0
    while os.path.exists(os.path.join(base, f"{filename}_{inc}{ext}")):
        inc += 1

    return os.path.join(base, f"{filename}_{inc}{ext}")


class ThreadLogFilter(logging.Filter):
    """
    Only shows messages of thead with the same threadid.
    Uses threading.get_ident by default.
    """

    def __init__(self, threadid: Optional[int] = None):
        if threadid is None:
            self._threadid = threading.get_ident()

    def filter(self, record: logging.LogRecord) -> bool:
        if record.thread != self._threadid:
            return False

        return True


def setup_thread_log_file(logger: logging.Logger, log_path: str) -> logging.Handler:
    handler = logging.handlers.RotatingFileHandler(
        log_path,
        maxBytes=10485760,  # 10MiB
        encoding="UTF-8")
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter(
        "%(asctime)-15s - %(name)-9s - %(levelname)-6s - %(message)s")
    handler.setFormatter(formatter)
    filter = ThreadLogFilter()
    handler.addFilter(filter)

    logger.addHandler(handler)

    return handler


@contextlib.contextmanager
def setup_thread_log_file_autoremove(
        logger: logging.Logger, log_path: str) -> Iterator[logging.Handler]:
    handler = setup_thread_log_file(logger, log_path)
    yield handler
    logger.removeHandler(handler)


def unique_iterator(to_iter: Iterable[T], key: str = 'path') -> Iterator[T]:

    seen: Set[str] = set()
    for item in to_iter:
        ident = getattr(item, key)
        if ident not in seen:
            yield item
            seen.add(ident)
