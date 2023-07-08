import os
import dataclasses

from typing import Callable


def sanitize_filename(s: str, replacement_char='_') -> str:
    BANNED_CHARS = ('/', '<', '>', ':', '"', '\\', '|', '?', '*')
    return "".join(c if c not in BANNED_CHARS else replacement_char
                   for c in s.strip())


def bool_from_str(s: str) -> bool:
    if s.lower() in ('y', 'yes', 'true', '1'):
        return True
    return False


def format_dataclass_fields(dc: dataclasses.dataclass,
                            filter: Callable[[dataclasses.Field], bool]) -> str:
    builder = []
    for field in dataclasses.fields(dc):
        if filter(field):
            builder.append(f"{field.name} = {getattr(dc, field.name)}")

    return "\n".join(builder)


def get_device_identifier(path: str) -> int:
    # st_dev
    # Identifier of the device on which this file resides.
    stat = os.stat(path)
    return stat.st_dev


def unique_filename(path: str) -> str:
    if not os.path.exists(path):
        return path
    base, filename = os.path.split(path)
    filename, ext = os.path.splitext(filename)
    inc = 0
    while os.path.exists(os.path.join(base, f"{filename}_{inc}{ext}")):
        inc += 1

    return os.path.join(base, f"{filename}_{inc}{ext}")
