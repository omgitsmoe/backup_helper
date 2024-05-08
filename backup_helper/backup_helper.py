import sys
import os
import dataclasses
import json
import contextlib
import logging
import time

from typing import (
    List, Optional, Dict, Any, Union, cast, Callable, Set,
    Iterator, Iterable, TYPE_CHECKING, Tuple, overload
)

from backup_helper import helpers
from backup_helper.exceptions import (
    SourceNotFound, TargetNotFound,
    SourceAlreadyExists, AliasAlreadyExists,
)
from backup_helper.source import Source
from backup_helper.target import Target
from backup_helper import work

logger = logging.getLogger(__name__)


class BackupHelper:
    def __init__(self, sources: List[Source]):
        self._sources = {}
        # don't serialize this, will be set when loading, so the file can be moved!
        self._working_dir = '.'
        for source in sources:
            self._sources[source.path] = source
            if source.alias:
                self._sources[source.alias] = source
        self._queue = work.setup_work_queue([])

    @ classmethod
    def load_state(cls, path: str) -> 'BackupHelper':
        if os.path.exists(path):
            with open(path, "r", encoding='utf-8') as f:
                contents = f.read()
            bh = cls.from_json(contents)
            bh._working_dir = os.path.dirname(path)
            return bh
        else:
            return cls([])

    def to_json(self) -> Dict[Any, Any]:
        result = {"version": 1, "type": type(self).__name__}

        sources: List[Dict[str, Any]] = []
        # sources contain both path as well as alias as keys, so we have to
        # deduplicate them
        for source in self.unique_sources():
            sources.append(source.to_json())

        result["sources"] = sources

        return result

    @ staticmethod
    def from_json(json_str: str) -> 'BackupHelper':
        d = json.loads(json_str, object_hook=BackupHelper.from_json_hook)
        return d

    @ staticmethod
    def from_json_hook(json_object: Dict[Any, Any]) -> Union[
            'BackupHelper', Source, Target, Dict[Any, Any]]:
        # if this is used as object_hook in json_loads it
        # will call the method iteratively as it builds the object bottom up

        if "type" not in json_object:
            return json_object

        # version = json_object["version"]
        obj_type = json_object["type"]

        # dispatch to appropriate from_json method
        if obj_type == "BackupHelper":
            sources = json_object["sources"]
            return BackupHelper(sources)
        elif obj_type == "Source":
            return Source.from_json(json_object)
        elif obj_type == "Target":
            return Target.from_json(json_object)
        else:
            return json_object

    def unique_sources(self) -> Iterator[Source]:
        # sources contain both path as well as alias as keys, so we have to
        # deduplicate them
        yield from helpers.unique_iterator(self._sources.values())

    def save_state(self, path: str):
        d = self.to_json()
        with open(path, "w", encoding='utf-8') as f:
            f.write(json.dumps(d, indent=2))

    def add_source(self, source: Source):
        if source.path in self._sources:
            raise SourceAlreadyExists(
                f"Source '{source.path}' already exists!", source.path)

        self._sources[source.path] = source
        if source.alias:
            if source.alias in self._sources:
                raise AliasAlreadyExists(
                    f"Alias '{source.alias}' already exists!", source.alias)
            self._sources[source.alias] = source

    def get_source(self, source_key: str) -> Source:
        try:
            return self._sources[source_key]
        except KeyError:
            raise SourceNotFound(
                f"Source '{source_key}' not found!", source_key)

    def hash_all(self) -> None:
        for s in self.unique_sources():
            s.hash_queue(self._queue, log_dir=self._working_dir)
        success, errors = self._queue.start_and_join_all()
        work.report_results(success, errors)

    def transfer_all(self) -> None:
        for src in self.unique_sources():
            src.transfer_queue_all(self._queue)

        success, errors = self._queue.start_and_join_all()
        work.report_results(success, errors)

    def verify_all(self) -> None:
        for src in self.unique_sources():
            src.verify_target_queue_all(self._queue)

        success, errors = self._queue.start_and_join_all()
        work.report_results(success, errors)

    def start_all(self) -> None:
        for src in self.unique_sources():
            src.hash_queue(self._queue, log_dir=self._working_dir)
            src.transfer_queue_all(self._queue)
            src.verify_target_queue_all(self._queue)

        success, errors = self._queue.start_and_join_all()
        work.report_results(success, errors)

    def workers_running(self) -> bool:
        return self._queue.workers_running()

    def join(self) -> None:
        self._queue.join()

    def status(self, source_key: str) -> str:
        try:
            src = self.get_source(source_key)
        except SourceNotFound as e:
            return f"Source '{e.source}' not found!"
        else:
            return src.status()

    def status_all(self) -> str:
        builder = []
        for source in self.unique_sources():
            builder.append(f"--- Source: {source.path} ---")
            builder.append(source.status())

        return "\n".join(builder)


@ contextlib.contextmanager
def load_backup_state(
        path: str, instance: Optional[BackupHelper] = None) -> Iterator[BackupHelper]:
    """Contextmangaer that saves state on Exception"""
    if instance is None:
        bh = BackupHelper.load_state(path)
    else:
        bh = instance

    try:
        yield bh
    except Exception:
        fn, ext = os.path.splitext(path)
        bh.save_state(helpers.unique_filename(f"{fn}_crash{ext}"))
        raise


@ contextlib.contextmanager
def load_backup_state_save_always(
        path: str, instance: Optional[BackupHelper] = None) -> Iterator[BackupHelper]:
    """Contextmangaer that saves state on exit"""
    with load_backup_state(path, instance) as bh:
        yield bh
        bh.save_state(path)
