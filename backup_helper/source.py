import os
import time
import json
import dataclasses
import logging
import shutil
import threading
import fnmatch

from typing import (
    Optional, Dict, Union, List, Any, Iterator, Set, overload, Tuple, Iterable,
    Callable
)

from backup_helper import helpers
from backup_helper.exceptions import (
    TargetNotFound, TargetAlreadyExists, AliasAlreadyExists,
    HashError, BackupHelperException
)
from backup_helper.target import Target
from backup_helper.disk_work_queue import DiskWorkQueue
from backup_helper import work


from checksum_helper import checksum_helper as ch


logger = logging.getLogger(__name__)


@ dataclasses.dataclass
class Source:
    path: str
    alias: Optional[str]
    hash_algorithm: str
    hash_file: Optional[str]
    hash_log_file: Optional[str]
    targets: Dict[str, Target]
    force_single_hash: bool
    # glob patterns
    blocklist: List[str]

    def __init__(self, path: str, alias: Optional[str], hash_algorithm: str,
                 hash_file: Optional[str], hash_log_file: Optional[str],
                 targets: Dict[str, Target], force_single_hash: bool = False,
                 blocklist: Optional[List[str]] = None):
        # TODO realpath, target too?
        self.path = os.path.normpath(os.path.abspath(path))
        self.alias = alias
        self.hash_algorithm = hash_algorithm
        self.hash_file = hash_file
        self.hash_log_file = hash_log_file
        self.targets = targets
        self.force_single_hash = force_single_hash
        if blocklist is None:
            self.blocklist = []
        else:
            self.blocklist = blocklist

    def to_json(self) -> Dict[Any, Any]:
        result = {"version": 1, "type": type(self).__name__}
        for k, v in self.__dict__.items():
            if k in result:
                raise RuntimeError("Duplicate field key")
            elif k == "targets":
                continue
            result[k] = v

        targets: List[Dict[str, Any]] = []
        # targets contain both path as well as alias as keys, so we have to
        # deduplicate them
        seen: Set[str] = set()
        for target in self.targets.values():
            if target.path not in seen:
                targets.append(target.to_json())
                seen.add(target.path)

        result["targets"] = targets

        return result

    @ staticmethod
    def from_json(json_object: Dict[Any, Any]) -> 'Source':
        targets: Dict[str, Target] = {}
        for target in json_object["targets"]:
            targets[target.path] = target
            targets[target.alias] = target

        return Source(
            json_object["path"],
            json_object["alias"],
            json_object["hash_algorithm"],
            json_object["hash_file"],
            json_object["hash_log_file"],
            targets,
            json_object["force_single_hash"],
            json_object["blocklist"],
        )

    def unique_targets(self) -> Iterator[Target]:
        # targets contain both path as well as alias as keys, so we have to
        # deduplicate them
        yield from helpers.unique_iterator(self.targets.values())

    def add_target(self, target: Target):
        if target.path in self.targets:
            raise TargetAlreadyExists(
                f"Target '{target.path}' already exists on source '{self.path}'!",
                self.path, target.path)
        self.targets[target.path] = target
        if target.alias:
            if target.alias in self.targets:
                raise AliasAlreadyExists(
                    f"Alias '{target.alias}' already exists on source '{self.path}'!",
                    target.alias)
            self.targets[target.alias] = target

    def get_target(self, target_key: str) -> Target:
        try:
            return self.targets[target_key]
        except KeyError:
            raise TargetNotFound(
                f"Target '{target_key}' not found on source '{self.path}'!",
                self.path, target_key)

    def _generate_hash_file_path(self) -> str:
        hashed_directory_name = os.path.basename(self.path)
        hash_file_name = os.path.join(
            self.path,
            f"{hashed_directory_name}_bh_{time.strftime('%Y-%m-%dT%H-%M-%S')}"
            f".{self.hash_algorithm if self.force_single_hash else 'cshd'}"
        )

        return hash_file_name

    def hash(self, log_directory: str = '.', force: bool = False):
        if self.hash_file and not force:
            return

        log_path = os.path.join(
            log_directory,
            f"{helpers.sanitize_filename(self.path)}_inc_"
            f"{time.strftime('%Y-%m-%dT%H-%M-%S')}.log")

        # auto removes logging handler once done
        with helpers.setup_thread_log_file_autoremove(ch.logger, log_path):
            c = ch.ChecksumHelper(self.path)
            # always include all files in output hash
            c.options["include_unchanged_files_incremental"] = True
            # unlimited depth
            c.options["discover_hash_files_depth"] = -1
            # TODO provide arg for this
            # or use for re-stage/hash command
            c.options['incremental_skip_unchanged'] = False
            c.options['incremental_collect_fstat'] = True

            try:
                incremental = c.do_incremental_checksums(
                    self.hash_algorithm, single_hash=self.force_single_hash,
                    blacklist=self.blocklist if self.blocklist else None,
                    # whether to create checksums for files without checksums only
                    only_missing=False)
            except Exception:
                logger.exception("Failed to create checksums!")
                raise HashError("Failed to create checksums!")
            else:
                if incremental is not None:
                    incremental.relocate(self._generate_hash_file_path())
                    incremental.write()
                    self.hash_file = incremental.get_path()
                    self.hash_log_file = log_path
                    logger.info(
                        "Successfully created hash file for '%s', the log was saved "
                        "at '%s'!", self.hash_file, log_path)
                else:
                    raise HashError("Empty hash file!")

    def hash_queue(self, queue: work.WorkQueue, log_dir: str):
        if not self.hash_file:
            queue.add_work([work.WorkHash(self, log_dir=log_dir)])

    def _create_fnmatch_ignore(self) -> Optional[
            Callable[[str, List[str]], Iterable[str]]]:
        if not self.blocklist:
            return None

        def _ignore(path: str, names: List[str]) -> Iterable[str]:
            # path is absolute, create relpath from self.path so we can
            # compare it against the full relative path and not just the name
            # (fn generated by shutil.ignore_patterns will just match against
            #  the name)
            relpath_dir = os.path.relpath(path, start=self.path)
            ignored = []
            for name in names:
                # so that relpath doesn't start with ./ or .\
                relpath = os.path.join(
                    '' if relpath_dir == '.' else relpath_dir, name)
                print(relpath)
                for pattern in self.blocklist:
                    if fnmatch.fnmatch(relpath, pattern):
                        ignored.append(name)
                        # already ignored
                        break

            logger.debug("Source '%s' ignoring file(s) in '%s':\n%s",
                         self.path, relpath, "\n".join(n for n in ignored))
            return ignored

        return _ignore

    def _transfer(self, target: Target, force: bool = False):
        if target.transfered and not force:
            return
        # this needs to handle skipping files when permissions are missing
        # or you get interrupted by antivirus
        # -> return list of skipped files
        # -> already handled by copytree, where all errors get raised at the
        #    end as part of shutil.Error
        logger.info("Tranferring %s to %s on thread %d ...",
                    self.path, target.path, threading.get_ident())

        try:
            shutil.copytree(self.path, target.path, dirs_exist_ok=True,
                            ignore=self._create_fnmatch_ignore())
        except shutil.Error as e:
            # e.args[0] contains a list of 3-tuples with (src, dst, error)
            logger.warning(
                "Failed to copy the following files when transferring '%s' "
                "to '%s':\n%s",
                self.path, target.path,
                "\n".join(f"{err}: {src} -> {dst}" for src,
                          dst, err in e.args[0]))
        else:
            target.transfered = True

    @ overload
    def transfer(
        self, target: str, force: bool = False) -> None: ...

    @ overload
    def transfer(
        self, target: Target, force: bool = False) -> None: ...

    def transfer(
        self,
        target: Union[str, Target],
        force: bool = False,
    ) -> None:
        if isinstance(target, str):
            target = self.get_target(target)
            self._transfer(target, force)
        else:
            self._transfer(target, force)

    def transfer_queue_all(self, queue: Optional[work.WorkQueue] = None) -> work.WorkQueue:
        if queue is None:
            queue = work.setup_work_queue([])

        for target in self.unique_targets():
            if target.transfered:
                continue
            queue.add_work([work.WorkTransfer(self, target)])

        return queue

    def transfer_all(self, queue: Optional[work.WorkQueue] = None) -> Tuple[
            List[work.WorkType], List[Tuple[work.WorkResult, str]]]:
        queue = self.transfer_queue_all(queue)
        return queue.start_and_join_all()

    def verify_target_queue_all(self, queue: Optional[work.WorkQueue] = None) -> work.WorkQueue:
        # NOTE: not having a hash_file yet is fine, since the WorkVerifyTransfer
        # will not be `is_ready` until this source has a hash_file

        if queue is None:
            queue = work.setup_work_queue([])

        for target in self.unique_targets():
            if not target.verify or target.verified:
                continue
            queue.add_work([work.WorkVerifyTransfer(self, target)])

        return queue

    def verify_target_all(self, queue: Optional[work.WorkQueue] = None) -> Tuple[
            List[work.WorkType], List[Tuple[work.WorkResult, str]]]:
        queue = self.verify_target_queue_all(queue)
        return queue.start_and_join_all()

    def status(self) -> str:
        return json.dumps(self.to_json(), indent=2)

    def modifiable_fields(self) -> str:
        return helpers.format_dataclass_fields(self, lambda f: f.name != 'targets')

    def set_modifiable_field(self, field_name: str, value_str: str):
        if field_name == "path":
            self.path = value_str
        elif field_name == "alias":
            self.alias = value_str
        elif field_name == "hash_algorithm":
            self.hash_algorithm = value_str
        elif field_name == "hash_file":
            self.hash_file = value_str
        elif field_name == "hash_log_file":
            self.hash_log_file = value_str
        elif field_name == "force_single_hash":
            self.force_single_hash = helpers.bool_from_str(value_str)
        elif field_name == "blocklist":
            if not value_str:
                self.blocklist = []
            else:
                self.blocklist = [value_str]
        else:
            raise ValueError(f"Unkown field '{field_name}'!")

    def set_modifiable_field_multivalue(self, field_name: str, values: List[str]):
        if field_name == "blocklist":
            self.blocklist = values
        else:
            raise ValueError(
                f"Cannot set multiple values for field '{field_name}'!")
