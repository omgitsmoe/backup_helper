import os
import time
import json
import dataclasses
import logging

from typing import (
    Optional, Dict, Union, List, Any, Iterator, Set
)

from backup_helper import backup_helper
from backup_helper import helpers
from backup_helper.exceptions import (
    TargetNotFound, TargetAlreadyExists, AliasAlreadyExists,
    HashError,
)
from backup_helper.target import Target


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
    # glob pattern, mutually exclusive
    allowlist: List[str]
    blocklist: List[str]

    def __init__(self, path: str, alias: Optional[str], hash_algorithm: str,
                 hash_file: Optional[str], hash_log_file: Optional[str],
                 targets: Dict[str, Target], force_single_hash: bool = False,
                 allowlist: Optional[List[str]] = None,
                 blocklist: Optional[List[str]] = None):
        # TODO realpath, target too?
        self.path = os.path.normpath(os.path.abspath(path))
        self.alias = alias
        self.hash_algorithm = hash_algorithm
        self.hash_file = hash_file
        self.hash_log_file = hash_log_file
        self.targets = targets
        self.force_single_hash = force_single_hash
        if allowlist is None:
            self.allowlist = []
        else:
            self.allowlist = allowlist
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
            json_object["allowlist"],
            json_object["blocklist"],
        )

    def unique_targets(self) -> Iterator[Target]:
        # targets contain both path as well as alias as keys, so we have to
        # deduplicate them
        yield from backup_helper.unique_iterator(self.targets.values())

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

    def hash(self, log_directory: str = '.'):
        log_path = os.path.join(
            log_directory,
            f"{helpers.sanitize_filename(self.path)}_inc_"
            f"{time.strftime('%Y-%m-%dT%H-%M-%S')}.log")
        c = ch.ChecksumHelper(
            self.path,
            # TODO a) does nothing
            # b) log calls of checksum_helper get included in backup_helper log
            # don't use rootlogger with basicConfig create own instead or w/e
            log_path=log_path)
        # always include all files in output hash
        c.options["include_unchanged_files_incremental"] = True
        # unlimited depth
        c.options["discover_hash_files_depth"] = -1
        # TODO provide arg for this
        # or use for re-stage/hash command
        c.options['incremental_skip_unchanged'] = False
        c.options['incremental_collect_fstat'] = True

        incremental = c.do_incremental_checksums(
            self.hash_algorithm, single_hash=self.force_single_hash,
            whitelist=self.allowlist if self.allowlist else None,
            blacklist=self.blocklist if self.blocklist else None,
            # whether to create checksums for files without checksums only
            only_missing=False)

        if incremental is not None:
            incremental.relocate(self._generate_hash_file_path())
            incremental.write()
            self.hash_file = incremental.get_path()
            self.hash_log_file = log_path
            logger.info(
                "Successfully created hash file for '%s', the log was saved "
                "at '%s'!", self.hash_file, log_path)
        else:
            raise HashError("Failed to create cecksums!")

    def _transfer(self, target: Target):
        # this needs to handle skipping files when permissions are missing
        # or you get interrupted by antivirus
        # - return list of skipped files
        raise NotImplementedError

    def transfer(self, target_key: str):
        target = self.get_target(target_key)
        self._transfer(target)

    def transfer_all(self):
        for target in self.unique_targets():
            self._transfer(target)

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
        elif field_name == "allowlist":
            self.allowlist = [value_str]
        elif field_name == "blocklist":
            self.blocklist = [value_str]
        else:
            raise ValueError(f"Unkown field '{field_name}'!")

    def set_modifiable_field_multivalue(self, field_name: str, values: List[str]):
        if field_name == "allowlist":
            self.allowlist = values
        elif field_name == "blocklist":
            self.blocklist = values
        else:
            raise ValueError(
                f"Cannot set multiple values for field '{field_name}'!")
