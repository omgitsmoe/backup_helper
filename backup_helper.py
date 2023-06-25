import sys
import os
import argparse
import dataclasses
import json
import contextlib
import logging

from typing import List, Optional, Dict, Any, Iterator, Union, cast, Callable

# TODO
logging.basicConfig(filename="backup_helper.log", level=logging.INFO)
logger = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    # > stage anime --alias anime
    # > stage ebooks-add --alias ebooks
    # > add-target anime U:\series\anime --alias e11
    # > add-target anime V:\series\anime --alias e12 --no-verify
    # > modify --source anime hash-algorithm md5
    # > modify --source anime --target e12 verify True
    # > hash
    # > transfer
    # > status
    # anime
    #   path: "..."
    #   hash-algorithm: "sha512"
    #   targets: 2
    #   hash-file: "..." / None
    #   hash-log-file: "..." / None
    #   [{path: "...", transfered: True,
    #     alias: "e11",
    #     verify: True,
    #     verified: { log: "..", errors: 11, missing: 11, crc_missmatch: 0 },
    #    {path: "..." ...}]
    parser = argparse.ArgumentParser(
        description="Create checksum files, copy files and verify the transfer!")

    # save name of used subcmd in var subcmd
    subparsers = parser.add_subparsers(
        title='subcommands', description='valid subcommands',
        dest="subcmd")

    # add parser that is used as parent parser for all subcmd parsers so they
    # can have common options without adding arguments to each one
    parent_parser = argparse.ArgumentParser(add_help=False)
    parent_parser.add_argument(
        "--status-file", nargs=1, default="backup_status.json",
        help="Path to the file that contains the state of the current backup "
             "process")

    stage = subparsers.add_parser(
        "stage", parents=[parent_parser],
        help="Stage a source path for backup")
    stage.add_argument(
        "path", type=str,
        help="Source path, which is also used as target name in commands")
    stage.add_argument(
        "--alias", type=str,
        help="Alias that can be used instead of the full path when addressing "
             "the source in commands")
    stage.add_argument(
        "--hash-algorithm", type=str, default="sha512",
        help="Which hash algorithm to use when creating checksum files")
    stage.set_defaults(func=_cl_stage)

    add_target = subparsers.add_parser(
        "add-target", parents=[parent_parser],
        help="Add a target for a source to back up to. Adding the same path "
             "again will modify the target")
    add_target.add_argument(
        "source", type=str,
        help="Source the target should be added to")
    add_target.add_argument(
        "path", type=str,
        help="Target path, which is also used as 'target' name in commands")
    add_target.add_argument(
        "--alias", type=str,
        help="Alias that can be used instead of the full path when addressing "
             "the source in commands")
    add_target.add_argument(
        "--no-verify", action="store_true",
        help="Don't verify target after transfer")
    add_target.set_defaults(func=_cl_add_target)

    modify = subparsers.add_parser(
        "modify", parents=[parent_parser],
        help="Modify a config value of a source or a target of a source. "
             "If no value is specified the current setting is shown. "
             "Use `status` to list all possible options!")
    modify.add_argument(
        "source", type=str,
        help="The source that should be modified (path or alias)")
    modify.add_argument(
        "key", type=str, nargs='?', help="Attribute to be modified")
    modify.add_argument(
        "value", type=str, nargs='?', help="New value")
    modify.add_argument(
        "--target", type=str,
        help="The target on the source that should be modified (path or alias)")
    modify.set_defaults(func=_cl_modify)

    hash = subparsers.add_parser(
        "hash", parents=[parent_parser],
        help="Hash a source (by default all of them) for backup")
    hash.add_argument(
        "--source", type=str, metavar='PathOrAlias',
        help="Only hash a specific source")
    hash.set_defaults(func=_cl_hash)

    transfer = subparsers.add_parser(
        "transfer", parents=[parent_parser],
        help="Transfer a source (by default all of them) to all of their targets")
    transfer.add_argument(
        "--source", type=str, metavar='PathOrAlias',
        help="Only transfer a specific source")
    transfer.add_argument(
        "--target", type=str, metavar='PathOrAlias',
        help="Only transfer to a specific target")
    transfer.set_defaults(func=_cl_transfer)

    status = subparsers.add_parser(
        "status", parents=[parent_parser],
        help="Show the status of a source (by default all of them)")
    status.add_argument(
        "--source", type=str, metavar='PathOrAlias',
        help="Only show the status of a specific source")
    status.set_defaults(func=_cl_status)

    return parser


class BackupHelperException(Exception):
    def __init__(self):
        super().__init__()


class SourceAlreadyExists(BackupHelperException):
    def __init__(self, source: str):
        super().__init__()
        self.source = source


class TargetAlreadyExists(BackupHelperException):
    def __init__(self, source: str, target: str):
        super().__init__()
        self.source = source
        self.target = target


class AliasAlreadyExists(BackupHelperException):
    def __init__(self, name: str):
        super().__init__()
        self.name = name


class SourceNotFound(BackupHelperException):
    def __init__(self, source: str):
        super().__init__()
        self.source = source


class TargetNotFound(BackupHelperException):
    def __init__(self, source: str, target: str):
        super().__init__()
        self.source = source
        self.target = target


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


@dataclasses.dataclass
class VerifiedInfo:
    errors: int
    missing: int
    crc_errors: int
    log_file: str


@dataclasses.dataclass
class Target:
    path: str
    alias: Optional[str]
    transfered: bool
    verify: bool
    verified: Optional[VerifiedInfo]

    def to_json(self) -> Dict[Any, Any]:
        result = {"version": 1, "type": type(self).__name__}
        for k, v in self.__dict__.items():
            if k in result:
                raise RuntimeError("Duplicate field key")
            elif k == "verified":
                continue
            result[k] = v

        result["verified"] = self.verified.__dict__ if self.verified else None

        return result

    @staticmethod
    def from_json(json_object: Dict[Any, Any]) -> 'Target':
        if json_object["verified"]:
            verified = VerifiedInfo(**json_object["verified"])
        else:
            verified = None
        return Target(
            json_object["path"],
            json_object["alias"],
            json_object["transfered"],
            json_object["verify"],
            verified,
        )

    def modifiable_fields(self) -> str:
        return format_dataclass_fields(self, lambda f: f.name != 'verified')

    def set_modifiable_field(self, field_name: str, value_str: str):
        if field_name == "path":
            self.path = value_str
        elif field_name == "alias":
            self.alias = value_str
        elif field_name == "transfered":
            self.transfered = bool_from_str(value_str)
        elif field_name == "verify":
            self.verify = bool_from_str(value_str)
        else:
            raise ValueError(f"Unkown field '{field_name}'!")


@dataclasses.dataclass
class Source:
    path: str
    alias: Optional[str]
    hash_algorithm: str
    hash_file: Optional[str]
    hash_log_file: Optional[str]
    targets: Dict[str, Target]

    def to_json(self) -> Dict[Any, Any]:
        result = {"version": 1, "type": type(self).__name__}
        for k, v in self.__dict__.items():
            if k in result:
                raise RuntimeError("Duplicate field key")
            elif k == "targets":
                continue
            result[k] = v

        targets: List[Dict[str, Any]] = []
        for target in self.targets.values():
            targets.append(target.to_json())

        result["targets"] = targets

        return result

    @staticmethod
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
        )

    def add_target(self, target: Target):
        if target.path in self.targets:
            raise TargetAlreadyExists(self.path, target.path)
        self.targets[target.path] = target
        if target.alias:
            if target.alias in self.targets:
                raise AliasAlreadyExists(target.alias)
            self.targets[target.alias] = target

    def get_target(self, target_key: str) -> Target:
        try:
            return self.targets[target_key]
        except KeyError:
            raise TargetNotFound(self.path, target_key)

    def hash(self):
        raise NotImplementedError

    def _transfer(self, target: Target):
        raise NotImplementedError

    def transfer(self, target_key: str):
        target = self.get_target(target_key)
        self._transfer(target)

    def transfer_all(self):
        for target in self.targets.values():
            self._transfer(target)

    def status(self) -> str:
        return json.dumps(self.to_json(), indent=2)

    def modifiable_fields(self) -> str:
        return format_dataclass_fields(self, lambda f: f.name != 'targets')

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
        else:
            raise ValueError(f"Unkown field '{field_name}'!")


class BackupHelper:
    def __init__(self, sources: List[Source]):
        self._sources = {}
        for source in sources:
            self._sources[source.path] = source
            if source.alias:
                self._sources[source.alias] = source

    @classmethod
    def load_state(cls, path: str) -> 'BackupHelper':
        if os.path.exists(path):
            with open(path, "r", encoding='utf-8') as f:
                contents = f.read()
            return cls.from_json(contents)
        else:
            return cls([])

    def to_json(self) -> Dict[Any, Any]:
        result = {"version": 1, "type": type(self).__name__}

        sources: List[Dict[str, Any]] = []
        for source in self._sources.values():
            sources.append(source.to_json())

        result["sources"] = sources

        return result

    @staticmethod
    def from_json(json_str: str) -> 'BackupHelper':
        d = json.loads(json_str, object_hook=BackupHelper.from_json_hook)
        return d

    @staticmethod
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

    def save_state(self, path: str):
        d = self.to_json()
        with open(path, "w", encoding='utf-8') as f:
            f.write(json.dumps(d))

    def add_source(self, source: Source):
        if source.path in self._sources:
            raise SourceAlreadyExists(source.path)

        self._sources[source.path] = source
        if source.alias:
            if source.alias in self._sources:
                raise AliasAlreadyExists(source.alias)
            self._sources[source.alias] = source

    def get_source(self, source_key: str) -> Source:
        try:
            return self._sources[source_key]
        except KeyError:
            raise SourceNotFound(source_key)

    def add_target(self, source_key: str, target: Target):
        self.get_source(source_key).add_target(target)

    def hash(self, source_key: str):
        self.get_source(source_key).hash()

    def hash_all(self):
        for src in self._sources.values():
            src.hash()

    def transfer_from_source_to_target(
            self, source_key: str, transfer_key: str):
        self.get_source(source_key).transfer(transfer_key)

    def transfer_from_source(
            self, source_key: str):
        self.get_source(source_key).transfer_all()

    def transfer_all(self):
        for src in self._sources.values():
            src.transfer_all()

    def status(self, source_key: str) -> str:
        try:
            src = self.get_source(source_key)
        except SourceNotFound as e:
            return f"Source '{e.source}' not found!"
        else:
            return src.status()

    def status_all(self) -> str:
        builder = []
        for source in self._sources.values():
            builder.append(f"--- Source: {source.path} ---")
            builder.append(source.status())

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


@contextlib.contextmanager
def load_backup_state(path: str) -> Iterator[BackupHelper]:
    """Contextmangaer that saves state on Exception"""
    bh = BackupHelper.load_state(path)
    try:
        yield bh
    except Exception:
        fn, ext = os.path.splitext(path)
        bh.save_state(unique_filename(f"{fn}_crash{ext}"))
        raise


@contextlib.contextmanager
def load_backup_state_save_always(path: str) -> Iterator[BackupHelper]:
    """Contextmangaer that saves state on exit"""
    with load_backup_state(path) as bh:
        yield bh
        bh.save_state(path)


def _cl_stage(args: argparse.Namespace):
    with load_backup_state_save_always(args.status_file) as bh:
        bh.add_source(Source(
            args.path, args.alias, args.hash_algorithm, None, None, {}))
        print("Staged:", args.path)
        if args.alias:
            print("    with alias:", args.alias)


def _cl_add_target(args: argparse.Namespace):
    with load_backup_state_save_always(args.status_file) as bh:
        bh.add_target(
            args.source,
            Target(args.path, args.alias, False, not args.no_verify, None))
        print("Added target", args.path)
        if args.alias:
            print("    with alias:", args.alias)


def _cl_hash(args: argparse.Namespace):
    with load_backup_state_save_always(args.status_file) as bh:
        if args.source:
            bh.hash(args.source)
        else:
            bh.hash_all()


def _cl_transfer(args: argparse.Namespace):
    with load_backup_state_save_always(args.status_file) as bh:
        if args.source:
            if args.target:
                bh.transfer_from_source_to_target(args.source, args.target)
            else:
                bh.transfer_from_source(args.source)
        else:
            bh.transfer_all()


def _cl_status(args: argparse.Namespace):
    print("Status:")
    with load_backup_state(args.status_file) as bh:
        if args.source:
            print(bh.status(args.source))
        else:
            print(bh.status_all())


def _cl_modify(args: argparse.Namespace):
    print("WARNING: You need to know what you're doing if you modify fields "
          "like src.target.transfered, etc.")
    loader_func = (
        load_backup_state_save_always if args.value else load_backup_state)
    with loader_func(args.status_file) as bh:
        try:
            src = bh.get_source(args.source)
        except SourceNotFound as e:
            print(f"ERROR: No such source '{e.source}'!")
            return

        target: Union[Source, Target] = src
        if args.target:
            try:
                target = src.get_target(args.target)
            except TargetNotFound as e:
                print(
                    f"ERROR: No such target '{e.target}' on source '{e.source}'!")
                return
            else:
                target = target

        if args.key:
            if args.value:
                try:
                    target.set_modifiable_field(args.key, args.value)
                except ValueError:
                    print("ERROR: Unkown field or could not convert value!")
                else:
                    print(f"{args.key} = {getattr(target, args.key)}")
            else:
                try:
                    print(f"{args.key} = {getattr(target, args.key)}")
                except AttributeError:
                    print("ERROR: Unkown field!")
        else:
            print(target.modifiable_fields())

# TODO use realpath to decide which operations can run in parallel
# (to see which have same root drive)


if __name__ == "__main__":
    parser = build_parser()
    parsed_args = parser.parse_args()
    if hasattr(parsed_args, 'func') and parsed_args.func:
        parsed_args.func(parsed_args)