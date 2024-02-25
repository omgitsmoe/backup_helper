import logging
import logging.config
import argparse
import os
import sys

from typing import Union, List, Optional

from backup_helper.backup_helper import (
    BackupHelper, Source, Target, load_backup_state, load_backup_state_save_always
)
from backup_helper.exceptions import SourceNotFound, TargetNotFound
from backup_helper.interactive import BackupHelperInteractive
from backup_helper import work


def configure_logging(log_path):
    logging_conf = {
        'version': 1,
        'formatters': {
            'console': {'format': '%(asctime)s - %(levelname)s - %(message)s', 'datefmt': "%H:%M:%S"},
            "file": {"format": "%(asctime)-15s - %(name)-9s - %(levelname)-6s - %(message)s"}
        },
        'handlers': {
            'console': {
                'level': 'INFO',
                'class': 'logging.StreamHandler',
                'formatter': 'console',
                'stream': 'ext://sys.stdout'
            },
        },
        'loggers': {
        },
        "root": {
            'level': 'DEBUG',
            'handlers': ['console']
        },
        'disable_existing_loggers': False
    }
    if log_path:
        logging_conf["handlers"].update({
            'file': {
                'level': 'DEBUG',
                'class': 'logging.handlers.RotatingFileHandler',
                'formatter': 'file',
                'filename': log_path,
                'maxBytes': 1048576,
                'backupCount': 5,
                "encoding": "UTF-8"
            }
        })
        logging_conf.update(
            {
                "root": {
                    'level': 'DEBUG',
                    'handlers': ['console', 'file']
                }
            }
        )

    logging.config.dictConfig(logging_conf)


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
        description="Backup tool for transfering lots of data to multiple "
                    "targets efficiently, while each source/target gets their "
                    "own file containing checksums, which is verified on the "
                    "target.")

    # save name of used subcmd in var subcmd
    subparsers = parser.add_subparsers(
        title='subcommands', description='valid subcommands',
        dest="subcmd")

    # add parser that is used as parent parser for all subcmd parsers so they
    # can have common options without adding arguments to each one
    parent_parser = argparse.ArgumentParser(add_help=False)
    parent_parser.add_argument(
        "--status-file", default="backup_status.json",
        help="Path to the JSON file that contains the state of the current backup "
             "process. Log files will be saved in the same directory.")

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
    stage.add_argument(
        "--single-hash", action="store_true",
        help="Force files to be written as single hash (*.sha512, *.md5, etc.) "
        "files. Does not support storing mtimes (default format is .cshd)!")
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
             "the target in commands")
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
        "value", type=str, nargs='*', help="New value")
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

    verify_target = subparsers.add_parser(
        "verify", parents=[parent_parser],
        help="Verify target(s) (by default all of them)")
    verify_target.add_argument(
        "--source", type=str, metavar='PathOrAlias',
        help="Only verify target(s) of a specific source")
    verify_target.add_argument(
        "--target", type=str, metavar='PathOrAlias',
        help="Only verify a specific target")
    verify_target.set_defaults(func=_cl_verify_target)

    start = subparsers.add_parser(
        "start", parents=[parent_parser],
        help="Starts a queue such that sources are hashed, transfered "
             "and the targets are verified, while using a device at most "
             "once at the same time")
    start.set_defaults(func=_cl_start)

    interactive = subparsers.add_parser(
        "interactive", parents=[parent_parser],
        help="Launch into interactive mode, where you can stage sources and "
             "add targets while the queue is running in the background!")
    interactive.set_defaults(func=_cl_interactive)

    status = subparsers.add_parser(
        "status", parents=[parent_parser],
        help="Show the status of a source (by default all of them)")
    status.add_argument(
        "--source", type=str, metavar='PathOrAlias',
        help="Only show the status of a specific source")
    status.set_defaults(func=_cl_status)

    return parser


def _cl_stage(args: argparse.Namespace, instance: Optional[BackupHelper] = None):
    if not os.path.isdir(args.path):
        print("Error: Path is not a directory:", args.path)
        return

    with load_backup_state_save_always(args.status_file, instance) as bh:
        src = Source(
            args.path, args.alias, args.hash_algorithm, None, None, {},
            force_single_hash=args.single_hash)
        bh.add_source(src)
        print("Staged:", src.path)
        if src.alias:
            print("    with alias:", src.alias)


def _cl_add_target(args: argparse.Namespace, instance: Optional[BackupHelper] = None):
    with load_backup_state_save_always(args.status_file, instance) as bh:
        target = Target(args.path, args.alias, False, not args.no_verify, None)
        bh.get_source(args.source).add_target(target)
        print("Added target", target.path)
        if target.alias:
            print("    with alias:", target.alias)


def _cl_hash(args: argparse.Namespace, instance: Optional[BackupHelper] = None):
    with load_backup_state_save_always(args.status_file, instance) as bh:
        if args.source:
            bh.get_source(args.source).hash()
        else:
            bh.hash_all()


def _cl_transfer(args: argparse.Namespace, instance: Optional[BackupHelper] = None):
    with load_backup_state_save_always(args.status_file, instance) as bh:
        if args.source:
            if args.target:
                bh.get_source(args.source).transfer(args.target)
            else:
                success, errors = bh.get_source(args.source).transfer_all()
                work.report_results(success, errors)
        else:
            bh.transfer_all()


def _cl_verify_target(args: argparse.Namespace, instance: Optional[BackupHelper] = None):
    with load_backup_state_save_always(args.status_file, instance) as bh:
        if args.source:
            if args.target:
                src = bh.get_source(args.source)
                if not src.hash_file:
                    print("Error: Source does not have a hash file!",
                          file=sys.stderr)
                else:
                    src.get_target(
                        args.target).verify_from(src.hash_file)
            else:
                success, errors = bh.get_source(
                    args.source).verify_target_all()
                work.report_results(success, errors)
        else:
            bh.verify_all()


def _cl_start(args: argparse.Namespace, instance: Optional[BackupHelper] = None):
    with load_backup_state_save_always(args.status_file, instance) as bh:
        bh.start_all()


def _cl_status(args: argparse.Namespace, instance: Optional[BackupHelper] = None):
    print("Status:")
    with load_backup_state(args.status_file, instance) as bh:
        if args.source:
            print(bh.status(args.source))
        else:
            print(bh.status_all())


def _cl_modify(args: argparse.Namespace, instance: Optional[BackupHelper] = None):
    print("WARNING: You need to know what you're doing if you modify fields "
          "like src.target.transfered, etc.")
    loader_func = (
        load_backup_state_save_always if args.value else load_backup_state)
    with loader_func(args.status_file, instance) as bh:
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
                    if len(args.value) > 1:
                        target.set_modifiable_field_multivalue(
                            args.key, args.value)
                    else:
                        target.set_modifiable_field(args.key, args.value[0])
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


def _cl_interactive(args: argparse.Namespace, parser: argparse.ArgumentParser,
                    instance: Optional[BackupHelper] = None):
    with load_backup_state_save_always(args.status_file, instance) as bh:
        cmd = BackupHelperInteractive(parser, args.status_file, bh)
        while True:
            try:
                cmd.cmdloop()
            except KeyboardInterrupt:
                print("Intercepted KeyboardInterrupt! Please use the exit "
                      "command to properly leave interactive mode and wait "
                      "for all operations to finish!")
                pass
            else:
                break


def main(args: List[str]) -> None:
    configure_logging(None)
    parser = build_parser()
    parsed_args = parser.parse_args(args)
    if hasattr(parsed_args, 'func') and parsed_args.func:
        workdir = os.path.dirname(parsed_args.status_file)
        configure_logging(os.path.join(workdir, 'backup_helper.log'))

        if parsed_args.func == _cl_interactive:
            _cl_interactive(parsed_args, parser)
        else:
            parsed_args.func(parsed_args)
    else:
        parser.print_usage()
