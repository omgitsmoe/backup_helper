import cmd
import shlex
import argparse
import os
import glob

from typing import Optional

from backup_helper.backup_helper import BackupHelper
from backup_helper.exceptions import BackupHelperException


def _readline_get_argument_idx(line: str) -> int:
    # use punctuation_chars=True so / - etc. are not returned as single tokens
    lexer = shlex.shlex(line, posix=True, punctuation_chars=True)
    # allow ' to be used as escaped quote as well
    # -> also disables that enclosing chars in '' preserves their literal value
    lexer.escapedquotes = "'\""
    idx = sum(1 for _ in lexer) - 1
    if line.endswith(" "):
        # already at start of next arg
        idx += 1

    return idx


def _format_path_completion(path: str) -> str:
    if os.path.isdir(path) and not path.endswith(os.sep):
        return f"{path}{os.sep}"
    return path


def _readline_get_full_arg(
    text: str, line: str, begidx: int, endidx: int, arg_sep: str = " "
) -> tuple[str, int]:
    # NOTE: readline treats chars like / or - as separators, so path
    #       comletion doesn't work out of the box, since
    #       "command /home/usr-name" would call us with
    #       text: "name" instead of "usr-name"
    #       and we need to return sth. starting with `text`
    space_idx_before_arg = line.rfind(arg_sep, 0, begidx)
    if space_idx_before_arg == -1:
        raise BackupHelperException(
            f"Argument separator '{arg_sep}' not found before idx {begidx} "
            f"in command line '{line}'")

    # we need to return sth. starting with `text` but it may start
    # at the wron location, so need to trim the start of the string
    # later
    chars_to_remove_from_result = begidx - space_idx_before_arg
    full_arg = line[space_idx_before_arg + 1:endidx]

    return full_arg, chars_to_remove_from_result


def _complete_path_argument(
    text: str, line: str, begidx: int, endidx: int
) -> list[str]:
    try:
        full_path, chars_to_remove_from_result = _readline_get_full_arg(
            text, line, begidx, endidx)
    except BackupHelperException:
        return []

    pattern = f"{full_path}*"

    completions = []
    for path in glob.glob(pattern):
        formatted = _format_path_completion(path)
        trimmed = formatted[chars_to_remove_from_result - 1:]
        completions.append(trimmed)

    return completions


class BackupHelperInteractive(cmd.Cmd):
    intro = ""
    prompt = "bh > "

    def __init__(self, parser: argparse.ArgumentParser, state_file_path: str,
                 instance: Optional[BackupHelper] = None):
        super().__init__()
        self.parser = parser
        self._state_file = state_file_path
        if instance is None:
            self._instance = BackupHelper.load_state(state_file_path)
        else:
            self._instance = instance

    def parse_params(self, command: str, argline: str):
        args = shlex.split(argline)
        args.insert(0, command)

        try:
            parsed_args = self.parser.parse_args(args)
            parsed_args.status_file = self._state_file
        except SystemExit:
            # don't auto-exit after a command
            pass
        else:
            if hasattr(parsed_args, 'func') and parsed_args.func:
                parsed_args.func(parsed_args, instance=self._instance)
            else:
                self.parser.print_usage()

    # commands are methods prefixed wiht `do_`, so the command 'help'
    # would map to the `do_help` method
    def do_help(self, arg: str):
        self.parser.print_help()
        if arg:
            print("To get help on subcommands use `<subcommand> --help`")

    def do_stage(self, arg: str):
        self.parse_params('stage', arg)

    def complete_stage(
        self, text: str, line: str, begidx: int, endidx: int
    ) -> list[str]:
        return _complete_path_argument(text, line, begidx, endidx)

    def do_add_target(self, arg: str):
        self.parse_params('add-target', arg)

    def complete_add_target(
        self, text: str, line: str, begidx: int, endidx: int
    ) -> list[str]:
        arg_idx = _readline_get_argument_idx(line)
        if arg_idx == 1:
            return self._complete_source_argument(text, line, begidx, endidx)
        elif arg_idx == 2:
            return _complete_path_argument(text, line, begidx, endidx)
        else:
            return []

    def do_modify(self, arg: str):
        self.parse_params('modify', arg)

    def complete_modify(
        self, text: str, line: str, begidx: int, endidx: int
    ) -> list[str]:
        arg_idx = _readline_get_argument_idx(line)
        if arg_idx == 1:
            return self._complete_source_argument(text, line, begidx, endidx)
        else:
            return []

    def do_hash(self, arg: str):
        self.parse_params('hash', arg)

    def do_transfer(self, arg: str):
        self.parse_params('transfer', arg)

    def do_verify(self, arg: str):
        self.parse_params('verify', arg)

    def do_start(self, arg: str):
        self.parse_params('start', arg)

    def do_status(self, arg: str):
        self.parse_params('status', arg)

    def do_exit(self, arg: str):
        print("Waiting on running workers...")
        # wait till running workers are finished
        while self._instance.workers_running():
            try:
                self._instance.join()
            except KeyboardInterrupt:
                print("Interrupted!")
                break
        else:
            print("All workers done!")
            # will only happen if loop wasn't exited by a break
            raise SystemExit

    def emptyline(self):
        # default method repeats last cmd, overwrite to prevent this
        pass

    def close(self):
        pass

    def _complete_source_argument(
        self, text: str, line: str, begidx: int, endidx: int
    ) -> list[str]:
        if not text:
            return list(self._instance._sources.keys())

        full_arg, chars_to_remove_from_result = _readline_get_full_arg(
            text, line, begidx, endidx)

        return [key[chars_to_remove_from_result - 1:]
                for key in self._instance._sources.keys()
                if key.startswith(full_arg)]
