import cmd
import shlex
import argparse

from typing import List, Optional

from backup_helper.backup_helper import BackupHelper


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

    def do_add_target(self, arg: str):
        self.parse_params('add-target', arg)

    def do_modify(self, arg: str):
        self.parse_params('modify', arg)

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
