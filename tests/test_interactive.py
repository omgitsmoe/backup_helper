import pytest

from unittest.mock import patch

from backup_helper.backup_helper import BackupHelper
from backup_helper.interactive import (
    BackupHelperInteractive, _readline_get_argument_idx
)
from backup_helper.cli import build_parser


@patch('backup_helper.cli._cl_stage')
def test_instance_and_status_file_passed(_cl_stage):
    instance = BackupHelper([])
    bhi = BackupHelperInteractive(
        build_parser(), "state_file_passed", instance)

    bhi.parse_params('stage', '/home/test/backup_status.json')
    _cl_stage.assert_called_once()
    assert _cl_stage.call_args.args[0].status_file == 'state_file_passed'
    assert _cl_stage.call_args.kwargs['instance'] is instance


def test_cli_func_system_exit_caught():

    class Raises:
        def parse_args(*args, **kwargs):
            raise SystemExit

    bhi = BackupHelperInteractive(
        Raises(), "state_file_passed")
    bhi.parse_params('stage', '/home/test/backup_status.json')


@pytest.mark.parametrize(
    'line,expected',
    [
        ("stage foo bar", 2),
        ("stage /home/usr\\ name bar", 2),
        ("stage 'foo foo' bar", 2),
        (r"stage 'foo \'foo' bar", 2),
        (r"stage 'foo \'foo' bar ", 3),
        (r'stage "foo \"foo" bar', 2),
        ("stage", 0),
        ("stage ", 1),
        ("stage test", 1),
        ("stage test ", 2),
    ]
)
def test_readline_get_argument_idx(line: str, expected: int):
    assert _readline_get_argument_idx(line) == expected


