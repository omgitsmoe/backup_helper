import pytest

from unittest.mock import patch

from backup_helper.backup_helper import BackupHelper
from backup_helper.interactive import BackupHelperInteractive
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
