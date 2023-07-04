import pytest
import json
import os

from backup_helper import backup_helper

BH_WITH_ONE_SOURCE_JSON = """{
   "version":1,
   "type":"BackupHelper",
   "sources":[
      {
         "version":1,
         "type":"Source",
         "path":"E:\\bg2",
         "alias":"bg2",
         "hash_algorithm":"md5",
         "hash_file": null,
         "hash_log_file": null,
         "force_single_hash":false,
         "allowlist":[],
         "blocklist":[],
         "targets":[]
      }
   ]
}"""


class MockFile:
    def __init__(self, read_data=None):
        self.read_data = read_data
        self.written = None

    def __enter__(self):
        return self

    def __exit__(self, *args):
        print(args)
        return

    def read(self, *args):
        return self.read_data

    def write(self, contents):
        self.written = contents
        return len(contents)


@pytest.fixture
def read_empty_backup_helper(monkeypatch):
    def mock_open(filename, mode='r', encoding=''):
        return MockFile(
            read_data=json.dumps(backup_helper.BackupHelper([]).to_json()))
    monkeypatch.setattr('builtins.open', mock_open)


@pytest.fixture
def read_backup_helper_state_return_written(monkeypatch):
    written = {'contents': MockFile(), 'filename': None}

    def mock_open(filename, mode='r', encoding=''):
        if 'w' in mode:
            written['filename'] = filename
            return written['contents']
        else:
            return MockFile()
    monkeypatch.setattr('builtins.open', mock_open)
    return written


def test_load_state_saves_crash(read_backup_helper_state_return_written):
    written = read_backup_helper_state_return_written
    try:
        with backup_helper.load_backup_state("test") as bh:
            raise RuntimeError("test")
    except RuntimeError:
        pass

    assert written['filename'] == 'test_crash'
    assert json.loads(written['contents'].written) == backup_helper.BackupHelper(
        []).to_json()


def test_load_state_no_save_without_crash(read_backup_helper_state_return_written):
    written = read_backup_helper_state_return_written
    with backup_helper.load_backup_state("test") as bh:
        pass

    assert written['contents'].written is None


def test_load_state_saves_crash_name_exists(monkeypatch, read_backup_helper_state_return_written):
    written = read_backup_helper_state_return_written

    count = 0

    def mock_exists(path: str):
        if path.startswith("test_crash"):
            nonlocal count
            if count == 3:
                return False
            else:
                count += 1
                return True
        else:
            return False

    monkeypatch.setattr('os.path.exists', mock_exists)
    try:
        with backup_helper.load_backup_state("test") as bh:
            raise RuntimeError("test")
    except RuntimeError:
        pass

    assert written['filename'] == 'test_crash_2'


def test_load_backup_state_save_always_save_normal(read_backup_helper_state_return_written):
    written = read_backup_helper_state_return_written
    with backup_helper.load_backup_state_save_always("test") as bh:
        pass

    assert written['filename'] == 'test'
    assert json.loads(written['contents'].written) == backup_helper.BackupHelper(
        []).to_json()


def test_load_backup_state_save_always_save_crash(read_backup_helper_state_return_written):
    written = read_backup_helper_state_return_written
    # saves as _crash
    try:
        with backup_helper.load_backup_state_save_always("test") as bh:
            raise RuntimeError
    except RuntimeError:
        pass

    assert written['filename'] == 'test_crash'
    assert json.loads(written['contents'].written) == backup_helper.BackupHelper(
        []).to_json()


def test_load_state_creates_sets_workdir(monkeypatch, read_empty_backup_helper):
    monkeypatch.setattr('os.path.exists', lambda *args: True)

    bh = backup_helper.BackupHelper.load_state(
        os.path.join(os.path.abspath('.'),
                     'workdir',
                     'test.json'))
    assert bh._working_dir == os.path.join(os.path.abspath('.'), 'workdir')


def test_backup_helper_to_json_init_state():
    bh = backup_helper.BackupHelper([]).to_json()
    assert bh == {'version': 1, 'type': 'BackupHelper', 'sources': []}


def test_backup_helper_only_save_unique_sources():
    bh = backup_helper.BackupHelper([])
    bh.add_source(
        backup_helper.Source('test/1', 'test1', 'md5', *(2*[None]), {}))
    bh.add_source(
        backup_helper.Source('test/2', 'test2', 'md5', *(2*[None]), {}))

    d = bh.to_json()
    assert len(d['sources']) == 2
    assert d['sources'][0]['path'] == os.path.abspath('test/1')
    assert d['sources'][1]['path'] == os.path.abspath('test/2')


def test_backup_helper_to_json():
    bh = backup_helper.BackupHelper([])
    src1 = backup_helper.Source(
        'test/1', 'test1', 'md5', 'hashfile1', 'hashlog1', {})
    src1_target1 = backup_helper.Target(
        'test/target/1', 'target1', False, False, None)
    src1_target2 = backup_helper.Target(
        'test/target/2', 'target2', False, True,
        backup_helper.VerifiedInfo(2, 2, 0, 'verifylog2'))
    src1.add_target(src1_target1)
    src1.add_target(src1_target2)
    src2 = backup_helper.Source(
        'test/2', 'test2', 'md5', 'hashfile2', 'hashlog2', {})
    bh.add_source(src1)
    bh.add_source(src2)

    d = bh.to_json()
    assert d == {
        'version': 1, 'type': 'BackupHelper',
        'sources': [
            {
                'version': 1, 'type': 'Source',
                'path': src1.path, 'alias': src1.alias,
                'hash_algorithm': src1.hash_algorithm,
                'hash_file': src1.hash_file,
                'hash_log_file': src1.hash_log_file,
                'force_single_hash': src1.force_single_hash,
                'allowlist': [],
                'blocklist': [],
                'targets': [
                    {
                        'version': 1, 'type': 'Target',
                        'path': src1_target1.path,
                        'alias': src1_target1.alias,
                        'transfered': src1_target1.transfered,
                        'verify': src1_target1.verify,
                        'verified': None,
                    },
                    {
                        'version': 1, 'type': 'Target',
                        'path': src1_target2.path,
                        'alias': src1_target2.alias,
                        'transfered': src1_target2.transfered,
                        'verify': src1_target2.verify,
                        'verified': {
                            'errors': 2,
                            'missing': 2,
                            'crc_errors': 0,
                            'log_file': 'verifylog2',
                        },
                    },
                ],
            },
            {
                'version': 1, 'type': 'Source',
                'path': src2.path, 'alias': src2.alias,
                'hash_algorithm': src2.hash_algorithm,
                'hash_file': src2.hash_file,
                'hash_log_file': src2.hash_log_file,
                'force_single_hash': src2.force_single_hash,
                'allowlist': [],
                'blocklist': [],
                'targets': [],
            },
        ],
    }
