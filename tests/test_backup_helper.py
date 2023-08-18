import pytest
import json
import os

from backup_helper import backup_helper
from backup_helper.target import VerifiedInfo

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


def test_load_state_uses_passed_instance(read_backup_helper_state_return_written):
    written = read_backup_helper_state_return_written
    expected = backup_helper.BackupHelper.from_json(BH_WITH_ONE_SOURCE_JSON)
    with backup_helper.load_backup_state("test", expected) as bh:
        assert bh is expected


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


@pytest.fixture
def setup_backup_helper_2sources_2targets_1verified():
    bh = backup_helper.BackupHelper([])
    src1 = backup_helper.Source(
        'test/1', 'test1', 'md5', 'hashfile1', 'hashlog1', {})
    src1_target1 = backup_helper.Target(
        'test/target/1', 'target1', False, False, None)
    src1_target2 = backup_helper.Target(
        'test/target/2', 'target2', False, True,
        VerifiedInfo(4, 2, 2, 0, 'verifylog2'))
    src1.add_target(src1_target1)
    src1.add_target(src1_target2)
    src2 = backup_helper.Source(
        'test/2', 'test2', 'md5', 'hashfile2', 'hashlog2', {})
    bh.add_source(src1)
    bh.add_source(src2)

    return {
        'bh': bh, 'src1': src1, 'src2': src2,
        'src1_target1': src1_target1, 'src1_target2': src1_target2
    }


def test_backup_helper_to_json(setup_backup_helper_2sources_2targets_1verified):
    setup = setup_backup_helper_2sources_2targets_1verified
    bh = setup['bh']
    src1 = setup['src1']
    src2 = setup['src2']
    src1_target1 = setup['src1_target1']
    src1_target2 = setup['src1_target2']

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
                            'checked': 4,
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
                'blocklist': [],
                'targets': [],
            },
        ],
    }


# format string, so {{ to escape the {
BH_WITH_2SOURCES_2TARGETS_1VERIFIED_JSON = """
{{
  "version": 1,
  "type": "BackupHelper",
  "sources": [
    {{
      "version": 1,
      "type": "Source",
      "path": "{workdir}{os_sep}test{os_sep}1",
      "alias": "test1",
      "hash_algorithm": "md5",
      "hash_file": "hashfile1",
      "hash_log_file": "hashlog1",
      "force_single_hash": false,
      "blocklist": [],
      "targets": [
        {{
          "version": 1,
          "type": "Target",
          "path": "{workdir}{os_sep}test{os_sep}target{os_sep}1",
          "alias": "target1",
          "transfered": false,
          "verify": false,
          "verified": null
        }},
        {{
          "version": 1,
          "type": "Target",
          "path": "{workdir}{os_sep}test{os_sep}target{os_sep}2",
          "alias": "target2",
          "transfered": false,
          "verify": true,
          "verified": {{
            "checked": 4,
            "errors": 2,
            "missing": 2,
            "crc_errors": 0,
            "log_file": "verifylog2"
          }}
        }}
      ]
    }},
    {{
      "version": 1,
      "type": "Source",
      "path": "{workdir}{os_sep}test{os_sep}2",
      "alias": "test2",
      "hash_algorithm": "md5",
      "hash_file": "hashfile2",
      "hash_log_file": "hashlog2",
      "force_single_hash": true,
      "blocklist": ["bar", "baz"],
      "targets": []
    }}
  ]
}}
"""


def test_backup_helper_from_json(setup_backup_helper_2sources_2targets_1verified):
    setup = setup_backup_helper_2sources_2targets_1verified
    bh = setup['bh']
    src1 = setup['src1']
    src2 = setup['src2']
    src2.blocklist.extend(['bar', 'baz'])
    src2.force_single_hash = True
    src1_target1 = setup['src1_target1']
    src1_target2 = setup['src1_target2']

    # TODO test loading where abspath saved in json is different
    workdir = os.path.abspath('.')
    json_str = BH_WITH_2SOURCES_2TARGETS_1VERIFIED_JSON.format(
        workdir=workdir.replace("\\", "\\\\") if os.sep == '\\' else workdir,
        os_sep="\\\\" if os.sep == '\\' else os.sep)
    print(json_str)

    loaded = backup_helper.BackupHelper.from_json(json_str)

    # 2 sources but 2path 2alias
    assert len(bh._sources) == 4

    loaded_src1 = loaded._sources[src1.path]
    # accessible using alias
    assert loaded_src1 is loaded._sources[src1.alias]
    assert loaded_src1.path == src1.path
    assert loaded_src1.alias == src1.alias

    assert loaded_src1.hash_algorithm == src1.hash_algorithm
    assert loaded_src1.hash_file == src1.hash_file
    assert loaded_src1.hash_log_file == src1.hash_log_file
    assert loaded_src1.force_single_hash is src1.force_single_hash
    assert loaded_src1.blocklist == src1.blocklist

    loaded_src2 = loaded._sources[src2.path]
    # accessible using alias
    assert loaded_src2 is loaded._sources[src2.alias]
    assert loaded_src2.path == src2.path
    assert loaded_src2.alias == src2.alias

    assert loaded_src2.hash_algorithm == src2.hash_algorithm
    assert loaded_src2.hash_file == src2.hash_file
    assert loaded_src2.hash_log_file == src2.hash_log_file
    assert loaded_src2.force_single_hash is src2.force_single_hash
    assert loaded_src2.blocklist == src2.blocklist

    # targets
    # 4targets -> 2path 2alias
    assert len(loaded_src1.targets) == 4
    loaded_src1_target1 = loaded_src1.targets[src1_target1.path]
    assert loaded_src1_target1 is loaded_src1.targets[src1_target1.alias]
    assert loaded_src1_target1.path == src1_target1.path
    assert loaded_src1_target1.alias == src1_target1.alias
    assert loaded_src1_target1.transfered is src1_target1.transfered
    assert loaded_src1_target1.verify is src1_target1.verify
    assert loaded_src1_target1.verified is src1_target1.verified

    loaded_src1_target2 = loaded_src1.targets[src1_target2.path]
    assert loaded_src1_target2 is loaded_src1.targets[src1_target2.alias]
    assert loaded_src1_target2.path == src1_target2.path
    assert loaded_src1_target2.alias == src1_target2.alias
    assert loaded_src1_target2.transfered is src1_target2.transfered
    assert loaded_src1_target2.verify is src1_target2.verify
    assert loaded_src1_target2.verified == src1_target2.verified


def test_unique_sources(setup_backup_helper_2sources_2targets_1verified):
    setup = setup_backup_helper_2sources_2targets_1verified

    order = list(setup['bh'].unique_sources())
    assert len(order) == 2
    assert order[0] is setup['src1']
    assert order[1] is setup['src2']


def test_backup_helper_add_source():
    bh = backup_helper.BackupHelper([])

    src = backup_helper.Source(
        'test/1', 'test1', 'md5', None, None, {}, False, None)
    bh.add_source(src)

    # added with path AND alias
    assert bh._sources[os.path.join(os.path.abspath('.'), 'test', '1')] is src
    assert bh._sources['test1'] is src


def test_backup_helper_add_source_already_present():
    bh = backup_helper.BackupHelper([])

    src = backup_helper.Source(
        'test/1', 'test1', 'md5', None, None, {}, False, None)
    bh.add_source(src)
    with pytest.raises(backup_helper.SourceAlreadyExists):
        bh.add_source(src)


def test_backup_helper_add_source_alias_already_present():
    bh = backup_helper.BackupHelper([])

    src = backup_helper.Source(
        'test/1', 'test1', 'md5', None, None, {}, False, None)
    bh.add_source(src)
    with pytest.raises(backup_helper.AliasAlreadyExists):
        bh.add_source(backup_helper.Source(
            'test/2', 'test1', 'md5', None, None, {}, False, None))


def test_backup_helper_get_source():
    bh = backup_helper.BackupHelper([])
    src = backup_helper.Source(
        # os.sep as first item to creat absolute path
        # then abspath to get a drive letter on windows
        os.path.abspath(os.path.join(os.sep, 'test', '1')),
        'test1', 'md5', None, None, {}, False, None)
    bh.add_source(src)

    assert bh.get_source(
        os.path.abspath(os.path.join(os.sep, 'test', '1'))) is src
    assert bh.get_source('test1') is src


def test_backup_helper_get_source_not_found():
    bh = backup_helper.BackupHelper([])
    src = backup_helper.Source(
        'test/1', 'test1', 'md5', None, None, {}, False, None)
    bh.add_source(src)

    with pytest.raises(backup_helper.SourceNotFound):
        bh.get_source('foo')


def test_backup_helper_hash_all(setup_backup_helper_2sources_2targets_1verified, monkeypatch):
    setup = setup_backup_helper_2sources_2targets_1verified
    setup['src1'].hash_file = None
    setup['src2'].hash_file = None
    setup['src1'].hash_log_file = None
    setup['src2'].hash_log_file = None
    bh = setup['bh']

    def patched_hash_work(self):
        self.source.hash_file = True
        self.source.hash_log_file = True
        return self

    monkeypatch.setattr(
        'backup_helper.work.WorkHash.do_work', patched_hash_work)

    bh.hash_all()
    assert all(s.hash_file for s in bh.unique_sources())
    assert all(s.hash_log_file for s in bh.unique_sources())


def test_backup_helper_hash_all_does_not_queue_hashed(
        setup_backup_helper_2sources_2targets_1verified, monkeypatch):
    setup = setup_backup_helper_2sources_2targets_1verified
    setup['src1'].hash_file = 'unchanged'
    setup['src2'].hash_file = None
    setup['src1'].hash_log_file = 'unchanged'
    setup['src2'].hash_log_file = None
    bh = setup['bh']

    def patched_hash_work(self):
        self.source.hash_file = True
        self.source.hash_log_file = True
        return self

    monkeypatch.setattr(
        'backup_helper.work.WorkHash.do_work', patched_hash_work)

    bh.hash_all()
    assert setup['src1'].hash_file == 'unchanged'
    assert setup['src1'].hash_log_file == 'unchanged'


def test_backup_helper_transfer_all(
        setup_backup_helper_2sources_2targets_1verified,
        monkeypatch):
    setup = setup_backup_helper_2sources_2targets_1verified
    bh = setup['bh']
    setup['src1_target2'].transfered = False
    setup['src1_target2'].verified = None
    monkeypatch.setattr('shutil.copytree', lambda *args, **kwargs: True)
    bh.transfer_all()
    for src in bh.unique_sources():
        assert all(t.transfered for t in src.unique_targets())


def test_backup_helper_transfer_all_exception_does_not_abort(
        setup_backup_helper_2sources_2targets_1verified,
        monkeypatch):
    setup = setup_backup_helper_2sources_2targets_1verified
    bh = setup['bh']
    setup['src1_target2'].transfered = False
    setup['src1_target2'].verified = None
    err_target_path = setup['src1_target2'].path

    def raise_err(src, dst, *args, **kwargs):
        if dst == err_target_path:
            raise RuntimeError("don't abort")

    monkeypatch.setattr('shutil.copytree', raise_err)
    bh.transfer_all()
    for src in bh.unique_sources():
        assert setup['src1_target2'].transfered is False
        assert all(
            not t.transfered if t.path == err_target_path else t.transfered
            for t in src.unique_targets())
