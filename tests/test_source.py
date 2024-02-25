import pytest
import os
import threading

from typing import cast

from unittest.mock import patch, MagicMock, call

from checksum_helper.checksum_helper import ChecksumHelperData

from backup_helper.source import Source
from backup_helper.target import Target, VerifiedInfo
import backup_helper.exceptions as exc
import backup_helper.disk_work_queue as dwq
from backup_helper import work


@pytest.mark.parametrize(
    'path,alias,hash_algorithm,hash_file,hash_log_file,targets,force_single_hash,blocklist,expected',
    [
        ('/src1', 'src1', 'md5', None, None, [], False, None, {
            "path": os.path.abspath('/src1'), "alias": "src1",
            "hash_algorithm": "md5", "hash_file": None, "hash_log_file": None,
            "targets": [],
            "force_single_hash": False, "blocklist": [],
        }),
        ('/src2', None, 'md5', 'hash_file.md5', 'hash_file.log',
         [Target('/target1', 'tgt1', False, True, None),
          Target('/target2', None, True, True,
                 VerifiedInfo(checked=4, errors=2, missing=1, crc_errors=1, log_file='/log2'))],
         True, ['foo', 'bar'], {
             "path": os.path.abspath('/src2'), "alias": None,
             "hash_algorithm": "md5", "hash_file": "hash_file.md5",
             "hash_log_file": "hash_file.log",
             "targets": [
                 {
                     "version": 1, "type": "Target",
                     "path": os.path.abspath("/target1"), "alias": "tgt1", "transfered": False,
                     "verify": True, "verified": None,
                 },
                 {
                     "version": 1, "type": "Target",
                     "path": os.path.abspath("/target2"), "alias": None, "transfered": True,
                     "verify": True,
                     "verified": {"checked": 4, "errors": 2, "missing": 1, "crc_errors": 1, "log_file": "/log2"},
                 },
             ],
             "force_single_hash": True, "blocklist": ["foo", "bar"],
         }),
    ]
)
def test_to_json(
        path, alias, hash_algorithm, hash_file, hash_log_file,
        targets, force_single_hash, blocklist, expected):
    expected_default = {
        "version": 1, "type": "Source",
    }
    expected.update(expected_default)
    s = Source(path, alias, hash_algorithm, hash_file,
               hash_log_file, {}, force_single_hash, blocklist)
    if targets:
        for t in targets:
            s.add_target(t)
    assert s.to_json() == expected


@pytest.mark.parametrize('json_obj,expected', [
    ({
        "path": os.path.abspath('/src1'), "alias": "src1",
        "hash_algorithm": "md5", "hash_file": None, "hash_log_file": None,
        "targets": [],
        "force_single_hash": False, "blocklist": [],
    }, Source('/src1', 'src1', 'md5', None, None, {}, False, None)),
    ({
        "path": os.path.abspath('/src2'), "alias": None,
        "hash_algorithm": "md5", "hash_file": "hash_file.md5",
        "hash_log_file": "hash_file.log",
        "targets": [
            Target('/target1', 'tgt1', False, True, None),
            Target('/target2', None, True, True,
                   VerifiedInfo(checked=4, errors=2, missing=1, crc_errors=1, log_file='/log2'))
        ],
        "force_single_hash": True, "blocklist": ["foo", "bar"],
    },
        Source('/src2', None, 'md5', 'hash_file.md5', 'hash_file.log', {},
               True, ['foo', 'bar']),
    )
])
def test_from_json(json_obj, expected: Source):
    json_default = {
        "version": 1, "type": "Source",
    }
    json_obj.update(json_default)
    s = Source.from_json(json_obj)
    assert s.path == expected.path
    assert s.alias == expected.alias
    assert s.hash_algorithm == expected.hash_algorithm
    assert s.hash_file == expected.hash_file
    assert s.hash_log_file == expected.hash_log_file
    assert s.force_single_hash == expected.force_single_hash
    assert s.blocklist == expected.blocklist

    for t, expected in zip(s.unique_targets(), json_obj['targets']):
        cast(Target, t)
        cast(Target, expected)
        if expected.alias:
            assert s.targets[t.alias] is not None
            assert s.targets[t.alias] is s.targets[t.path]

        assert t.path == expected.path
        assert t.alias == expected.alias
        assert t.transfered == expected.transfered
        assert t.verify == expected.verify
        assert t.verified == expected.verified


@pytest.mark.parametrize('field,value,expected', [
    ('path', 'foo', 'foo'),
    ('alias', 'foo', 'foo'),
    ('hash_algorithm', 'foo', 'foo'),
    ('hash_file', 'foo', 'foo'),
    ('hash_log_file', 'foo', 'foo'),
    ('force_single_hash', 'yes', True),
    ('force_single_hash', 'True', True),
    ('force_single_hash', 'skfjla', False),
    ('blocklist', 'foo', ['foo']),
    ('blocklist', '', []),
])
def test_set_modifiable_field(field: str, value: str, expected):
    s = Source('test', None, None, None, None, {})
    s.set_modifiable_field(field, value)
    assert getattr(s, field) == expected


def test_set_modifiable_field_unkown_field():
    s = Source('test', None, None, None, None, {})
    with pytest.raises(ValueError):
        s.set_modifiable_field('fsdlkfjsdsl', 'sdfs')


def test_set_modifiable_field_multivalue():
    s = Source('test', None, None, None, None, {})
    value = ['foo', 'bar']
    s.set_modifiable_field_multivalue('blocklist', value)
    assert s.blocklist == value


def test_set_modifiable_field_multivalue_unkown_incompatible_field():
    s = Source('test', None, None, None, None, {})
    with pytest.raises(ValueError):
        s.set_modifiable_field_multivalue('safjlksadjflksdlk', ['sdfs'])
    with pytest.raises(ValueError):
        s.set_modifiable_field_multivalue('hash_file', ['sdfs'])


def test_modifiable_fields():
    s = Source('test', 'testalias', 'md5', 'hf.md5', None, {},
               False, ['foo', 'bar'])
    assert s.modifiable_fields() == f"""path = {os.path.abspath('test')}
alias = testalias
hash_algorithm = md5
hash_file = hf.md5
hash_log_file = None
force_single_hash = False
blocklist = ['foo', 'bar']"""


def test_unique_targets():
    s = Source('test', 'testalias', 'md5', 'hf.md5', None, {},
               False, ['foo', 'bar'])
    target1 = Target('path1', 'alias', False, True, None)
    target2 = Target('path2', None, False, True, None)
    s.add_target(target1)
    s.add_target(target2)

    assert list(s.unique_targets()) == [
        s.targets[target1.path], s.targets[target2.path]]


def test_add_target():
    s = Source('test', 'testalias', 'md5', 'hf.md5', None, {},
               False, ['foo', 'bar'])
    target1 = Target('path1', 'alias', False, True, None)
    target2 = Target('path2', None, False, True, None)
    s.add_target(target1)
    s.add_target(target2)

    assert len(s.targets) == 3
    assert s.targets[target1.path] is target1
    assert s.targets[target1.alias] is target1
    assert s.targets[target2.path] is target2


def test_add_target_already_exists():
    s = Source('test', 'testalias', 'md5', 'hf.md5', None, {},
               False, ['foo', 'bar'])
    target1 = Target('path1', None, False, True, None)
    s.add_target(target1)

    with pytest.raises(exc.TargetAlreadyExists):
        s.add_target(target1)


def test_add_target_alias_already_exists():
    s = Source('test', 'testalias', 'md5', 'hf.md5', None, {},
               False, ['foo', 'bar'])
    target1 = Target('path1', 'alias', False, True, None)
    s.add_target(target1)
    target2 = Target('path2', 'alias', False, True, None)

    with pytest.raises(exc.AliasAlreadyExists):
        s.add_target(target2)


@pytest.fixture
def setup_source_2targets_1verified():
    src1 = Source(
        'test/1', 'test1', 'md5', 'hashfile1', 'hashlog1', {})
    src1_target1 = Target(
        'test/target/1', 'target1', False, False, None)
    src1_target2 = Target(
        'test/target/2', 'target2', False, True,
        VerifiedInfo(4, 2, 2, 0, 'verifylog2'))
    src1.add_target(src1_target1)
    src1.add_target(src1_target2)

    return src1, src1_target1, src1_target2


def test_get_target(setup_source_2targets_1verified):
    src1, src1_target1, src1_target2 = setup_source_2targets_1verified
    assert src1.get_target(src1_target1.path) is src1_target1
    assert src1.get_target(src1_target1.alias) is src1_target1
    assert src1.get_target(src1_target2.path) is src1_target2


def test_get_target_not_found(setup_source_2targets_1verified):
    src1, src1_target1, src1_target2 = setup_source_2targets_1verified
    with pytest.raises(exc.TargetNotFound):
        src1.get_target('fskdlflsd')


def test_transfer_queue_all_queue_passed_in(monkeypatch, setup_source_2targets_1verified):
    src1, src1_target1, src1_target2 = setup_source_2targets_1verified
    src1_target1.transfered = False
    src1_target2.transfered = False

    q = work.setup_work_queue([])

    src1.transfer_queue_all(q)

    assert len(q._work) == 2
    assert q._work[0].work == work.WorkTransfer(src1, src1_target1)
    assert q._work[1].work == work.WorkTransfer(src1, src1_target2)


def test_transfer_queue_all(monkeypatch, setup_source_2targets_1verified):
    src1, src1_target1, src1_target2 = setup_source_2targets_1verified
    src1_target1.transfered = False
    src1_target2.transfered = True

    q = src1.transfer_queue_all()

    assert len(q._work) == 1
    assert q._work[0].work == work.WorkTransfer(src1, src1_target1)


def test_transfer_all(monkeypatch, setup_source_2targets_1verified):
    src1, src1_target1, src1_target2 = setup_source_2targets_1verified
    src1_target1.transfered = False
    src1_target2.transfered = False

    copied = []

    def patched_copytree(src, dst, *args, **kwargs):
        if dst == src1_target2.path:
            raise RuntimeError("testfail")
        copied.append((src, dst))

    monkeypatch.setattr('shutil.copytree', patched_copytree)

    success, error = src1.transfer_all()

    assert len(success) == 1
    assert len(error) == 1

    assert copied == [(src1.path, src1_target1.path)]
    assert success == [work.WorkTransfer(src1, src1_target1)]
    assert error == [(work.WorkTransfer(src1, src1_target2), "testfail")]


def test_transfer_already(monkeypatch, setup_source_2targets_1verified):
    src1, src1_target1, src1_target2 = setup_source_2targets_1verified
    src1_target1.transfered = True
    src1_target2.transfered = True

    called = []

    def patched_copytree(src, dst, *args, **kwargs):
        called.append(True)

    monkeypatch.setattr('shutil.copytree', patched_copytree)

    src1.transfer(src1_target1)
    assert not called


def test_transfer_already_force(monkeypatch, setup_source_2targets_1verified):
    src1, src1_target1, src1_target2 = setup_source_2targets_1verified
    src1_target1.transfered = True
    src1_target2.transfered = True

    called = []

    def patched_copytree(src, dst, *args, **kwargs):
        called.append(True)

    monkeypatch.setattr('shutil.copytree', patched_copytree)

    src1.transfer(src1_target1, force=True)
    assert called
    assert src1_target1.transfered is True


def test_transfer(monkeypatch, setup_source_2targets_1verified):
    src1, src1_target1, src1_target2 = setup_source_2targets_1verified
    src1_target1.transfered = False
    src1_target2.transfered = False

    called = []

    def patched_copytree(src, dst, *args, **kwargs):
        called.append((src, dst))

    monkeypatch.setattr('shutil.copytree', patched_copytree)

    src1.transfer(src1_target1)
    assert called == [(src1.path, src1_target1.path)]
    assert src1_target1.transfered is True


def test_transfer_blocklist(monkeypatch, setup_source_2targets_1verified):
    src1, src1_target1, src1_target2 = setup_source_2targets_1verified
    src1_target1.transfered = False
    src1_target2.transfered = False
    src1.path = '/test/xyz/'
    src1.blocklist = ['nom*', 'bla/baz/foo*', '*bar*']

    kw = {}

    def patched_copytree(src, dst, *args, **kwargs):
        kw.update(kwargs)

    monkeypatch.setattr('shutil.copytree', patched_copytree)

    src1.transfer(src1_target1)

    ignore_callable = kw['ignore']
    # root dir
    assert ignore_callable(
        '/test/xyz',
        ['nomde', 'amnom', 'foo', 'foo/bar', 'bla', 'bla/foo', 'bla/barbla']
    ) == ['nomde', 'foo/bar', 'bla/barbla']

    assert ignore_callable(
        '/test/xyz/bla',
        ['nomde', 'foo', 'foo/bar', 'baz', 'baz/foo', 'bla/foo', 'bla/barbla']
    ) == ['foo/bar', 'baz/foo', 'bla/barbla']

    assert ignore_callable(
        '/test/xyz/bla/baz',
        ['nomde', 'foo', 'foo.js', 'foo/bar', 'baz',
            'baz/foo', 'bla/foo', 'bla/barbla']
    ) == ['foo', 'foo.js', 'foo/bar', 'bla/barbla']

    assert ignore_callable(
        '/test/xyz/bla/baz/xer',
        ['nomde', 'foo', 'foo.js', 'foo/bar', 'baz',
            'baz/foo', 'bla/foo', 'bla/barbla']
    ) == ['foo/bar', 'bla/barbla']


@patch('backup_helper.source.helpers.setup_thread_log_file')
@patch('backup_helper.source.ch.ChecksumHelper.do_incremental_checksums',
       return_value=None)
def test_hash_empty_hash(
    do_incremental_checksums,
    setup_thread_log_file,
    setup_source_2targets_1verified
):
    src1, src1_target1, src1_target2 = setup_source_2targets_1verified
    src1.hash_file = None
    with pytest.raises(exc.HashError, match='Empty hash.*'):
        src1.hash()


@patch('backup_helper.source.helpers.setup_thread_log_file')
@patch('backup_helper.source.ch.ChecksumHelper.do_incremental_checksums',
       side_effect=RuntimeError("test"))
def test_hash_any_exception(
    do_incremental_checksums,
    setup_thread_log_file,
    setup_source_2targets_1verified
):
    src1, src1_target1, src1_target2 = setup_source_2targets_1verified
    src1.hash_file = None
    with pytest.raises(exc.HashError, match='Failed.*checksums.*'):
        src1.hash()


@patch('backup_helper.source.helpers.setup_thread_log_file')
@patch('backup_helper.source.ch.ChecksumHelper')
def test_hash_checksum_helper_options(
    ChecksumHelper,
    setup_thread_log_file,
    setup_source_2targets_1verified
):
    src1, src1_target1, src1_target2 = setup_source_2targets_1verified
    src1.hash_file = None

    instance = ChecksumHelper.return_value
    d = {}
    instance.options = d
    src1.hash()
    assert d == {
        'include_unchanged_files_incremental': True,
        'discover_hash_files_depth': -1,
        'incremental_skip_unchanged': False,
        'incremental_collect_fstat': True,
    }


@patch('backup_helper.source.helpers.setup_thread_log_file')
@patch('backup_helper.source.ch.ChecksumHelper.do_incremental_checksums')
def test_hash_checksum_helper_params(
    do_incremental_checksums,
    setup_thread_log_file,
    setup_source_2targets_1verified
):
    src1, src1_target1, src1_target2 = setup_source_2targets_1verified
    src1.blocklist = ['foo', 'bar']
    src1.hash_file = None

    src1.hash()

    do_incremental_checksums.assert_called_once_with(
        'md5', single_hash=False, blacklist=['foo', 'bar'], only_missing=False)


@patch('backup_helper.source.helpers.setup_thread_log_file')
@patch('backup_helper.source.ch.logger')
@patch('backup_helper.source.ch.ChecksumHelper')
@patch('backup_helper.source.helpers.sanitize_filename', **{'return_value': 'foopath'})
@patch('backup_helper.source.time.strftime', **{'return_value': 'footime'})
def test_hash_checksum_helper_calls(
    patched_strftime,
    patched_sanitize,
    ChecksumHelper,
    ch_logger,
    setup_thread_log_file,
    setup_source_2targets_1verified
):
    src1, src1_target1, src1_target2 = setup_source_2targets_1verified
    src1.blocklist = ['foo', 'bar']
    src1.hash_file = None
    src1.hash_log_file = None

    instance = ChecksumHelper.return_value
    inc = instance.do_incremental_checksums.return_value
    inc.get_path.return_value = 'foo_hf_path'
    src1.hash('foodir')

    hf_path = os.path.abspath(os.path.join('test', '1', '1_bh_footime.cshd'))
    inc.relocate.assert_called_once_with(hf_path)
    inc.write.assert_called_once()

    log_path = os.path.join(
        'foodir', 'foopath_inc_footime.log')
    setup_thread_log_file.assert_called_once_with(
        ch_logger, log_path)

    assert src1.hash_file == 'foo_hf_path'
    assert src1.hash_log_file == log_path


def setup_src_to_hash(tmp_path, dir_prefix: str, **kwargs):
    hash_dir = tmp_path / dir_prefix
    hash_dir.mkdir()
    hash_log_dir = tmp_path / f'{dir_prefix}_logs'
    hash_log_dir.mkdir()
    src1 = Source(hash_dir, f'{dir_prefix}_alias',
                  'md5', None, None, {}, **kwargs)

    file_hashes = []

    file1 = hash_dir / f'{dir_prefix}_foo.txt'
    with open(file1, 'w') as f:
        f.write('foo')

    file2 = hash_dir / f'{dir_prefix}_xer.txt'
    with open(file2, 'w') as f:
        f.write('xer')

    hash_sub_dir = hash_dir / f'{dir_prefix}_sub'
    hash_sub_dir.mkdir()

    file3 = hash_sub_dir / 'bar'
    with open(file3, 'w') as f:
        f.write('sub_bar')

    file_hashes.extend([
        (file1, 'acbd18db4cc2f85cedef654fccc4a4d8'),
        (file2, 'faa709c5035aea00f9efb278f2ad5df0'),
        (file3, '0b62138143523bb6cbd88f937cb1616a'),
    ])

    return hash_dir, hash_log_dir, src1, file_hashes


@patch('backup_helper.source.helpers.sanitize_filename', **{'return_value': 'foopath'})
@patch('backup_helper.source.time.strftime', **{'return_value': 'footime'})
def test_hash_isolated_log_in_threads(patched_strftime, patched_sanitize, tmp_path):
    tmp = tmp_path
    thread1_dir, thread1_log_dir, src1, _ = setup_src_to_hash(tmp, 'thread1')
    thread2_dir, thread2_log_dir, src2, _ = setup_src_to_hash(tmp, 'thread2')

    def in_thread(log_dir, src):
        src.hash(log_directory=log_dir)

    t1 = threading.Thread(target=in_thread, args=[thread1_log_dir, src1])
    t2 = threading.Thread(target=in_thread, args=[thread2_log_dir, src2])

    t1.start()
    t2.start()
    t1.join()
    t2.join()

    # log of each thread only contain the contents of what was done in that thread
    with open(thread1_log_dir / 'foopath_inc_footime.log') as f:
        assert [[s for s in l.split()[1:] if s != '-'][1:]
                for l in f.readlines()] == [
            ['INFO', 'Wrote', os.path.join(
                thread1_dir, 'thread1_bh_footime.cshd')],
        ]

    with open(thread2_log_dir / 'foopath_inc_footime.log') as f:
        assert [[s for s in l.split()[1:] if s != '-'][1:]
                for l in f.readlines()] == [
            ['INFO', 'Wrote', os.path.join(
                thread2_dir, 'thread2_bh_footime.cshd')],
        ]


@patch('backup_helper.source.helpers.sanitize_filename', **{'return_value': 'foopath'})
@patch('backup_helper.source.time.strftime', **{'return_value': 'footime'})
def test_hash_file_contents(patched_strftime, patched_sanitize, tmp_path):
    tmp = tmp_path
    hash_dir, _, src1, file_hashes = setup_src_to_hash(tmp, 'test1')

    src1.hash()

    hf = ChecksumHelperData(None, hash_dir / 'test1_bh_footime.cshd')
    hf.read()
    assert len(hf) == len(file_hashes)
    for fpath, hash in file_hashes:
        assert hf.get_entry(fpath).hex_hash() == hash


@patch('backup_helper.source.helpers.sanitize_filename', **{'return_value': 'foopath'})
@patch('backup_helper.source.time.strftime', **{'return_value': 'footime'})
def test_hash_thread_log_handler_removed_after(patched_strftime, patched_sanitize, tmp_path):
    tmp = tmp_path
    hash_dir, hash_log_dir, src1, file_hashes = setup_src_to_hash(tmp, 'test1')
    from backup_helper import source as src_module

    src1.hash(log_directory=hash_log_dir)

    src_module.ch.logger.info("Should not be in the log file below!")

    with open(hash_log_dir / 'foopath_inc_footime.log') as f:
        assert [[s for s in l.split()[1:] if s != '-'][1:]
                for l in f.readlines()] == [
            ['INFO', 'Wrote', os.path.join(
                hash_dir, 'test1_bh_footime.cshd')],
        ]


@patch('backup_helper.disk_work_queue.DiskWorkQueue.add_work')
def test_verify_target_queue_all(add_work, setup_source_2targets_1verified):
    src1, src1_target1, src1_target2 = setup_source_2targets_1verified
    src1_target1.transfered = True
    src1_target2.transfered = True
    src1_target1.verify = True
    src1_target2.verify = True
    src1_target1.verified = None
    src1_target2.verified = None

    src1.verify_target_queue_all()

    add_work.assert_has_calls(
        [
            call([work.WorkVerifyTransfer(src1, src1_target1)]),
            call([work.WorkVerifyTransfer(src1, src1_target2)]),
        ]
    )


def test_verify_target_queue_all_use_injected_q(setup_source_2targets_1verified):
    src1, src1_target1, src1_target2 = setup_source_2targets_1verified
    src1_target1.transfered = True
    src1_target2.transfered = True
    src1_target1.verify = True
    src1_target2.verify = True
    src1_target1.verified = None
    src1_target2.verified = None

    class DummyQ:
        def __init__(self):
            self.work = []

        def add_work(self, work):
            self.work.append(work)

    q = DummyQ()
    src1.verify_target_queue_all(q)

    assert len(q.work) == 2
    assert q.work[0] == [work.WorkVerifyTransfer(src1, src1_target1)]
    assert q.work[1] == [work.WorkVerifyTransfer(src1, src1_target2)]


@patch('backup_helper.disk_work_queue.DiskWorkQueue.add_work')
def test_verify_target_queue_all_does_not_queue_not_verify_or_verified(
        add_work, setup_source_2targets_1verified):
    src1, src1_target1, src1_target2 = setup_source_2targets_1verified
    src1_target1.transfered = True
    src1_target2.transfered = True
    src1_target1.verify = False
    src1_target2.verify = True
    src1_target1.verified = None
    src1_target2.verified = VerifiedInfo(4, 2, 1, 1, 'log')

    src1.verify_target_queue_all()

    add_work.assert_not_called()


@patch('backup_helper.target.Target.verify_from')
def test_verify_target_all(verify_from, monkeypatch, setup_source_2targets_1verified):
    src1, src1_target1, src1_target2 = setup_source_2targets_1verified
    src1.hash_file = 'foohashfile'
    src1_target1.transfered = True
    src1_target2.transfered = True
    src1_target1.verify = True
    src1_target2.verify = False

    success, error = src1.verify_target_all()

    verify_from.assert_called_once_with(src1.hash_file)
    assert success == [work.WorkVerifyTransfer(src1, src1_target1)]
    assert error == []
