import pytest
import os

from backup_helper.source import Source
from backup_helper.target import Target, VerifiedInfo
import backup_helper.exceptions as exc
import backup_helper.disk_work_queue as dwq


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
                 VerifiedInfo(errors=2, missing=1, crc_errors=1, log_file='/log2'))],
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
                     "verified": {"errors": 2, "missing": 1, "crc_errors": 1, "log_file": "/log2"},
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
                   VerifiedInfo(errors=2, missing=1, crc_errors=1, log_file='/log2'))
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
        VerifiedInfo(2, 2, 0, 'verifylog2'))
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

    q = Source.setup_transfer_queue()

    src1.transfer_queue_all(q)

    assert len(q._work) == 2
    assert q._work[0].work == (src1, src1_target1)
    assert q._work[1].work == (src1, src1_target2)
    assert q._work[0].involved_devices == [
        dwq.get_device_identifier(src1.path),
        dwq.get_device_identifier(src1_target1.path)]
    assert q._work[0].involved_devices == [
        dwq.get_device_identifier(src1.path),
        dwq.get_device_identifier(src1_target2.path)]


def test_transfer_queue_all(monkeypatch, setup_source_2targets_1verified):
    src1, src1_target1, src1_target2 = setup_source_2targets_1verified
    src1_target1.transfered = False
    src1_target2.transfered = True

    q = src1.transfer_queue_all()

    assert len(q._work) == 1
    assert q._work[0].work == (src1, src1_target1)
    assert q._work[0].involved_devices == [
        dwq.get_device_identifier(src1.path),
        dwq.get_device_identifier(src1_target1.path)]


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
    assert success == [(src1, src1_target1)]
    assert error == [((src1, src1_target2), "testfail")]


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


# TODO
# - hash
#   - one log per thread in log_directory
#   - hash file name
#   - hash file contents
