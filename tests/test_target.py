import pytest
import os

from unittest.mock import patch, MagicMock

from backup_helper.target import Target, VerifiedInfo


@pytest.mark.parametrize('path,alias,transfered,verify,verified,expected', [
    ('/target1', 'tgt1', False, True, None, {
        "path": os.path.abspath("/target1"), "alias": "tgt1", "transfered": False,
        "verify": True, "verified": None,
    }),
    ('/target2', None, True, True,
     VerifiedInfo(checked=4, errors=2, missing=1,
                  crc_errors=1, log_file='/log2'),
     {
         "path": os.path.abspath("/target2"), "alias": None, "transfered": True,
         "verify": True,
         "verified": {"checked": 4, "errors": 2, "missing": 1, "crc_errors": 1, "log_file": "/log2"},
     }),
])
def test_to_json(path, alias, transfered, verify, verified, expected):
    expected_default = {
        "version": 1, "type": "Target",
    }
    expected.update(expected_default)
    t = Target(path, alias, transfered, verify, verified)
    assert t.to_json() == expected


@pytest.mark.parametrize('json_obj,expected', [
    ({
        "path": os.path.abspath("/target1"), "alias": "tgt1", "transfered": False,
        "verify": True, "verified": None,
    }, Target('/target1', 'tgt1', False, True, None)),
    ({
        "path": os.path.abspath("/target2"), "alias": None, "transfered": True,
        "verify": True,
        "verified": {"checked": 4, "errors": 2, "missing": 1, "crc_errors": 1, "log_file": "/log2"},
    },
        Target('/target2', None, True, True,
               VerifiedInfo(checked=4, errors=2, missing=1, crc_errors=1, log_file='/log2')),
    ),
])
def test_from_json(json_obj, expected):
    json_default = {
        "version": 1, "type": "Target",
    }
    json_obj.update(json_default)
    t = Target.from_json(json_obj)
    assert t.path == expected.path
    assert t.alias == expected.alias
    assert t.transfered == expected.transfered
    assert t.verify == expected.verify
    assert t.verified == expected.verified


@pytest.mark.parametrize('field,value,expected', [
    ('path', 'foo', 'foo'),
    ('alias', 'foo', 'foo'),
    ('transfered', 'yes', True),
    ('transfered', 'True', True),
    ('transfered', 'skfjla', False),
    ('verify', 'yes', True),
])
def test_set_modifiable_field(field: str, value: str, expected):
    t = Target('test', None, None, None, None)
    t.set_modifiable_field(field, value)
    assert getattr(t, field) == expected


def test_set_modifiable_field_unkown_field():
    t = Target('test', None, None, None, None)
    with pytest.raises(ValueError):
        t.set_modifiable_field('fsdlkfjsdsl', 'sdfs')


def test_set_modifiable_field_multivalue_unkown_incompatible_field():
    t = Target('test', None, None, None, None)
    with pytest.raises(ValueError):
        t.set_modifiable_field_multivalue('fsdlkfjsdsl', ['sdfs'])


def test_modifiable_fields():
    t = Target('test', 'testalias', False, True, None)
    assert t.modifiable_fields() == f"""path = {os.path.abspath('test')}
alias = testalias
transfered = False
verify = True"""


@patch('backup_helper.source.helpers.setup_thread_log_file')
@patch('backup_helper.source.ch.logger')
@patch('checksum_helper.checksum_helper.ChecksumHelperData')
@patch('backup_helper.source.helpers.sanitize_filename', **{'return_value': 'foohashname'})
@patch('backup_helper.source.time.strftime', **{'return_value': 'footime'})
def test_target_verify_checks_flag(
    patched_strftime,
    patched_sanitize,
    ChecksumHelperData,
    ch_logger,
    setup_thread_log_file,
    caplog
):
    target = Target('foodir', 'fooalias', False, False, None)
    instance = ChecksumHelperData.return_value
    log_name = os.path.join(target.path, 'foohashname_vf_footime.log')
    expected = VerifiedInfo(5, 2, 1, 1, log_name)
    instance.verify.return_value = (
        [('baz', 'crc')],
        ['missing'],
        ['matches', 'foo', 'bar'])
    instance.entries = [1, 2, 3, 4, 5]

    assert target.verify_from('foo:hashname') is None
    assert target.verify_from('foo:hashname', force=True) is None
    target.transfered = True
    assert target.verify_from('foo:hashname', force=True) == expected

    instance.read.assert_called_once()
    instance.verify.assert_called_once()


@patch('backup_helper.source.helpers.setup_thread_log_file')
@patch('backup_helper.source.ch.logger')
@patch('checksum_helper.checksum_helper.ChecksumHelperData')
@patch('backup_helper.source.helpers.sanitize_filename', **{'return_value': 'foohashname'})
@patch('backup_helper.source.time.strftime', **{'return_value': 'footime'})
def test_target_verify(
    patched_strftime,
    patched_sanitize,
    ChecksumHelperData,
    ch_logger,
    setup_thread_log_file,
    caplog
):
    target = Target('foodir', 'fooalias', True, True, None)
    instance = ChecksumHelperData.return_value
    log_name = os.path.join(target.path, 'foohashname_vf_footime.log')
    expected = VerifiedInfo(5, 2, 1, 1, log_name)
    instance.verify.return_value = (
        [('baz', 'crc')],
        ['missing'],
        ['matches', 'foo', 'bar'])
    instance.entries = [1, 2, 3, 4, 5]

    assert target.verify_from('foo:hashname') == expected

    instance.read.assert_called_once()
    instance.verify.assert_called_once()
    setup_thread_log_file.assert_called_once_with(
        ch_logger, log_name)
