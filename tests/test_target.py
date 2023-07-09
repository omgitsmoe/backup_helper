import pytest
import os

from backup_helper.target import Target, VerifiedInfo


@pytest.mark.parametrize('path,alias,transfered,verify,verified,expected', [
    ('/target1', 'tgt1', False, True, None, {
        "path": os.path.abspath("/target1"), "alias": "tgt1", "transfered": False,
        "verify": True, "verified": None,
    }),
    ('/target2', None, True, True,
     VerifiedInfo(errors=2, missing=1, crc_errors=1, log_file='/log2'),
     {
         "path": os.path.abspath("/target2"), "alias": None, "transfered": True,
         "verify": True,
         "verified": {"errors": 2, "missing": 1, "crc_errors": 1, "log_file": "/log2"},
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
        "verified": {"errors": 2, "missing": 1, "crc_errors": 1, "log_file": "/log2"},
    },
        Target('/target2', None, True, True,
               VerifiedInfo(errors=2, missing=1, crc_errors=1, log_file='/log2')),
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
