import pytest
import os

from backup_helper import helpers


@pytest.mark.parametrize('value,repl_char,expected', [
    ('D://foo?bar*?baz',  '_', 'D___foo_bar__baz'),
    (' D://foo?bar*?baz', '_', 'D___foo_bar__baz'),
    ('/foo<>bar|"baz" ',   '_', '_foo__bar__baz_'),
    ('/foo<>bar|"baz"',   '.', '.foo..bar..baz.'),
])
def test_sanitize_filename(value: str, repl_char: str, expected: str):
    assert helpers.sanitize_filename(
        value, replacement_char=repl_char) == expected


@pytest.mark.parametrize('fn,existing,expected', [
    ('/foo/bar/baz.log', 2, '/foo/bar/baz_1.log'),
    ('/foo/bar/baz.log.txt', 0, '/foo/bar/baz.log.txt'),
    ('/foo/bar/baz.log.txt', 1, '/foo/bar/baz.log_0.txt'),
    ('/foo/bar/baz', 3, '/foo/bar/baz_2'),
])
def test_sanitize_filename(fn: str, existing: int, expected: str, monkeypatch):
    i = 0

    def patched(p):
        nonlocal i
        print(i, p)
        if i == existing:
            return False

        i += 1
        return True

    monkeypatch.setattr('backup_helper.helpers.os.path.exists', patched)
    norm = os.path.normpath(fn)
    norm_expected = os.path.normpath(expected)
    assert helpers.unique_filename(norm) == norm_expected
