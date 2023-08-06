import pytest
import pathlib
import json
import os

from typing import List, Tuple

from backup_helper.backup_helper import BackupHelper
from backup_helper import cli
from backup_helper.source import Source
from backup_helper.target import Target, VerifiedInfo
from backup_helper import helpers

from checksum_helper.checksum_helper import gen_hash_from_file


def create_source_dir(
    source_dir: pathlib.Path,
    hash_type: str = 'md5',
    **kwargs
) -> List[Tuple[pathlib.Path, str]]:
    file_hashes = []

    file1 = source_dir / f'{source_dir.name}_foo.txt'
    with open(file1, 'w') as f:
        f.write('foo')

    file2 = source_dir / f'{source_dir.name}_xer.txt'
    with open(file2, 'w') as f:
        f.write('xer')

    hash_sub_dir = source_dir / f'{source_dir.name}_sub'
    hash_sub_dir.mkdir()

    file3 = hash_sub_dir / 'bar'
    with open(file3, 'w') as f:
        f.write('sub_bar')

    hash_sub_sub_dir = hash_sub_dir / f'{source_dir.name}_sub_sub'
    hash_sub_sub_dir.mkdir(parents=True)
    file4 = hash_sub_sub_dir / f'{source_dir.name}_baz.txt'
    with open(file4, 'w') as f:
        f.write('sub_sub_baz')

    if hash_type == 'md5':
        file_hashes.extend([
            (file1, 'acbd18db4cc2f85cedef654fccc4a4d8'),
            (file2, 'faa709c5035aea00f9efb278f2ad5df0'),
            (file3, '0b62138143523bb6cbd88f937cb1616a'),
            (file4, '15a3b75b6ba3f1278e3d93088f4b571b'),
        ])
    elif hash_type == 'sha512':
        file_hashes.extend([
            (file1, 'f7fbba6e0636f890e56fbbf3283e524c6fa3204ae298382d624741d0'
                    'dc6638326e282c41be5e4254d8820772c5518a2c5a8c0c7f7eda1959'
                    '4a7eb539453e1ed7'),
            (file2, '605e44784a6e0cf6528f0506964f33876f1c6eb8c2332872d89a405a'
                    'e963745264c5a59f836fb271234bb8f85e3f308204e8ed1e8e1218b8'
                    '8516357abecba9e1'),
            (file3, '6ae501e4dee977b629afac7d5632fe3884b1e55d43509ab58820c980'
                    '41e694ab5dde72520699b0404db2c6895c4b0dd8e2c202eed4dc277f'
                    'ec00af9d76faa7eb'),
            (file4, '98b7a10e466970debcb9587898e78498e81da80fd6a7fb6a67ade6ca'
                    '7c256908e00c59726f5f96fe45eea9d2a2b41e040f1e931efc65b302'
                    'fd75823a0471e5fc'),
        ])
    else:
        raise RuntimeError(f"Unsupported hash type: {hash_type}")

    return file_hashes


def hash_file_match(
    base_path: pathlib.Path,
    actual: List[str],
    expected_abspath_hash: List[Tuple[pathlib.Path, str]]
):
    actual_rel_fn_hash = []
    for ln in actual:
        _, _, hash_and_fn = ln.split(',', 2)
        hash, fn = hash_and_fn.split(" ")
        actual_rel_fn_hash.append((fn.strip(), hash.strip()))

    actual_rel_fn_hash.sort(key=lambda x: x[0])
    expected_rel_fn_hash = [(abspath.relative_to(base_path).as_posix(), hash)
                            for abspath, hash in expected_abspath_hash]
    expected_rel_fn_hash.sort(key=lambda x: x[0])

    assert expected_rel_fn_hash == actual_rel_fn_hash


@pytest.fixture
def setup(tmp_path: pathlib.Path):
    src1_dir = tmp_path / 'src1'
    src1_dir.mkdir()
    src1_file_hashes = create_source_dir(src1_dir)
    src2_dir = tmp_path / 'sub' / 'src2'
    src2_dir.mkdir(parents=True)
    src2_file_hashes = create_source_dir(src2_dir, hash_type='sha512')

    return (
        tmp_path,
        src1_dir, src1_file_hashes,
        src2_dir, src2_file_hashes)


def test_stage_add_targets(setup):
    (base_path,
     src1_dir, src1_file_hashes,
     src2_dir, src2_file_hashes) = setup

    src1_target1_dir = str(base_path / 'src1_target1')
    src1_target2_dir = str(base_path / 'src1_target2')
    src2_target1_dir = str(base_path / 'target_sub' / 'src2_target1')

    status_file = str(base_path / 'backup_status.json')
    print(status_file)
    cli.main(['stage', '--status-file', status_file,
             str(src1_dir), '--alias', 'src1_alias',
              '--hash-algorithm', 'md5'])
    cli.main(['stage', '--status-file', status_file,
              str(src2_dir), '--alias', 'src2_alias'])

    cli.main(['add-target', '--status-file', status_file,
              str(src1_dir), src1_target1_dir,
              '--alias', 'src1_target1_alias'])
    # use alias
    cli.main(['add-target', '--status-file', status_file,
              'src1_alias', src1_target2_dir,
              '--alias', 'src1_target2_alias',
              '--no-verify'])
    cli.main(['add-target', '--status-file', status_file,
              str(src2_dir), src2_target1_dir,
              '--alias', 'src2_target1_alias'])

    with open(status_file, 'r') as f:
        state = json.loads(f.read())

    assert state == {
        'sources': [{'alias': 'src1_alias',
                     'blocklist': [],
                     'force_single_hash': False,
                     'hash_algorithm': 'md5',
                     'hash_file': None,
                     'hash_log_file': None,
                     'path': str(src1_dir),
                     'targets': [{'alias': 'src1_target1_alias',
                                  'path': src1_target1_dir,
                                  'transfered': False,
                                  'type': 'Target',
                                  'verified': None,
                                  'verify': True,
                                  'version': 1},
                                 {'alias': 'src1_target2_alias',
                                  'path': src1_target2_dir,
                                  'transfered': False,
                                  'type': 'Target',
                                  'verified': None,
                                  'verify': False,
                                  'version': 1}],
                     'type': 'Source',
                     'version': 1},
                    {'alias': 'src2_alias',
                     'blocklist': [],
                     'force_single_hash': False,
                     'hash_algorithm': 'sha512',
                     'hash_file': None,
                     'hash_log_file': None,
                     'path': str(src2_dir),
                     'targets': [{'alias': 'src2_target1_alias',
                                  'path': src2_target1_dir,
                                  'transfered': False,
                                  'type': 'Target',
                                  'verified': None,
                                  'verify': True,
                                  'version': 1}],
                     'type': 'Source',
                     'version': 1}],
        'type': 'BackupHelper',
        'version': 1,
    }


def files_were_copied_and_hashes_match(
    src_dir: pathlib.Path,
    src_target_dir: pathlib.Path,
    src_file_hashes: List[Tuple[pathlib.Path, str]],
    hash_type: str,
):
    for abs_fn, hash in src_file_hashes:
        rel_fn = abs_fn.relative_to(src_dir)
        copied_file = src_target_dir / rel_fn
        assert copied_file.exists()
        assert gen_hash_from_file(
            str(copied_file), hash_type, _hex=True) == hash


def file_contents_match(a: str, b: str):
    with open(a, 'r') as f:
        a_contents = f.read()
    with open(b, 'r') as f:
        b_contents = f.read()
    assert a_contents == b_contents


def test_stage_hash_transfer_verify(setup):
    (base_path,
     src1_dir, src1_file_hashes,
     src2_dir, src2_file_hashes) = setup

    src1_target1_dir = str(base_path / 'src1_target1')
    src1_target2_dir = str(base_path / 'src1_target2')
    src2_target1_dir = str(base_path / 'target_sub' / 'src2_target1')

    status_file = str(base_path / 'backup_status.json')
    print(status_file)
    cli.main(['stage', '--status-file', status_file,
             str(src1_dir), '--alias', 'src1_alias',
              '--hash-algorithm', 'md5'])
    cli.main(['stage', '--status-file', status_file,
              str(src2_dir), '--alias', 'src2_alias'])

    cli.main(['add-target', '--status-file', status_file,
              str(src1_dir), src1_target1_dir,
              '--alias', 'src1_target1_alias'])
    # use alias
    cli.main(['add-target', '--status-file', status_file,
              'src1_alias', src1_target2_dir,
              '--alias', 'src1_target2_alias',
              '--no-verify'])
    cli.main(['add-target', '--status-file', status_file,
              str(src2_dir), src2_target1_dir,
              '--alias', 'src2_target1_alias'])

    cli.main(['start', '--status-file', status_file])

    bh = BackupHelper.load_state(status_file)

    assert bh._working_dir == str(base_path)
    assert (base_path / 'backup_helper.log').exists()

    sources = list(bh.unique_sources())

    # --- sources were hashed ---
    assert len(sources) == 2
    assert sources[0].hash_file
    assert sources[1].hash_file

    with open(sources[0].hash_file, 'r') as f:
        actual_src1_file_hashes = f.readlines()
    hash_file_match(src1_dir, actual_src1_file_hashes, src1_file_hashes)

    with open(sources[1].hash_file, 'r') as f:
        actual_src2_file_hashes = f.readlines()
    hash_file_match(src2_dir, actual_src2_file_hashes, src2_file_hashes)

    # log files written to same dir as backup_status.json
    assert os.path.isfile(sources[0].hash_log_file)
    assert os.path.isfile(sources[1].hash_log_file)
    assert pathlib.Path(sources[0].hash_log_file).parent == base_path
    assert pathlib.Path(sources[1].hash_log_file).parent == base_path
    files_root = os.listdir(base_path)
    assert any(f.startswith(helpers.sanitize_filename(str(src1_dir)))
               for f in files_root)
    assert any(f.startswith(helpers.sanitize_filename(str(src2_dir)))
               for f in files_root)

    # --- sources transfered to targets ---
    src1_target1 = sources[0].get_target('src1_target1_alias')
    src1_target2 = sources[0].get_target('src1_target2_alias')
    src2_target1 = sources[1].get_target('src2_target1_alias')
    assert src1_target1.transfered
    assert src1_target2.transfered
    assert src2_target1.transfered
    files_were_copied_and_hashes_match(
        src1_dir, src1_target1_dir, src1_file_hashes, 'md5')
    files_were_copied_and_hashes_match(
        src1_dir, src1_target2_dir, src1_file_hashes, 'md5')
    files_were_copied_and_hashes_match(
        src2_dir, src2_target1_dir, src2_file_hashes, 'sha512')
    # hash files also transfered
    file_contents_match(
        sources[0].hash_file,
        os.path.join(src1_target1_dir, os.path.basename(sources[0].hash_file)))
    file_contents_match(
        sources[0].hash_file,
        os.path.join(src1_target2_dir, os.path.basename(sources[0].hash_file)))
    file_contents_match(
        sources[1].hash_file,
        os.path.join(src2_target1_dir, os.path.basename(sources[1].hash_file)))

    # --- targets were verified ---
    assert src1_target1.verified is not None
    assert src1_target1.verified.checked == 4
    assert src1_target1.verified.missing == 0
    assert src1_target1.verified.crc_errors == 0
    assert os.path.isfile(src1_target1.verified.log_file)
    # verify was False
    assert src1_target2.verified is None
    assert src2_target1.verified is not None
    assert src2_target1.verified.checked == 4
    assert src2_target1.verified.missing == 0
    assert src2_target1.verified.crc_errors == 0
    assert os.path.isfile(src2_target1.verified.log_file)

    # --- backup_status.json matches ---
    with open(status_file, 'r') as f:
        state = json.loads(f.read())

    assert state == {
        'sources': [{'alias': 'src1_alias',
                     'blocklist': [],
                     'force_single_hash': False,
                     'hash_algorithm': 'md5',
                     'hash_file': sources[0].hash_file,
                     'hash_log_file': sources[0].hash_log_file,
                     'path': str(src1_dir),
                     'targets': [{'alias': 'src1_target1_alias',
                                  'path': src1_target1_dir,
                                  'transfered': True,
                                  'type': 'Target',
                                  'verified': {
                                      'checked': src1_target1.verified.checked,
                                      'errors': src1_target1.verified.errors,
                                      'missing': src1_target1.verified.missing,
                                      'crc_errors': src1_target1.verified.crc_errors,
                                      'log_file': src1_target1.verified.log_file,
                                  },
                                  'verify': True,
                                  'version': 1},
                                 {'alias': 'src1_target2_alias',
                                  'path': src1_target2_dir,
                                  'transfered': True,
                                  'type': 'Target',
                                  'verified': None,
                                  'verify': False,
                                  'version': 1}],
                     'type': 'Source',
                     'version': 1},
                    {'alias': 'src2_alias',
                     'blocklist': [],
                     'force_single_hash': False,
                     'hash_algorithm': 'sha512',
                     'hash_file': sources[1].hash_file,
                     'hash_log_file': sources[1].hash_log_file,
                     'path': str(src2_dir),
                     'targets': [{'alias': 'src2_target1_alias',
                                  'path': src2_target1_dir,
                                  'transfered': True,
                                  'type': 'Target',
                                  'verified': {
                                      'checked': src2_target1.verified.checked,
                                      'errors': src2_target1.verified.errors,
                                      'missing': src2_target1.verified.missing,
                                      'crc_errors': src2_target1.verified.crc_errors,
                                      'log_file': src2_target1.verified.log_file,
                                  },
                                  'verify': True,
                                  'version': 1}],
                     'type': 'Source',
                     'version': 1}],
        'type': 'BackupHelper',
        'version': 1,
    }
