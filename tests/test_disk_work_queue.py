import pytest
import os

from typing import List, Tuple, Dict

from backup_helper import disk_work_queue as dwq
from backup_helper.exceptions import QueueItemsWillNeverBeReady


@pytest.mark.parametrize('test_devices,busy_map,expected', [
    ([13], {}, False),
    ([13], {13: False}, False),
    ([13], {13: True}, True),
    ([13, 3, 9], {}, False),
    ([13, 3, 9], {13: False, 3: False}, False),
    ([13, 3, 9], {13: True}, True),
    ([13, 3, 9], {13: False, 3: False, 9: True}, True),
])
def test_any_device_busy(test_devices, busy_map, expected):
    assert dwq.DiskWorkQueue._any_device_busy(
        busy_map, test_devices) is expected


def test_add_work(monkeypatch) -> None:
    def get_devid(path: str) -> int:
        return {
            'path1': 0,
            'path2': 1,
            'path3': 3,
            'path4': 0,
        }[path]

    monkeypatch.setattr(dwq, 'get_device_identifier', get_devid)

    def get_paths(x: str) -> List[str]:
        return {
            'work0': ['path1'],
            'work1': ['path2', 'path1'],
            'work2': ['path3', 'path4'],
            'work3': ['path4', 'path1', 'path3'],
        }[x]

    q: dwq.DiskWorkQueue[str, str] = dwq.DiskWorkQueue(
        get_paths, lambda x: x, lambda x: True)
    q.add_work(['work0', 'work1', 'work2', 'work3'])

    assert q._work[0].work == 'work0'
    assert q._work[0].involved_devices == [0]
    assert q._work[0].started is False

    assert q._work[1].work == 'work1'
    assert q._work[1].involved_devices == [1, 0]
    assert q._work[1].started is False

    assert q._work[2].work == 'work2'
    assert q._work[2].involved_devices == [3, 0]
    assert q._work[2].started is False

    assert q._work[3].work == 'work3'
    assert q._work[3].involved_devices == [0, 0, 3]
    assert q._work[3].started is False


@pytest.fixture
def setup_disk_work_queue_start_ready(monkeypatch) -> Tuple[
        dwq.DiskWorkQueue[str, str], Dict[str, bool]]:
    # order in terms of affected devices:
    # start work0, work1
    # start work2
    # start work3, work4

    def get_devid(path: str) -> int:
        return {
            'path1': 0,
            'path2': 1,
            'path3': 3,
            'path4': 0,
        }[path]

    monkeypatch.setattr(dwq, 'get_device_identifier', get_devid)

    def get_paths(x: str) -> List[str]:
        return {
            'work0': ['path1'],
            'work1': ['path2', 'path3'],
            'work2': ['path1', 'path4', 'path2'],
            'work3': ['path4', 'path1', 'path3'],
            'work4': ['path2'],
        }[x]

    started: Dict[str, bool] = {}

    def do_work(x: str):
        nonlocal started
        started[x] = True
        if x == 'work4':
            raise RuntimeError("Error text")

        return x

    ready_map = {
        'work0': True,
        'work1': True,
        'work2': True,
        'work3': True,
        'work4': True,
    }

    def is_ready(x: str) -> bool:
        return ready_map[x]

    q: dwq.DiskWorkQueue[str, str] = dwq.DiskWorkQueue(
        get_paths, do_work, is_ready)
    q.add_work(['work0', 'work1', 'work2', 'work3', 'work4'])

    return q, started, ready_map


def test_start_ready_devices(setup_disk_work_queue_start_ready) -> None:
    q, started, _ = setup_disk_work_queue_start_ready

    started.clear()
    q.start_ready_devices()
    q.join()
    assert started == {'work0': True, 'work1': True}
    success, errors = q.get_finished_items()
    assert success == ['work0', 'work1']
    assert errors == []

    started.clear()
    q.start_ready_devices()
    q.join()
    assert started == {'work2': True}
    success, errors = q.get_finished_items()
    assert success == ['work0', 'work1', 'work2']
    assert errors == []

    started.clear()
    q.start_ready_devices()
    q.join()
    assert started == {'work3': True, 'work4': True}
    success, errors = q.get_finished_items()
    assert success == ['work0', 'work1', 'work2', 'work3']
    assert errors == [('work4', 'Error text')]


def test_start_ready_devices_uses_work_ready_func(setup_disk_work_queue_start_ready) -> None:
    q, started, _ = setup_disk_work_queue_start_ready
    q._work_ready_func = lambda x: x != 'work0'

    started.clear()
    q.start_ready_devices()
    q.join()
    assert started == {'work1': True}
    success, errors = q.get_finished_items()
    assert success == ['work1']
    assert errors == []

    q._work_ready_func = lambda x: x != 'work2'
    started.clear()
    q.start_ready_devices()
    q.join()
    assert started == {'work0': True, 'work4': True}
    success, errors = q.get_finished_items()
    assert success == ['work1', 'work0']
    assert errors == [('work4', 'Error text')]

    q._work_ready_func = lambda x: True
    started.clear()
    q.start_ready_devices()
    q.join()
    assert started == {'work2': True}
    success, errors = q.get_finished_items()
    assert success == ['work1', 'work0', 'work2']
    assert errors == [('work4', 'Error text')]


def test_get_finished_items():
    q = dwq.DiskWorkQueue(lambda x: x, lambda x: x, lambda x: True)
    # to make sure get_finished_items also includes finished threads that
    # were not yet put into q._finished
    q._thread_done.put(
        dwq.WrappedResult(dwq.WrappedWork('work2', []), 'work2', None),
    )
    q._finished.extend([
        dwq.WrappedResult(dwq.WrappedWork('work0', []), 'work0', None),
        dwq.WrappedResult(dwq.WrappedWork('work1', []), 'work1', None),
        dwq.WrappedResult(dwq.WrappedWork('work3', []), None, 'work3 Error'),
    ])

    assert q.get_finished_items() == (
        ['work0', 'work1', 'work2'],
        [('work3', 'work3 Error')],
    )


def test_get_finished_items_missing_result():
    q = dwq.DiskWorkQueue(lambda x: x, lambda x: x, lambda x: True)
    q._finished.extend([
        dwq.WrappedResult(dwq.WrappedWork('work0', []), 'work0', None),
        dwq.WrappedResult(dwq.WrappedWork('work3', []), None, None),
    ])

    with pytest.raises(RuntimeError):
        q.get_finished_items()


def test_join(setup_disk_work_queue_start_ready):
    q, started, _ = setup_disk_work_queue_start_ready
    q.start_ready_devices()
    q.join()
    assert all(not x for x in q._busy_devices.values())
    assert q._running == 0
    assert q.get_finished_items() == (
        ['work0', 'work1'],
        [],
    )


def test_start_and_join_all(setup_disk_work_queue_start_ready):
    q, started, _ = setup_disk_work_queue_start_ready
    success, errors = q.start_and_join_all()
    assert started == {'work0': True, 'work1': True, 'work2': True,
                       'work3': True, 'work4': True}
    assert success == ['work0', 'work1', 'work2', 'work3']
    assert errors == [('work4', 'Error text')]
    assert q._running == 0
    assert len(q._finished) == len(q._work)
    assert all(not x for x in q._busy_devices.values())


def test_get_device_identifier_uses_pardir_if_nonexistant():
    # if a path hasn't been created yet get_device_identifier
    # should walk up till it finds an existing dir
    path = os.path.join(os.path.abspath('.'), 'sfdkls', 'tsrfsdfsdfshfhfdg')
    expected = os.stat(os.path.abspath('.')).st_dev
    assert dwq.get_device_identifier(path) == expected


def test_queue_will_not_block_if_item_never_ready(setup_disk_work_queue_start_ready):
    q, started, ready_map = setup_disk_work_queue_start_ready
    ready_map['work2'] = False

    with pytest.raises(QueueItemsWillNeverBeReady) as e:
        while True:
            q.start_ready_devices()
    assert e.value.work_not_ready == [dwq.WrappedWork('work2', [0,0,1], False)]
    # work2 is left in q, since it might be ready on next start
    success, errors = q.get_finished_items()
    assert started == {'work0': True, 'work1': True,
                       'work3': True, 'work4': True}
    assert success == ['work0', 'work1', 'work3']
    assert errors == [('work4', 'Error text')]
    assert q._running == 0
    assert len(q._finished) == len(q._work) - 1
    assert all(not x for x in q._busy_devices.values())
