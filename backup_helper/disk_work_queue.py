import os
import threading
import queue
import dataclasses
import logging
import logging.config
import time


from typing import (
    TypeVar, Generic, Sequence, Optional, Callable, Tuple, Iterable,
    List, Dict, cast
)


logger = logging.getLogger(__name__)


def get_device_identifier(path: str) -> int:
    # st_dev
    # Identifier of the device on which this file resides.
    curpath = path
    while curpath:
        try:
            stat = os.stat(curpath)
            return stat.st_dev
        except FileNotFoundError:
            curpath = os.path.dirname(os.path.realpath(curpath))

    raise RuntimeError(f"Could not determine device of path {path}")


WorkType = TypeVar('WorkType')
ResultType = TypeVar('ResultType')


@dataclasses.dataclass
class WrappedWork(Generic[WorkType]):
    work: WorkType
    involved_devices: List[int]
    started: bool = False


@dataclasses.dataclass
class WrappedResult(Generic[WorkType, ResultType]):
    work: WrappedWork[WorkType]
    result: Optional[ResultType]
    error: Optional[str]


class DiskWorkQueue(Generic[WorkType, ResultType]):
    """Not thread-safe!"""

    def __init__(
            self,
            get_involved_paths: Callable[[WorkType], Iterable[str]],
            worker_func: Callable[[WorkType], ResultType],
            work: Optional[List[WorkType]] = None):
        """
        :params work: WorkType
        """
        self._path_getter = get_involved_paths
        self._worker_func = self._wrap_worker(worker_func)
        # maps os.stat().st_dev to whether they're currently in use by this
        # BackupHelper instance
        self._busy_devices: Dict[int, bool] = {}
        self._work: List[WrappedWork[WorkType]] = []
        self._running: int = 0
        # worker threads put the ResultType
        self._thread_done: queue.Queue[
            WrappedResult[WorkType, ResultType]] = queue.Queue()
        self._finished: List[WrappedResult[WorkType, ResultType]] = []

        if work:
            self.add_work(work)

    @staticmethod
    def _any_device_busy(
            busy_devices: Dict[int, bool], device_ids: Iterable[int]) -> bool:
        """
        :param deviceIds: Iterable of deviceIds as returned by os.stat().st_dev
        :returns: Whether any device is currently busy (in the context of
                  this BackupHelper instance)
        """
        for device_id in device_ids:
            try:
                busy = busy_devices[device_id]
            except KeyError:
                busy = False

            if busy:
                return True

        return False

    def _wrap_worker(
        self,
        worker_func: Callable[[WorkType], ResultType]
    ) -> Callable[[WrappedWork[WorkType]], None]:
        def wrapped(work: WrappedWork[WorkType]) -> None:
            logger.debug('Starting work: %s', work.work)
            try:
                result = worker_func(work.work)
            except Exception as e:
                logger.warning('Failed work: %s: %s', work.work, str(e))
                self._thread_done.put(WrappedResult(work, None, str(e)))
            else:
                logger.debug('Successfully completed work: %s', work.work)
                self._thread_done.put(WrappedResult(work, result, None))

        return wrapped

    def _get_involved_devices(self, work: WorkType) -> List[int]:
        paths = self._path_getter(work)
        device_ids = [get_device_identifier(p) for p in paths]
        return device_ids

    def _can_start(self, work: WorkType) -> Tuple[bool, Optional[Iterable[int]]]:
        device_ids = self._get_involved_devices(work)
        if self._any_device_busy(self._busy_devices, device_ids):
            return False, None

        return True, device_ids

    def add_work(self, work: Iterable[WorkType]):
        for w in work:
            device_ids = self._get_involved_devices(w)
            self._work.append(WrappedWork(w, device_ids))

    def _work_done(self, result: WrappedResult[WorkType, ResultType]):
        self._finished.append(result)
        # update devices
        for dev in result.work.involved_devices:
            self._busy_devices[dev] = False

        self._running -= 1

    def _update_finished_threads(self):
        """Does nothing when queue is empty"""

        while not self._thread_done.empty():
            wrapped_result = self._thread_done.get_nowait()
            self._work_done(wrapped_result)
            self._thread_done.task_done()

    def _wait_till_one_thread_finished_and_update(self):
        """
        Blocks until at leas one item is retrieved from the Queue.
        Then updates it.
        """
        wrapped_result = self._thread_done.get()
        self._work_done(wrapped_result)
        self._thread_done.task_done()

    def start_ready_devices(self):
        """
        Starts a Thread on all work items if all involved devices are
        currently not in used by this DiskWorkQueue
        """
        # first update the busy devices if there are finished threads
        self._update_finished_threads()

        for work in self._work:
            if work.started:
                continue

            can_start, devices = self._can_start(work.work)
            if can_start:
                for dev in devices:
                    self._busy_devices[dev] = True

                t = threading.Thread(target=self._worker_func, args=[work])
                t.start()
                self._running += 1
                work.started = True

    def get_finished_items(self) -> Tuple[List[ResultType], List[Tuple[WorkType, str]]]:
        self._update_finished_threads()

        success: List[ResultType] = []
        errors: List[Tuple[WorkType, str]] = []
        for wrapped_result in self._finished:
            if wrapped_result.error is not None:
                cast(str, wrapped_result.error)
                errors.append((wrapped_result.work.work, wrapped_result.error))
            else:
                if wrapped_result.result is None:
                    raise RuntimeError(
                        "Fatal error: No error, but result is missing for "
                        f"work {wrapped_result.work}")
                else:
                    success.append(wrapped_result.result)

        return success, errors

    def join(self) -> None:
        while self._running > 0:
            self._wait_till_one_thread_finished_and_update()

    def start_and_join_all(self) -> Tuple[List[ResultType], List[Tuple[WorkType, str]]]:
        """
        Wait till all work items are finished
        :returns: Successful items, Error strings of failed items/worker_func
        """
        self.start_ready_devices()
        while len(self._finished) < len(self._work):
            # since start_ready_devices can update self._finished this
            # needs to happen first
            self._wait_till_one_thread_finished_and_update()
            self.start_ready_devices()

        return self.get_finished_items()
