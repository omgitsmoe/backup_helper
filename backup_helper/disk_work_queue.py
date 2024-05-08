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

from backup_helper.exceptions import QueueItemsWillNeverBeReady


logger = logging.getLogger(__name__)


# NOTE: don't cache the result, since if the path doesn't exist a parent
#       path's device id will be used
# NOTE: might fail if symlink points to an invalid path on windows
# NOTE: if we have a symlink and we go beyond it, because that device
# is not connected at the moment, it will still be fine, since
# the work item will not be able to run or fail to run immediately.
# So it will not end up erroneously blocking a devcie that
# is not involved in the  operation.
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
    started: bool = False


@dataclasses.dataclass
class WrappedResult(Generic[WorkType, ResultType]):
    work: WrappedWork[WorkType]
    result: Optional[ResultType]
    error: Optional[str]


class DiskWorkQueue(Generic[WorkType, ResultType]):
    """Not thread-safe!
    NOTE: Please do not use a `work_ready_func` that uses timers, since the
    queue currently assumes that if no jobs are running and no items
    could be started anymore, those left-over items will never be ready.
    """

    def __init__(
            self,
            get_involved_paths: Callable[[WorkType], Iterable[str]],
            worker_func: Callable[[WorkType], ResultType],
            work_ready_func: Callable[[WorkType], bool],
            report_progress_timestep_seconds=0,
            work: Optional[List[WorkType]] = None):
        """
        :params work: Optional list of work items to initialize the queue with
        :params report_progress_timestep_seconds:
            Time frame in seconds, where each work item in progress will
            be printed. Values <= 0 will result in no progress reports!
        """
        self._path_getter = get_involved_paths
        self._worker_func = self._wrap_worker(worker_func)
        self._work_ready_func = work_ready_func
        # maps os.stat().st_dev to whether they're currently in use by this
        # BackupHelper instance
        self._busy_devices: Dict[int, bool] = {}
        self._work: List[WrappedWork[WorkType]] = []
        self._running: int = 0
        # worker threads put the ResultType
        self._thread_done: queue.Queue[
            WrappedResult[WorkType, ResultType]] = queue.Queue()
        self._finished: List[WrappedResult[WorkType, ResultType]] = []
        self._report_progress_timestep_seconds = report_progress_timestep_seconds
        self._last_report: float = 0

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
        """NOTE: the device ids should not be safed in case a parent dir
        of an involved path is a symlink and the actual target like
        a mounted device does not exist yet"""
        paths = self._path_getter(work)
        device_ids = [get_device_identifier(p) for p in paths]
        return device_ids

    def _can_start(self, work: WorkType) -> Tuple[bool, Optional[Iterable[int]]]:
        if not self._work_ready_func(work):
            return False, None

        try:
            device_ids = self._get_involved_devices(work)
            if self._any_device_busy(self._busy_devices, device_ids):
                return False, None
        except RuntimeError:
            return False, None
        else:
            return True, device_ids

    def add_work(self, work: Iterable[WorkType]):
        for w in work:
            self._work.append(WrappedWork(w))

    def _work_done(self, result: WrappedResult[WorkType, ResultType]):
        self._finished.append(result)
        # update devices
        for dev in self._get_involved_devices(result.work.work):
            self._busy_devices[dev] = False

        self._running -= 1

    def _update_finished_threads(self) -> None:
        """Does nothing when queue is empty"""

        while not self._thread_done.empty():
            wrapped_result = self._thread_done.get_nowait()
            self._work_done(wrapped_result)
            self._thread_done.task_done()

    def _report_progress(self) -> None:
        if self._report_progress_timestep_seconds <= 0:
            return

        now = time.time()
        if now - self._last_report < self._report_progress_timestep_seconds:
            return
        self._last_report = now

        for work in self._work:
            if work.started:
                print("Active job:", work.work)

    def _wait_till_one_thread_finished_and_update(self):
        """
        Blocks until at least one item is retrieved from the Queue.
        Then updates it.
        """
        # NOTE: thread.join, q.get etc. are not interruptable by SIGINT
        # need to use own implementation that uses the original with a timeout
        # and a sleep (the latter is interruptable)
        while True:
            self._report_progress()
            try:
                wrapped_result = self._thread_done.get(timeout=0.1)
            except queue.Empty:
                time.sleep(0.2)
            else:
                break
        self._work_done(wrapped_result)
        self._thread_done.task_done()

    def start_ready_devices(self):
        """
        Starts a Thread on all work items if all involved devices are
        currently not in use by this DiskWorkQueue
        """
        # first update the busy devices if there are finished threads
        self._update_finished_threads()

        started = False
        for work in self._work:
            if work.started:
                continue

            can_start, devices = self._can_start(work.work)
            if can_start:
                started = True
                for dev in devices:
                    self._busy_devices[dev] = True

                t = threading.Thread(target=self._worker_func, args=[work])
                t.start()
                self._running += 1
                work.started = True

        # we might've items that will never finish since they're
        # _work_ready_func will never return True
        # this condition is probably met if no work was started and we
        # don't have any running threads
        if not started and not self.workers_running() and len(self._finished) < len(self._work):
            raise QueueItemsWillNeverBeReady(
                "The queue items left will never be ready, since no more jobs "
                "are running and no jobs could be started!\n",
                [w for w in self._work if w.started is False])

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

    def workers_running(self) -> bool:
        return self._running > 0

    def join(self) -> None:
        """Wait till all workers are done! Can be interrupted by KeyboardInterrupt"""
        while self.workers_running():
            self._wait_till_one_thread_finished_and_update()

    # TODO: run all transfers on a device before any verify
    def start_and_join_all(self) -> Tuple[List[ResultType], List[Tuple[WorkType, str]]]:
        """
        Wait till all work items are finished
        Can be interrupted by KeyboardInterrupt
        :returns: Successful items, Error strings of failed items/worker_func
        """
        try:
            self.start_ready_devices()
            while len(self._finished) < len(self._work):
                # since start_ready_devices can update self._finished this
                # needs to happen first
                self._wait_till_one_thread_finished_and_update()
                self.start_ready_devices()
        except QueueItemsWillNeverBeReady:
            logger.warning(
                "Not all work items could be finished, since they were not "
                "ready while no jobs were running or could be started anymore!")
        except KeyboardInterrupt:
            print("Continue after Ctrl+C... Currently running items will be "
                  "finished and can keep the program running. Don't force "
                  "close it!")

        return self.get_finished_items()
