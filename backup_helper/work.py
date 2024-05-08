import dataclasses
import enum
import logging
import os

from typing import Optional, TYPE_CHECKING, Final, List, Union, Tuple, cast

if TYPE_CHECKING:
    from backup_helper.source import Source
    from backup_helper.target import Target, VerifiedInfo

from backup_helper.disk_work_queue import DiskWorkQueue


logger = logging.getLogger(__name__)


@dataclasses.dataclass
class WorkHash:
    source: 'Source'
    log_dir: str

    # whether it's dependencies have completed
    def is_ready(self) -> bool:
        return True

    def get_involved_paths(self) -> List[str]:
        return [self.source.path]

    def do_work(self):
        self.source.hash(log_directory=self.log_dir)
        return self

    def report_success(self) -> str:
        return (f"Hashed '{self.source.path}':\n"
                f"  Hash file: {self.source.hash_file}")

    def report_error(self) -> str:
        return f"Error hashing '{self.source.path}'!"

    def __str__(self) -> str:
        return f"Hashing '{self.source.path}'"


@dataclasses.dataclass
class WorkTransfer:
    source: 'Source'
    target: 'Target'

    # whether it's dependencies have completed
    def is_ready(self) -> bool:
        # we don't need a hash file on source if the target should not be
        # verified
        return not self.target.verify or bool(self.source.hash_file)

    def get_involved_paths(self) -> List[str]:
        return [self.source.path, self.target.path]

    def do_work(self):
        self.source.transfer(self.target)
        return self

    def report_success(self) -> str:
        return (f"Transfer successful:\n"
                f"  From: {self.source.path}\n"
                f"  To: {self.target.path}")

    def report_error(self) -> str:
        return ("Error transfering:\n"
                f"  From: {self.source.path}\n"
                f"  To: {self.target.path}")

    def __str__(self) -> str:
        return f"Transferring\n\t'{self.source.path}'\n\t-> '{self.target.path}'"


@dataclasses.dataclass
class WorkVerifyTransfer:
    source: 'Source'
    target: 'Target'

    # whether it's dependencies have completed
    def is_ready(self) -> bool:
        # we don't need a hash file on source if the target should not be verified
        return self.target.transfered and (
            not self.target.verify or bool(self.source.hash_file))

    def get_involved_paths(self) -> List[str]:
        return [self.target.path]

    def do_work(self):
        self.target.verify_from(self.source.hash_file)
        return self

    def report_success(self) -> str:
        cast('VerifiedInfo', self.target.verified)
        return (f"Verified transfer '{self.target.path}':\n"
                f"  Checked: {self.target.verified.checked}\n"
                f"  CRC Errors: {self.target.verified.crc_errors}\n"
                f"  Missing: {self.target.verified.missing}")

    def report_error(self) -> str:
        return f"Error verifying '{self.target.path}'!"

    def __str__(self) -> str:
        if not self.source.hash_file:
            return f"Waiting for source hash file in {self.source.path}"

        hash_file = os.path.join(
            self.target.path, os.path.basename(self.source.hash_file))
        return f"Verifying '{hash_file}'"


WorkType = Union[WorkHash, WorkTransfer, WorkVerifyTransfer]
WorkResult = WorkType
WorkQueue = DiskWorkQueue[WorkType, WorkResult]


def get_involved_paths(work: WorkType) -> List[str]:
    return work.get_involved_paths()


def do_work(work: WorkType) -> WorkResult:
    return work.do_work()


def is_ready(work: WorkType) -> bool:
    return work.is_ready()


def setup_work_queue(work_items: List[WorkType]) -> WorkQueue:
    return WorkQueue(get_involved_paths, do_work, is_ready,
                     report_progress_timestep_seconds=60,
                     work=work_items)


def report_results(success: List[WorkType], errors: List[Tuple[WorkType, str]]):
    if success:
        logger.info(
            "Successfully completed the following %d operation(s):\n%s",
            len(success),
            "\n".join(w.report_success() for w in success))
    if errors:
        logger.warning(
            "Failed to complete the following %d operation(s):\n%s",
            len(errors), "\n".join(f"{w.report_error()}:\n Error: {err}"
                                   for w, err in errors))

    if not success and not errors:
        logger.info("No operations were run maybe you forgot to run a "
                    "previous step, e.g. sources cannot be transfered until "
                    "they were hashed!")
