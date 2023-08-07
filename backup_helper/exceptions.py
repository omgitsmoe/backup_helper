from typing import List, TypeVar


class BackupHelperException(Exception):
    def __init__(self, message: str):
        super().__init__(message)


class SourceAlreadyExists(BackupHelperException):
    def __init__(self, message: str, source: str):
        super().__init__(message)
        self.source = source


class TargetAlreadyExists(BackupHelperException):
    def __init__(self, message: str, source: str, target: str):
        super().__init__(message)
        self.source = source
        self.target = target


class AliasAlreadyExists(BackupHelperException):
    def __init__(self, message: str, name: str):
        super().__init__(message)
        self.name = name


class SourceNotFound(BackupHelperException):
    def __init__(self, message: str, source: str):
        super().__init__(message)
        self.source = source


class TargetNotFound(BackupHelperException):
    def __init__(self, message: str, source: str, target: str):
        super().__init__(message)
        self.source = source
        self.target = target


class HashError(BackupHelperException):
    def __init__(self, message: str):
        super().__init__(message)


T = TypeVar('T')


class QueueItemsWillNeverBeReady(BackupHelperException):
    def __init__(self, message: str, work_not_ready: List[T]):
        super().__init__(message)
        self.work_not_ready = work_not_ready
