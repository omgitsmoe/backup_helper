import os
import dataclasses
import logging
import time

from typing import (
    Optional, List, Any, Iterator, Dict
)

import checksum_helper.checksum_helper as checksum_helper

from backup_helper import helpers


logger = logging.getLogger(__name__)


@ dataclasses.dataclass
class VerifiedInfo:
    checked: int
    errors: int
    missing: int
    crc_errors: int
    log_file: str


@dataclasses.dataclass
class Target:
    path: str
    alias: Optional[str]
    transfered: bool
    verify: bool
    verified: Optional[VerifiedInfo]

    def __init__(self, path: str, alias: Optional[str], transfered: bool,
                 verify: bool, verified: Optional[VerifiedInfo]):
        self.path = os.path.normpath(os.path.abspath(path))
        self.alias = alias
        self.transfered = transfered
        self.verify = verify
        self.verified = verified

    def to_json(self) -> Dict[Any, Any]:
        result = {"version": 1, "type": type(self).__name__}
        for k, v in self.__dict__.items():
            if k in result:
                raise RuntimeError("Duplicate field key")
            elif k == "verified":
                continue
            result[k] = v

        result["verified"] = self.verified.__dict__ if self.verified else None

        return result

    @ staticmethod
    def from_json(json_object: Dict[Any, Any]) -> 'Target':
        if json_object["verified"]:
            verified = VerifiedInfo(**json_object["verified"])
        else:
            verified = None
        return Target(
            json_object["path"],
            json_object["alias"],
            json_object["transfered"],
            json_object["verify"],
            verified,
        )

    def verify_from(
        self,
        hash_file_name: str,
        log_directory: Optional[str] = None,
        force: bool = False
    ) -> Optional[VerifiedInfo]:
        if not self.verify and not force:
            logger.info(
                "Skipping verification of %s, since `verify` is off!",
                self.path)
            return None
        if not self.transfered:
            logger.info(
                "Target %s has not been transfered yet! Nothing to verify!",
                self.path)
            return None

        if log_directory is None:
            log_directory = self.path
        log_path = os.path.join(
            log_directory,
            f"{helpers.sanitize_filename(hash_file_name)}_vf_"
            f"{time.strftime('%Y-%m-%dT%H-%M-%S')}.log")

        with helpers.setup_thread_log_file_autoremove(
                checksum_helper.logger, log_path):
            cshd = checksum_helper.ChecksumHelperData(
                None, os.path.join(self.path, hash_file_name))
            cshd.read()
            crc_errors, missing, matches = cshd.verify()
            checksum_helper.log_summary(
                len(cshd.entries),
                [(cshd.root_dir, missing)],
                [(cshd.root_dir, crc_errors)])

            self.verified = VerifiedInfo(
                len(cshd.entries), len(crc_errors) + len(missing),
                len(missing), len(crc_errors), log_path)

            return self.verified

    def modifiable_fields(self) -> str:
        return helpers.format_dataclass_fields(self, lambda f: f.name != 'verified')

    def set_modifiable_field(self, field_name: str, value_str: str):
        if field_name == "path":
            self.path = value_str
        elif field_name == "alias":
            self.alias = value_str
        elif field_name == "transfered":
            self.transfered = helpers.bool_from_str(value_str)
        elif field_name == "verify":
            self.verify = helpers.bool_from_str(value_str)
        else:
            raise ValueError(f"Unkown field '{field_name}'!")

    def set_modifiable_field_multivalue(self, field_name: str, values: List[str]):
        raise ValueError(
            f"Cannot set multiple values for field '{field_name}'!")
