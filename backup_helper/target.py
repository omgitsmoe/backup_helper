import os
import dataclasses

from typing import (
    Optional, List, Any, Iterator, Dict
)

from backup_helper import helpers


@ dataclasses.dataclass
class VerifiedInfo:
    errors: int
    missing: int
    crc_errors: int
    log_file: str


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
