from typing import Dict
from dataclasses import dataclass, field
from material.config import Config
from material.l2.utils.sql import sql_get_c_table, sql_get_h_table, sql_get_f_table

config = Config()

@dataclass(frozen=True)
class ArrayLogicArgs:
    start_time: str
    end_time: str
    fab_code_dict: dict
    fab: str
    fab_code: int
    process_stage: str
    h_table: Dict[str, str] = field(init=False)

    def __post_init__(self):
        object.__setattr__(self, "h_table", sql_get_h_table(
            fab=self.fab, process_stage=self.process_stage.lower()))
        
@dataclass(frozen=True)
class CellLogicArgs:
    start_time: str
    end_time: str
    fab_code_dict: dict
    fab: str
    fab_code: int
    process_stage: str
    h_table: Dict[str, str] = field(init=False)

    def __post_init__(self):
        object.__setattr__(self, "h_table", sql_get_h_table(
            fab=self.fab, process_stage=self.process_stage.lower()))


@dataclass(frozen=True)
class ModuleLogicArgs:
    start_time: str
    end_time: str
    fab: str
    fab_code: int
    process_stage: str
    c_table: Dict[str, str] = field(init=False)
    h_table: Dict[str, str] = field(init=False)
    f_table: Dict[str, str] = field(init=False)

    def __post_init__(self):
        object.__setattr__(self, "c_table", sql_get_c_table(
            fab=self.fab, process_stage=self.process_stage.lower()))
        object.__setattr__(self, "h_table", sql_get_h_table(
            fab=self.fab, process_stage=self.process_stage.lower()))
        object.__setattr__(self, "f_table", sql_get_f_table(
            fab=self.fab, process_stage=self.process_stage, test_schema=config.delta_schema))
        

@dataclass(frozen=True)
class BrisaLogicArgs:
    start_time: str
    end_time: str
    fab: str
    fab_code: int
    process_stage: str
    c_table: Dict[str, str] = field(init=False)
    h_table: Dict[str, str] = field(init=False)
    f_table: Dict[str, str] = field(init=False)

    def __post_init__(self):
        object.__setattr__(self, "c_table", sql_get_c_table(
            fab=self.fab, process_stage=self.process_stage.lower()))
        object.__setattr__(self, "h_table", sql_get_h_table(
            fab=self.fab, process_stage=self.process_stage.lower()))
        object.__setattr__(self, "f_table", sql_get_f_table(
            fab=self.fab, process_stage=self.process_stage, test_schema=config.delta_schema))
        

@dataclass(frozen=True)
class EdiLogicArgs:
    start_time: str
    end_time: str
    fab: str
    fab_code: int
    process_stage: str
    c_table: Dict[str, str] = field(init=False)
    h_table: Dict[str, str] = field(init=False)
    f_table: Dict[str, str] = field(init=False)

    def __post_init__(self):
        object.__setattr__(self, "c_table", sql_get_c_table(
            fab=self.fab, process_stage=self.process_stage.lower()))
        object.__setattr__(self, "h_table", sql_get_h_table(
            fab=self.fab, process_stage=self.process_stage.lower()))
        object.__setattr__(self, "f_table", sql_get_f_table(
            fab=self.fab, process_stage=self.process_stage, test_schema=config.delta_schema))
        
@dataclass(frozen=True)
class LCMBEOLLogicArgs:
    start_time: str
    end_time: str
    fab: str
    fab_code: int
    process_stage: str
    c_table: Dict[str, str] = field(init=False)
    h_table: Dict[str, str] = field(init=False)
    f_table: Dict[str, str] = field(init=False)

    def __post_init__(self):
        object.__setattr__(self, "c_table", sql_get_c_table(
            fab=self.fab, process_stage=self.process_stage.lower()))
        object.__setattr__(self, "h_table", sql_get_h_table(
            fab=self.fab, process_stage=self.process_stage.lower()))
        object.__setattr__(self, "f_table", sql_get_f_table(
            fab=self.fab, process_stage=self.process_stage, test_schema=config.delta_schema))