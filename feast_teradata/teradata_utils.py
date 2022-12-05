from teradataml import (
    create_context,
    get_context
)
from feast.repo_config import FeastConfigBaseModel
from typing import Dict, Optional
from pydantic import StrictStr
from feast.value_type import ValueType

import pyarrow as pa


class TeradataConfig(FeastConfigBaseModel):
    host: StrictStr
    port: int = 1025
    database: StrictStr
    user: StrictStr
    password: StrictStr
    log_mech: Optional[StrictStr] = "LDAP"


def teradata_type_to_feast_value_type(data_type):
    type_map: Dict[str, ValueType] = {
        "<class 'int'>": pa.int64(),
        "<class 'float'>": pa.float64(),
        "<class 'datetime.datetime'>": pa.timestamp('ns')  # TODO: Add more mappings here
    }

    return type_map[str(data_type)]


def get_conn(config: TeradataConfig):
    if get_context() is None:
        create_context(host=config.host,
                       username=config.user,
                       password=config.password,
                       database=config.database,
                       logmech=config.log_mech)

    return get_context()
