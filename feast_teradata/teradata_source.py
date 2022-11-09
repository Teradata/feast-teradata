import json
from typing import Callable, Dict, Iterable, Optional, Tuple
from pydantic import StrictStr
from typeguard import typechecked
import pyarrow as pa

from feast.data_source import DataSource
from feast.errors import DataSourceNoNameException
from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto
from feast.protos.feast.core.SavedDataset_pb2 import (
    SavedDatasetStorage as SavedDatasetStorageProto,
)
from feast.repo_config import RepoConfig
from feast.repo_config import FeastConfigBaseModel
from feast.saved_dataset import SavedDatasetStorage
from feast.value_type import ValueType
from teradataml import create_context
import pandas as pd
import numpy as np
from teradataml import (
create_context,
get_context,
get_connection,
copy_to_sql,
BIGINT, TIMESTAMP,
fastload
)

@typechecked
class TeradataSource(DataSource):
    def __init__(
        self,
        name: Optional[str] = None,
        query: Optional[str] = None,
        table: Optional[str] = None,
        timestamp_field: Optional[str] = "",
        created_timestamp_column: Optional[str] = "",
        field_mapping: Optional[Dict[str, str]] = None,
        description: Optional[str] = "",
        tags: Optional[Dict[str, str]] = None,
        owner: Optional[str] = "",
    ):
        self._teradata_options = TeradataOptions(name=name, query=query, table=table)

        # If no name, use the table as the default name.
        if name is None and table is None:
            raise DataSourceNoNameException()
        name = name or table
        assert name

        super().__init__(
            name=name,
            timestamp_field=timestamp_field,
            created_timestamp_column=created_timestamp_column,
            field_mapping=field_mapping,
            description=description,
            tags=tags,
            owner=owner,
        )

    def __hash__(self):
        return super().__hash__()

    def __eq__(self, other):
        if not isinstance(other, TeradataSource):
            raise TypeError(
                "Comparisons should only involve TeradataSource class objects."
            )

        return (
                super().__eq__(other)
                and self._teradata_options._query == other._teradata_options._query
                and self.timestamp_field == other.timestamp_field
                and self.created_timestamp_column == other.created_timestamp_column
                and self.field_mapping == other.field_mapping
        )

    @staticmethod
    def from_proto(data_source: DataSourceProto):
        assert data_source.HasField("custom_options")

        teradata_options = json.loads(data_source.custom_options.configuration)

        return TeradataSource(
            name=teradata_options["name"],
            query=teradata_options["query"],
            table=teradata_options["table"],
            field_mapping=dict(data_source.field_mapping),
            timestamp_field=data_source.timestamp_field,
            created_timestamp_column=data_source.created_timestamp_column,
            description=data_source.description,
            tags=dict(data_source.tags),
            owner=data_source.owner,
        )

    def to_proto(self) -> DataSourceProto:
        data_source_proto = DataSourceProto(
            name=self.name,
            type=DataSourceProto.CUSTOM_SOURCE,
            data_source_class_type="feast-teradata.teradata_source.TeradataSource",
            field_mapping=self.field_mapping,
            custom_options=self._teradata_options.to_proto(),
            description=self.description,
            tags=self.tags,
            owner=self.owner,
        )

        data_source_proto.timestamp_field = self.timestamp_field
        data_source_proto.created_timestamp_column = self.created_timestamp_column

        return data_source_proto

    def validate(self, config: RepoConfig):
        pass

    @staticmethod
    def source_datatype_to_feast_value_type() -> Callable[[str], ValueType]:
        type_map = {
            "BINARY": ValueType.BYTES,
            "VARCHAR": ValueType.STRING,
            "NUMBER32": ValueType.INT32,
            "NUMBER64": ValueType.INT64,
            "NUMBERwSCALE": ValueType.DOUBLE,
            "DOUBLE": ValueType.DOUBLE,
            "BOOLEAN": ValueType.BOOL,
            "DATE": ValueType.UNIX_TIMESTAMP,
            "TIMESTAMP": ValueType.UNIX_TIMESTAMP,
            "TIMESTAMP_TZ": ValueType.UNIX_TIMESTAMP,
            "TIMESTAMP_LTZ": ValueType.UNIX_TIMESTAMP,
            "TIMESTAMP_NTZ": ValueType.UNIX_TIMESTAMP,
        }
        return type_map

    def get_table_column_names_and_types(
        self, config: RepoConfig
    ) -> Iterable[Tuple[str, str]]:

        # use the types as defined in feast.type_map
        from feast.type_map import pg_type_code_to_pg_type, pg_type_to_feast_value_type
        return [("col1", "NUMBER32"),
                ("col2", "DOUBLE"),
                ("col3", "DOUBLE")]

    def get_table_query_string(self) -> str:
        if self._teradata_options._table:
            return f"{self._teradata_options._table}"
        else:
            return f"({self._teradata_options._query})"


class TeradataOptions:
    def __init__(
        self,
        name: Optional[str],
        query: Optional[str],
        table: Optional[str],
    ):
        self._name = name or ""
        self._query = query or ""
        self._table = table or ""

    @classmethod
    def from_proto(cls, teradata_options_proto: DataSourceProto.CustomSourceOptions):
        config = json.loads(teradata_options_proto.configuration.decode("utf8"))
        teradata_options = cls(
            name=config["name"], query=config["query"], table=config["table"]
        )

        return teradata_options

    def to_proto(self) -> DataSourceProto.CustomSourceOptions:
        teradata_options_proto = DataSourceProto.CustomSourceOptions(
            configuration=json.dumps(
                {"name": self._name, "query": self._query, "table": self._table}
            ).encode()
        )
        return teradata_options_proto


class SavedDatasetTeradataStorage(SavedDatasetStorage):
    _proto_attr_name = "custom_storage"

    teradata_options: TeradataOptions

    def __init__(self, table_ref: str):
        self.teradata_options = TeradataOptions(
            table=table_ref, name=None, query=None
        )

    @staticmethod
    def from_proto(storage_proto: SavedDatasetStorageProto) -> SavedDatasetStorage:
        return SavedDatasetTeradataStorage(
            table_ref=TeradataOptions.from_proto(storage_proto.custom_storage)._table
        )

    def to_proto(self) -> SavedDatasetStorageProto:
        return SavedDatasetStorageProto(custom_storage=self.teradata_options.to_proto())

    def to_data_source(self) -> DataSource:
        return TeradataSource(table=self.teradata_options._table)


class TeradataConfig(FeastConfigBaseModel):
    host: StrictStr
    port: int = 1025
    database: StrictStr
    user: StrictStr
    password: StrictStr
    log_mech: Optional[StrictStr] = "LDAP"


def _get_conn(config: TeradataConfig):
    if get_context() is None:
        create_context(host=config.host, username=config.user, password=config.password, database=config.user,
                       logmech=config.log_mech)
    return get_context()


def df_to_teradata_table(
        config: TeradataConfig, df: pd.DataFrame, table_name: str
) -> Dict[str, np.dtype]:
    """
    Create a table for the data frame, insert all the values, and return the table schema
    """

    with _get_conn(config).connect() as conn:

        col_type_dict = dict(zip(df.columns, df.dtypes))
        fastload(df=df,
                    table_name=table_name,
                    if_exists="fail")

        return col_type_dict


def teradata_type_to_feast_value_type(data_type):
    type_map: Dict[str, ValueType] = {
        "<class 'int'>": pa.int64(),
        "<class 'float'>": pa.float64(),
        "<class 'datetime.datetime'>": pa.timestamp('ns')  # TODO: Add more mappings here
    }

    return type_map[str(data_type)]