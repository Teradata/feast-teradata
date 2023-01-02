import json
from typing import Callable, Dict, Iterable, Optional, Tuple
from typeguard import typechecked

from feast.data_source import DataSource
from feast.errors import DataSourceNoNameException
from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto
from feast.protos.feast.core.SavedDataset_pb2 import (
    SavedDatasetStorage as SavedDatasetStorageProto,
)
from feast.repo_config import RepoConfig
from feast.saved_dataset import SavedDatasetStorage
from feast.value_type import ValueType
import pandas as pd
import numpy as np

from feast_teradata.teradata_utils import (
    get_conn,
    TeradataConfig
)
from teradataml import DataFrame


def td_type_to_feast_value_type(type_str: str) -> ValueType:
    type_map: Dict[str, ValueType] = {
        "<class 'byte'>": ValueType.BYTES,
        "<class 'str'>": ValueType.STRING,
        "<class 'int'>": ValueType.INT64,
        "<class 'float'>": ValueType.FLOAT,
        "<class 'datetime.datetime'>": ValueType.UNIX_TIMESTAMP,
    }
    value = (
        type_map[f"""{type_str}"""]
        if f"""{type_str}""" in type_map
        else ValueType.UNKNOWN
    )
    if value == ValueType.UNKNOWN:
        print("unknown type:", type_str)
    return value
@typechecked
class TeradataSource(DataSource):
    def __init__(
            self,
            name: Optional[str] = None,
            query: Optional[str] = None,
            table: Optional[str] = None,
            database: Optional[str] = None,
            timestamp_field: Optional[str] = "",
            created_timestamp_column: Optional[str] = "",
            field_mapping: Optional[Dict[str, str]] = None,
            description: Optional[str] = "",
            tags: Optional[Dict[str, str]] = None,
            owner: Optional[str] = "",
    ):
        self._teradata_options = TeradataOptions(name=name,
                                                 query=query,
                                                 database=database,
                                                 table=table)

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
            database=teradata_options["database"],
            field_mapping=dict(data_source.field_mapping),
            timestamp_field=data_source.timestamp_field,
            created_timestamp_column=data_source.created_timestamp_column,
            description=data_source.description,
            tags=dict(data_source.tags),
            owner=data_source.owner,
        )

    def get_table_query_string(self) -> str:
        """Returns a string that can directly be used to reference this table in SQL."""
        if self._teradata_options._table:
            return f"{self._teradata_options._table}"
        else:
            return f"({self._teradata_options._query})"

    def to_proto(self) -> DataSourceProto:
        data_source_proto = DataSourceProto(
            name=self.name,
            type=DataSourceProto.CUSTOM_SOURCE,
            data_source_class_type="feast_teradata.offline.teradata_source.TeradataSource",
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
        return td_type_to_feast_value_type

    def get_table_column_names_and_types(
            self, config: RepoConfig
    ) -> Iterable[Tuple[str, str]]:
        with get_conn(config.offline_store).raw_connection().cursor() as cur:
            # df = pd.read_sql(f"SELECT * FROM {self.get_table_query_string()} sample 1", conn)
            # column_names = df.columns
            # types = df.dtypes
            # return list(zip(column_names, types))
            cur.execute(
                f"SELECT * FROM {self.get_table_query_string()} sample 1"
            )
            name_desc_ret = [(item[0], item[1]) for item in cur.description]

            return name_desc_ret
class TeradataOptions:
    def __init__(
            self,
            name: Optional[str],
            query: Optional[str],
            table: Optional[str],
            database: Optional[str]
    ):
        self._name = name or ""
        self._query = query or ""
        self._table = table or ""
        self._database = database or ""

    @classmethod
    def from_proto(cls, teradata_options_proto: DataSourceProto.CustomSourceOptions):
        config = json.loads(teradata_options_proto.configuration.decode("utf8"))
        teradata_options = cls(
            name=config["name"],
            query=config["query"],
            table=config["table"],
            database=config["database"]
        )

        return teradata_options

    def to_proto(self) -> DataSourceProto.CustomSourceOptions:
        teradata_options_proto = DataSourceProto.CustomSourceOptions(
            configuration=json.dumps(
                {"name": self._name,
                 "query": self._query,
                 "table": self._table,
                 "database": self._database}
            ).encode()
        )
        return teradata_options_proto


class SavedDatasetTeradataStorage(SavedDatasetStorage):
    _proto_attr_name = "custom_storage"

    teradata_options: TeradataOptions

    def __init__(self, table_ref: str):
        self.teradata_options = TeradataOptions(
            table=table_ref,
            name=None,
            query=None,
            database=None
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


def df_to_teradata_table(config: TeradataConfig, df: pd.DataFrame, table_name: str) -> Dict[str, np.dtype]:
    """
    Create a table for the data frame, insert all the values, and return the table schema
    """

    with get_conn(config).connect() as conn:
        col_type_dict = dict(zip(df.columns, df.dtypes))
        df.to_sql(name=table_name, con=conn, if_exists='replace', index=False)

        return col_type_dict
