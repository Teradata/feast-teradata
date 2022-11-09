from datetime import datetime
from typing import Sequence, Union, List, Optional, Tuple, Dict, Callable, Any

import pytz
import itertools
from binascii import hexlify
from collections import defaultdict
from feast.usage import log_exceptions_and_usage
from feast import RepoConfig,FeatureView, Entity
from feast.infra.key_encoding_utils import serialize_entity_key
from feast.infra.online_stores.online_store import OnlineStore
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto

from feast.repo_config import FeastConfigBaseModel
from pydantic import StrictStr
from pydantic.typing import Literal
from teradataml import (
create_context,
get_context,
get_connection,
copy_to_sql,
BIGINT, TIMESTAMP,
fastload
)
from feast.utils import to_naive_utc
class TeradataConfig(FeastConfigBaseModel):
    host: StrictStr
    port: int = 1025
    database: StrictStr
    user: StrictStr
    password: StrictStr
    log_mech: Optional[StrictStr] = "LDAP"


class TeradataOnlineStoreConfig(TeradataConfig):
    """
    Configuration for the MySQL online store.
    NOTE: The class *must* end with the `OnlineStoreConfig` suffix.
    """
    type: Literal[
        "Feast_teradata_online_Store.teradata.TeradataOnlineStore"
    ] = "Feast_teradata_online_Store.teradata.TeradataOnlineStore"


class TeradataOnlineStore(OnlineStore):

    def _get_conn(self, config: RepoConfig):

        online_store_config = config.online_store
        assert isinstance(online_store_config, TeradataOnlineStoreConfig)

        if get_context() is None:
            create_context(host=online_store_config.host, username=online_store_config.user, password=online_store_config.password, database=online_store_config.user,
                           logmech=online_store_config.log_mech)
        return get_context()

    @log_exceptions_and_usage(online_store="teradata")
    def online_write_batch(

            self,
            config: RepoConfig,
            table: FeatureView,
            data: List[
                Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
            ],
            progress: Optional[Callable[[int], Any]],
    ) -> None:

        conn = self._get_conn(config).raw_connection().cursor()

        project = config.project

        with conn: #todo
            for entity_key, values, timestamp, created_ts in data:
                print(entity_key)
                print("------------")
                print(config.entity_key_serialization_version)
                entity_key_bin = serialize_entity_key(
                    entity_key,
                    entity_key_serialization_version=config.entity_key_serialization_version,
                )
                timestamp = to_naive_utc(timestamp)
                if created_ts is not None:
                    created_ts = to_naive_utc(created_ts)

                for feature_name, val in values.items():
                    conn.execute(
                        f"""
                                    UPDATE {_table_id(project, table)}
                                    SET value = ?, event_ts = ?, created_ts = ?
                                    WHERE (entity_key = ? AND feature_name = ?)
                                """,
                        (
                            # SET
                            val.SerializeToString(),
                            timestamp,
                            created_ts,
                            # WHERE
                            entity_key_bin,
                            feature_name,
                        ),
                    )

                    conn.execute(
                        f"""INSERT OR IGNORE INTO {_table_id(project, table)}
                                    (entity_key, feature_name, value, event_ts, created_ts)
                                    VALUES (?, ?, ?, ?, ?)""",
                        (
                            entity_key_bin,
                            feature_name,
                            val.SerializeToString(),
                            timestamp,
                            created_ts,
                        ),
                    )
                if progress:
                    progress(1)

    @staticmethod
    def write_to_table(created_ts, cur, entity_key_bin, feature_name, project, table, timestamp, val):
        cur.execute(
            f"""
                        UPDATE {_table_id(project, table)}
                        SET value = %s, event_ts = %s, created_ts = %s
                        WHERE (entity_key = %s AND feature_name = %s)
                    """,
            (
                # SET
                val.SerializeToString(),
                timestamp,
                created_ts,
                # WHERE
                entity_key_bin,
                feature_name,
            ),
        )
        cur.execute(
            f"""INSERT INTO {_table_id(project, table)}
                        (entity_key, feature_name, value, event_ts, created_ts)
                        VALUES (%s, %s, %s, %s, %s)""",
            (
                entity_key_bin,
                feature_name,
                val.SerializeToString(),
                timestamp,
                created_ts,
            ),
        )

    def online_read(
            self,
            config: RepoConfig,
            table: Union[FeatureView],
            entity_keys: List[EntityKeyProto],
            requested_features: Optional[List[str]] = None,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        cur = self._get_conn(config).raw_connection().cursor()

        result: List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]] = []

        project = config.project
        for entity_key in entity_keys:
            entity_key_bin = serialize_entity_key(entity_key).hex()

            query = f"SELECT feature_name, \'value\', event_ts FROM {_table_id(project, table)} WHERE entity_key = \'{entity_key_bin}\'"
            print(query)

            cur.execute(
                query
            )

            res = {}
            res_ts = None
            for feature_name, val_bin, ts in cur.fetchall():
                val = ValueProto()
                val.ParseFromString(val_bin)
                res[feature_name] = val
                res_ts = ts

            if not res:
                result.append((None, None))
            else:
                result.append((res_ts, res))
        return result

    def update(
            self,
            config: RepoConfig,
            tables_to_delete: Sequence[FeatureView],
            tables_to_keep: Sequence[FeatureView],
            entities_to_delete: Sequence[Entity],
            entities_to_keep: Sequence[Entity],
            partial: bool,
    ):
        conn = self._get_conn(config).connect()
        cur = self._get_conn(config).raw_connection().cursor()

        project = config.project

        # We don't create any special state for the entites in this implementation.

        for table in tables_to_keep:
            print(_table_id(project, table))
            cur.execute(
                f"CREATE TABLE {_table_id(project, table)} (entity_key VARCHAR(512), feature_name VARCHAR(256), \"value\" BLOB, event_ts timestamp, created_ts timestamp)"
            )
            # cur.execute(
            #     f"CREATE INDEX {_table_id(project, table)}_ek (entity_key) ON {_table_id(project, table)};"
            # )

        for table in tables_to_delete:
            # cur.execute(
            #     f"DROP INDEX {_table_id(project, table)}_ek ON {_table_id(project, table)};"
            # )
            cur.execute(f"DROP TABLE {_table_id(project, table)}")

    def teardown(
            self,
            config: RepoConfig,
            tables: Sequence[FeatureView],
            entities: Sequence[Entity],
    ):
        conn = self._get_conn(config).connect()
        cur = conn.raw_connection().cursor()
        project = config.project

        for table in tables:
            cur.execute(
                f"DROP INDEX {_table_id(project, table)}_ek ON {_table_id(project, table)};"
            )
            cur.execute(f"DROP TABLE {_table_id(project, table)}")



def _to_naive_utc(ts: datetime):
    if ts.tzinfo is None:
        return ts
    else:
        return ts.astimezone(pytz.utc).replace(tzinfo=None)

def _table_id(project: str, table: FeatureView) -> str:
    return f"{project}_{table.name}"


